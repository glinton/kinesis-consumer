package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Record is an alias of record returned from kinesis library
type Record = kinesis.Record

// Option is used to override defaults when creating a new Consumer
type Option func(*Consumer)

// WithCheckpoint overrides the default checkpoint
func WithCheckpoint(checkpoint Checkpoint) Option {
	return func(c *Consumer) {
		c.checkpoint = checkpoint
	}
}

// WithLogger overrides the default logger
func WithLogger(logger Logger) Option {
	return func(c *Consumer) {
		c.logger = logger
	}
}

// WithCounter overrides the default counter
func WithCounter(counter Counter) Option {
	return func(c *Consumer) {
		c.counter = counter
	}
}

// WithClient overrides the default client
func WithClient(client kinesisiface.KinesisAPI) Option {
	return func(c *Consumer) {
		c.client = client
	}
}

// ShardIteratorType overrides the starting point for the consumer
func WithShardIteratorType(t string) Option {
	return func(c *Consumer) {
		c.initialShardIteratorType = t
	}
}

// ScanStatus signals the consumer if we should continue scanning for next record
// and whether to checkpoint.
type ScanStatus struct {
	Error          error
	StopScan       bool
	SkipCheckpoint bool
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
type ScanFunc func(r *Record) ScanStatus

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, fmt.Errorf("must provide stream name")
	}

	// new consumer with no-op checkpoint, counter, and logger
	c := &Consumer{
		shards:                   make(map[*string]*kinesis.Shard),
		errc:                     make(chan error, 1),
		wg:                       &sync.WaitGroup{},
		streamName:               streamName,
		initialShardIteratorType: kinesis.ShardIteratorTypeTrimHorizon,
		checkpoint:               &noopCheckpoint{},
		counter:                  &noopCounter{},
		logger: &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		},
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client if none provided
	if c.client == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return nil, err
		}
		c.client = kinesis.New(newSession)
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType string
	client                   kinesisiface.KinesisAPI
	logger                   Logger
	checkpoint               Checkpoint
	counter                  Counter
	errc                     chan error
	shards                   map[*string]*kinesis.Shard
	mux                      sync.Mutex
	wg                       *sync.WaitGroup
}

// Scan scans each of the shards of the stream, calls the callback
// func with each of the kinesis records.
func (c *Consumer) Scan(ctx context.Context, fn ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := c.scanShards(ctx, fn); err != nil {
		return err
	}

	go func() {
		c.wg.Wait()
		close(c.errc)
	}()

	for err := range c.errc {
		return err
	}
	return nil
}

func (c *Consumer) addShard(shard *kinesis.Shard) {
	c.mux.Lock()
	c.shards[shard.ShardId] = shard
	c.mux.Unlock()
}

func (c *Consumer) existingShard(shard *kinesis.Shard) bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	_, ok := c.shards[shard.ShardId]
	return ok
}

func (c *Consumer) scanShards(ctx context.Context, fn ScanFunc) error {
	// shard ids
	shards, err := c.getShards(c.streamName)
	if err != nil {
		return fmt.Errorf("get shards error: %v", err)
	}

	// shards available
	if len(shards) == 0 {
		return fmt.Errorf("no shards available")
	}

	// process each shard in a separate goroutine
	for _, shard := range shards {
		fmt.Println(c.shards)
		if c.existingShard(shard) {
			fmt.Println("existing shard", aws.StringValue(shard.ShardId))
			continue
		}
		c.addShard(shard)
		c.wg.Add(1)

		go func(shardID string) {
			defer c.wg.Done()

			err := c.ScanShard(ctx, shardID, fn)
			if err != nil {
				select {
				case c.errc <- fmt.Errorf("shard %s error: %v", shardID, err):
					// first error to occur
				default:
					// error has already occured
				}
			}
		}(aws.StringValue(shard.ShardId))
	}

	return nil
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get checkpoint
	lastSeqNum, err := c.checkpoint.Get(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	c.logger.Log("starting scan", shardID, lastSeqNum)
	defer func() {
		c.logger.Log("stopping scan", shardID)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			if err != nil {
				shardIterator, err = c.getShardIterator(c.streamName, shardID, lastSeqNum)
				if err != nil {
					return fmt.Errorf("get shard iterator error: %v", err)
				}
				continue
			}

			// loop records of page
			for _, r := range resp.Records {
				isScanStopped, err := c.handleRecord(shardID, r, fn)
				if err != nil {
					return err
				}
				if isScanStopped {
					return nil
				}
				lastSeqNum = *r.SequenceNumber
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				return c.scanShards(ctx, fn)
			}
			shardIterator = resp.NextShardIterator
		}
	}
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) handleRecord(shardID string, r *Record, fn ScanFunc) (isScanStopped bool, err error) {
	status := fn(r)

	if !status.SkipCheckpoint {
		if err := c.checkpoint.Set(c.streamName, shardID, *r.SequenceNumber); err != nil {
			return false, err
		}
	}

	if err := status.Error; err != nil {
		return false, err
	}

	c.counter.Add("records", 1)

	if status.StopScan {
		return true, nil
	}
	return false, nil
}

func (c *Consumer) getShards(streamName string) ([]*kinesis.Shard, error) {
	resp, err := c.client.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("describe stream error: %v", err)
	}
	return resp.StreamDescription.Shards, nil
}

func (c *Consumer) getShardIterator(streamName, shardID, lastSeqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if lastSeqNum != "" {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(lastSeqNum)
	} else {
		params.ShardIteratorType = aws.String(c.initialShardIteratorType)
	}

	resp, err := c.client.GetShardIterator(params)
	if err != nil {
		return nil, err
	}
	return resp.ShardIterator, nil
}
