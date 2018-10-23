package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/mitchellh/mapstructure"
	"github.com/rcrowley/go-metrics"
	messaging "github.com/veritone/go-messaging-lib"
)

// SetLogger turns on sarama log
func SetLogger(log *log.Logger) {
	sarama.Logger = log
}

func init() {
	// disable go-metrics. We use prometheus for instrumentation.
	metrics.UseNilMetrics = true
}

type KafkaConsumer struct {
	*sync.Mutex

	client         sarama.Client
	singleConsumer *sarama.Consumer
	groupConsumer  *cluster.Consumer
	// in-memory cache for simple partition consumer
	partitionConsumer sarama.PartitionConsumer

	groupID       string
	topic         string
	partition     int32
	errors        chan error
	autoMark      bool
	brokers       []string
	initialOffset int64
}

type ClientOption func(*KafkaConsumer)

// WithDisableAutoMark gives clients the ability to turn off auto-marking which means clients are responsible to mark the offset themselves
// This is useful when clients want to retry certain message they fail to process
func WithDisableAutoMark() ClientOption {
	return func(client *KafkaConsumer) {
		client.autoMark = false
	}
}

// WithBrokers specifies brokers for kafka consumer
func WithBrokers(brokers ...string) ClientOption {
	return func(client *KafkaConsumer) {
		client.brokers = brokers
	}
}

// WithInitialOffset specifies the initial offset to use if no offset was previously committed.
func WithInitialOffset(offset int64) ClientOption {
	return func(client *KafkaConsumer) {
		client.initialOffset = offset
	}
}

// MarkOffset lets clients mark offset manually after they process each message
func (consumer *KafkaConsumer) MarkOffset(msg messaging.Event, metadata string) error {
	var obj sarama.ConsumerMessage
	// Convert to sarama consumer message
	err := mapstructure.Decode(msg.Metadata(), &obj)
	if err != nil {
		return fmt.Errorf("Error converting from msg to sarama message format: %s", err)
	}
	consumer.groupConsumer.MarkOffset(&obj, metadata)
	return nil
}

// NewConsumer initializes a default consumer client for consuming messages.
// This function uses consumer group and all partitions will be load balanced
func NewConsumer(topic, groupID string, opts ...ClientOption) (*KafkaConsumer, error) {
	if len(groupID) == 0 {
		return nil, errors.New("must supply groupID to use high-level consumer")
	}

	kafkaClient := &KafkaConsumer{
		Mutex:         new(sync.Mutex),
		groupID:       groupID,
		topic:         topic,
		partition:     -1,
		errors:        make(chan error, 1),
		autoMark:      true,
		initialOffset: sarama.OffsetOldest,
	}

	// Handle options
	for _, optionFunc := range opts {
		optionFunc(kafkaClient)
	}

	if len(kafkaClient.brokers) == 0 {
		return nil, errors.New("brokers must be specified")
	}
	if !isValidInitialOffset(kafkaClient.initialOffset) {
		return nil, errors.New("Initial offset must be -1 or -2")
	}

	conf := cluster.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5
	conf.Metadata.Retry.Max = 5
	conf.Metadata.Retry.Backoff = 1 * time.Second

	// This is necessary to read messages on newly created topics
	// before a consumer started listening
	conf.Consumer.Offsets.Initial = kafkaClient.initialOffset

	client, err := cluster.NewClient(kafkaClient.brokers, conf)
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumerFromClient(client, groupID, []string{topic})
	if err != nil {
		return nil, err
	}

	kafkaClient.client = client.Client
	kafkaClient.groupConsumer = consumer

	return kafkaClient, nil
}

// NewConsumerFromPartition initializes a default consumer client for consuming messages
func NewConsumerFromPartition(topic string, partition int, opts ...ClientOption) (*KafkaConsumer, error) {
	kafkaClient := &KafkaConsumer{
		Mutex:         new(sync.Mutex),
		groupID:       "",
		topic:         topic,
		partition:     int32(partition),
		errors:        make(chan error, 1),
		initialOffset: sarama.OffsetOldest,
	}

	// Handle options
	for _, optionFunc := range opts {
		optionFunc(kafkaClient)
	}

	if len(kafkaClient.brokers) == 0 {
		return nil, errors.New("brokers must be specified")
	}
	if !isValidInitialOffset(kafkaClient.initialOffset) {
		return nil, errors.New("Initial offset must be -1 or -2")
	}

	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5
	conf.Consumer.Offsets.Initial = kafkaClient.initialOffset
	conf.Metadata.Retry.Max = 5
	conf.Metadata.Retry.Backoff = 1 * time.Second

	client, err := sarama.NewClient(kafkaClient.brokers, conf)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	kafkaClient.client = client
	kafkaClient.singleConsumer = &consumer

	return kafkaClient, nil
}

func routeMsg(ctx context.Context, c *KafkaConsumer, msgs <-chan *sarama.ConsumerMessage, messages chan messaging.Event, errors chan error) {
	defer close(messages)
	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				return
			}
			log.Printf("Obtained message: Topic: %s; groupID: %s; Partition: %d, Value: %s", m.Topic, c.groupID, m.Partition, string(m.Value))
			messages <- &event{
				&Message{
					Key:       m.Key,
					Value:     m.Value,
					Offset:    m.Offset,
					Partition: m.Partition,
					Time:      m.Timestamp,
					Topic:     m.Topic,
				},
			}
			log.Println("Sent obtained message to chan")
			c.groupConsumer.MarkOffset(m, "")
		case <-ctx.Done():
			errors <- ctx.Err()
			return
		}
	}
}

func (c *KafkaConsumer) processSingleConsumer(ctx context.Context, messages chan messaging.Event, offset int64, errors chan error) error {
	pConsumer, err := (*c.singleConsumer).ConsumePartition(c.topic, c.partition, offset)
	if err != nil {
		return err
	}
	// cache open consumer to properly clean up later
	c.partitionConsumer = pConsumer
	// forward messages
	go routeMsg(ctx, c, pConsumer.Messages(), messages, errors)
	// forward errors
	go func(errs <-chan *sarama.ConsumerError) {
		for e := range errs {
			c.errors <- e.Err
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(e.Topic, "consumer", "", strconv.Itoa(int(e.Partition))).
				Add(1)
			break
		}
	}(pConsumer.Errors())

	return nil
}

func (c *KafkaConsumer) processGroupConsumer(ctx context.Context, messages chan messaging.Event, errors chan error) {
	// forward messages
	go routeMsg(ctx, c, c.groupConsumer.Messages(), messages, errors)
	// forward errors
	go func(errs <-chan error) {
		for e := range errs {
			c.errors <- e
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(c.groupID, "consumer", c.groupID, "").
				Add(1)
			break
		}
	}(c.groupConsumer.Errors())
}

func (c *KafkaConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}
	messages := make(chan messaging.Event, 0)
	if c.singleConsumer != nil {
		err := c.processSingleConsumer(ctx, messages, options.Offset, c.errors)
		if err != nil {
			return nil, err
		}
	} else {
		c.processGroupConsumer(ctx, messages, c.errors)
	}
	return messages, nil
}

func (c *KafkaConsumer) Close() error {
	c.Lock()
	defer c.Unlock()
	defer func() error {
		// Sarama panics when closing already closed Kafka clients.
		// This is not a fatal condition that warrants panicking.
		// Recover and rethrow a normal error so clients can act appropriately,
		// most likely print an error message then ignore.
		// There could be other conditions where Sarama panics. However the idea is
		// to let clients decide how to handle those situations instead of crashing
		// whole application.
		if r := recover(); r != nil {
			return fmt.Errorf("Recovered from panic: %v", r)
		}
		return nil
	}()
	var errorStrs []string
	if c.partitionConsumer != nil {
		if err := c.partitionConsumer.Close(); err != nil {
			errorStrs = append(errorStrs, err.Error())
		}
		// drain errors chan
		for err := range c.partitionConsumer.Errors() {
			errorStrs = append(errorStrs, err.Error())
		}
	}

	if c.singleConsumer != nil {
		if err := (*c.singleConsumer).Close(); err != nil {
			errorStrs = append(errorStrs, err.Error())
		}
	} else {
		if err := c.groupConsumer.Close(); err != nil {
			errorStrs = append(errorStrs, err.Error())
		}
		// drain errors chan
		for err := range c.groupConsumer.Errors() {
			errorStrs = append(errorStrs, err.Error())
		}
	}
	if err := c.client.Close(); err != nil {
		errorStrs = append(errorStrs, err.Error())
	}
	close(c.errors)
	// drains all errors and return them.
	for errs := range c.errors {
		errorStrs = append(errorStrs, errs.Error())
	}
	if len(errorStrs) > 0 {
		return fmt.Errorf(
			"(%d) errors while consuming: %s",
			len(errorStrs),
			strings.Join(errorStrs, "\n"))
	}
	return nil
}

func isValidInitialOffset(offset int64) bool {
	return offset == sarama.OffsetOldest || offset == sarama.OffsetNewest
}
