package kafka

import (
	"context"
	"errors"
	"fmt"
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

	groupID    string
	topic      string
	partition  int32
	eventChans map[chan *sarama.ConsumerMessage]bool
	errors     chan error
	autoMark   bool
	brokers    []string
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
		Mutex:      new(sync.Mutex),
		groupID:    groupID,
		topic:      topic,
		partition:  -1,
		eventChans: make(map[chan *sarama.ConsumerMessage]bool),
		errors:     make(chan error, 1),
		autoMark:   true,
	}

	// Handle options
	for _, optionFunc := range opts {
		optionFunc(kafkaClient)
	}

	if len(kafkaClient.brokers) == 0 {
		return nil, errors.New("brokers must be specified")
	}

	conf := cluster.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 1 * time.Second

	// This is necessary to read messages on newly created topics
	// before a consumer started listening
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest

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
func NewConsumerFromPartition(topic string, partition int, brokers ...string) (*KafkaConsumer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5

	client, err := sarama.NewClient(brokers, conf)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		Mutex:          new(sync.Mutex),
		client:         client,
		groupID:        "",
		topic:          topic,
		partition:      int32(partition),
		singleConsumer: &consumer,
		eventChans:     make(map[chan *sarama.ConsumerMessage]bool),
		errors:         make(chan error, 1),
	}, nil
}

func routeMsg(c *KafkaConsumer, msgs <-chan *sarama.ConsumerMessage, rawMessages chan *sarama.ConsumerMessage) {
	for m := range msgs {
		rawMessages <- m
		// ------ Prometheus metrics ---------
		bytesProcessed.
			WithLabelValues(m.Topic, "consumer", c.groupID, strconv.Itoa(int(m.Partition))).
			Add(float64(len(m.Value)))
		messagesProcessed.
			WithLabelValues(m.Topic, "consumer", c.groupID, strconv.Itoa(int(m.Partition))).
			Add(1)
		offsetMetrics.
			WithLabelValues(m.Topic, "consumer", c.groupID, strconv.Itoa(int(m.Partition))).
			Set(float64(m.Offset))
		// Only auto mark offset when automark is enabled and it's a group consumer
		if c.autoMark && c.groupConsumer != nil {
			c.groupConsumer.MarkOffset(m, "")
		}
	}
}

func constructConsumerMsg(ctx context.Context, rawMessages chan *sarama.ConsumerMessage, messages chan messaging.Event, errors chan error) {
ConsumerLoop:
	for {
		select {
		case m, ok := <-rawMessages:
			if !ok {
				break ConsumerLoop
			}
			// lagMetrics.WithLabelValues(s.Topic, "consumer", s.Partition).Set(float64(s.Lag))
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
		case <-ctx.Done():
			errors <- ctx.Err()
			break ConsumerLoop
		}
	}
	close(messages)
}

func (c *KafkaConsumer) processSingleConsumer(rawMessages chan *sarama.ConsumerMessage, offset int64) error {
	pConsumer, err := (*c.singleConsumer).ConsumePartition(c.topic, c.partition, offset)
	if err != nil {
		return err
	}
	// cache open consumer to properly clean up later
	c.partitionConsumer = pConsumer
	// forward messages
	go routeMsg(c, pConsumer.Messages(), rawMessages)
	// forward errors
	go func(errs <-chan *sarama.ConsumerError) {
		for e := range errs {
			c.errors <- e.Err
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(e.Topic, "consumer", "", strconv.Itoa(int(e.Partition))).
				Add(1)
			close(rawMessages)
			break
		}
	}(pConsumer.Errors())

	return nil
}

func (c *KafkaConsumer) processGroupConsumer(rawMessages chan *sarama.ConsumerMessage) {
	// forward messages
	go routeMsg(c, c.groupConsumer.Messages(), rawMessages)
	// forward errors
	go func(errs <-chan error) {
		for e := range errs {
			c.errors <- e
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(c.groupID, "consumer", c.groupID, "").
				Add(1)
			close(rawMessages)
			break
		}
	}(c.groupConsumer.Errors())
}

func (c *KafkaConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}
	messages := make(chan messaging.Event, 1)
	rawMessages := make(chan *sarama.ConsumerMessage, 1)
	// cache this event channel for clean up
	c.eventChans[rawMessages] = true

	if c.singleConsumer != nil {
		err := c.processSingleConsumer(rawMessages, options.Offset)
		if err != nil {
			return nil, err
		}
	} else {
		c.processGroupConsumer(rawMessages)
	}

	go constructConsumerMsg(ctx, rawMessages, messages, c.errors)
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
	// close all event channels, client should be able to escape
	// out of a range loop on the event channel
	for msgChan := range c.eventChans {
		close(msgChan)
	}
	if len(errorStrs) > 0 {
		return fmt.Errorf(
			"(%d) errors while consuming: %s",
			len(errorStrs),
			strings.Join(errorStrs, "\n"))
	}
	return nil
}
