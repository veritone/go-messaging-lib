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

// GetDefaultConfig returns default specific config
func GetDefaultConfig() (*sarama.Config, *cluster.Config) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.ClientID = "go-messaging-lib"

	conf.Metadata.Retry.Max = 5
	conf.Metadata.Retry.Backoff = 1 * time.Second

	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5

	conf.Producer.Retry.Max = 5
	conf.Producer.Retry.Backoff = 1 * time.Second
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true

	conf.Admin.Timeout = 30 * time.Second // Not used

	clusConf := cluster.NewConfig()
	clusConf.Config = *conf

	return conf, clusConf
}

type KafkaConsumer struct {
	*sync.Mutex

	client         sarama.Client
	singleConsumer sarama.Consumer
	groupConsumer  *cluster.Consumer
	// in-memory cache for simple partition consumer
	partitionConsumer sarama.PartitionConsumer

	groupID       string
	topic         string
	partition     int32
	eventChans    map[chan messaging.Event]bool
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
		eventChans:    make(map[chan messaging.Event]bool),
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

	_, conf := GetDefaultConfig()

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
		eventChans:    make(map[chan messaging.Event]bool),
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

	conf, _ := GetDefaultConfig()

	client, err := sarama.NewClient(kafkaClient.brokers, conf)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	kafkaClient.client = client
	kafkaClient.singleConsumer = consumer

	return kafkaClient, nil
}

func (c *KafkaConsumer) transformMessages(ctx context.Context, messages <-chan *sarama.ConsumerMessage, errc <-chan error) chan messaging.Event {
	events := make(chan messaging.Event)
	// cache this event channel for clean up
	c.eventChans[events] = true

	go func() {
		defer close(events)
		for {
			select {
			case err := <-errc:
				if err != nil {
					c.errors <- err
				}
				return

			case m, ok := <-messages:
				if !ok {
					return
				}
				// lagMetrics.WithLabelValues(s.Topic, "consumer", s.Partition).Set(float64(s.Lag))
				events <- &event{
					&Message{
						Key:       m.Key,
						Value:     m.Value,
						Offset:    m.Offset,
						Partition: m.Partition,
						Time:      m.Timestamp,
						Topic:     m.Topic,
					},
				}

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

			case <-ctx.Done():
				c.errors <- ctx.Err()
				return
			}
		}
	}()

	return events
}

func (c *KafkaConsumer) processSingleConsumer(ctx context.Context, offset int64) (<-chan messaging.Event, error) {
	pConsumer, err := c.singleConsumer.ConsumePartition(c.topic, c.partition, offset)
	if err != nil {
		return nil, err
	}
	// cache open consumer to properly clean up later
	c.partitionConsumer = pConsumer
	errc := make(chan error, 1)

	// forward errors
	go func(errs <-chan *sarama.ConsumerError) {
		defer close(errc)
		for e := range errs {
			errc <- e.Err
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(e.Topic, "consumer", "", strconv.Itoa(int(e.Partition))).
				Add(1)
			break
		}
	}(pConsumer.Errors())

	return c.transformMessages(ctx, pConsumer.Messages(), errc), nil
}

func (c *KafkaConsumer) processGroupConsumer(ctx context.Context) (<-chan messaging.Event, error) {
	errc := make(chan error, 1)

	// forward errors
	go func(errs <-chan error) {
		defer close(errc)
		for e := range errs {
			errc <- e
			// ------ Prometheus metrics ---------
			errorsCount.
				WithLabelValues(c.groupID, "consumer", c.groupID, "").
				Add(1)
			break
		}
	}(c.groupConsumer.Errors())

	return c.transformMessages(ctx, c.groupConsumer.Messages(), errc), nil
}

func (c *KafkaConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}

	if c.singleConsumer != nil {
		return c.processSingleConsumer(ctx, options.Offset)
	}

	return c.processGroupConsumer(ctx)
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
		if err := c.singleConsumer.Close(); err != nil {
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

	// drains all errors and return them - don't close errors chan because this causes a panic when goroutines try to send on it
	for {
		select {
		case err := <-c.errors:
			errorStrs = append(errorStrs, err.Error())
			continue
		default:
		}

		break
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

func isValidInitialOffset(offset int64) bool {
	return offset == sarama.OffsetOldest || offset == sarama.OffsetNewest
}
