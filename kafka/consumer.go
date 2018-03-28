package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

type KafkaConsumer struct {
	*sync.Mutex

	client         sarama.Client
	singleConsumer *sarama.Consumer
	groupConsumer  *cluster.Consumer
	// in-memory cache for simple partition consumer
	partitionConsumer sarama.PartitionConsumer

	groupID   string
	topic     string
	partition int32

	errors chan error
}

// Consumer initializes a default consumer client for consuming messages.
// This function uses consumer group and all paritions will be load balanced
func Consumer(topic, groupID string, brokers ...string) (*KafkaConsumer, error) {
	conf := cluster.NewConfig()
	conf.Version = sarama.V0_11_0_2
	conf.Consumer.Return.Errors = true
	client, err := cluster.NewClient(brokers, conf)
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumerFromClient(client, groupID, []string{topic})
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		Mutex:         new(sync.Mutex),
		client:        client.Client,
		groupConsumer: consumer,
		groupID:       groupID,
		topic:         topic,
		partition:     -1,
		errors:        make(chan error)}, nil
}

// ConsumerFromParition initializes a default consumer client for consuming messages
func ConsumerFromParition(topic string, partition int, brokers ...string) (*KafkaConsumer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_11_0_2
	conf.Consumer.Return.Errors = true
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
		errors:         make(chan error)}, nil
}

func (c *KafkaConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}
	messages := make(chan messaging.Event, 1)
	rawMessages := make(chan *sarama.ConsumerMessage, 1)

	consume := func(msgs <-chan *sarama.ConsumerMessage) {
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
		}
	}
	if c.singleConsumer != nil {
		pConsumer, err := (*c.singleConsumer).ConsumePartition(c.topic, c.partition, options.Offset)
		if err != nil {
			return nil, err
		}
		// cache open consumer to properly clean up later
		c.partitionConsumer = pConsumer
		// forward messages
		go consume(pConsumer.Messages())
		// forward errors
		go func(errs <-chan *sarama.ConsumerError) {
			for e := range errs {
				c.errors <- e.Err
				// ------ Prometheus metrics ---------
				errorsCount.
					WithLabelValues(e.Topic, "consumer", "", strconv.Itoa(int(e.Partition))).
					Add(1)
			}
			close(c.errors)
		}(pConsumer.Errors())
	} else {
		// forward messages
		go consume(c.groupConsumer.Messages())
		// forward errors
		go func(errs <-chan error) {
			for e := range errs {
				c.errors <- e
				// ------ Prometheus metrics ---------
				errorsCount.
					WithLabelValues(c.groupID, "consumer", c.groupID, "").
					Add(1)
			}
			close(c.errors)
		}(c.groupConsumer.Errors())
	}
	go func() {
	ConsumerLoop:
		for {
			select {
			case m, ok := <-rawMessages:
				if !ok {
					break ConsumerLoop
				}
				// lagMetrics.WithLabelValues(s.Topic, "consumer", s.Partition).Set(float64(s.Lag))
				messages <- &event{
					&gKafka.Message{
						Key:       m.Key,
						Value:     m.Value,
						Offset:    m.Offset,
						Partition: int(m.Partition),
						Time:      m.Timestamp,
						Topic:     m.Topic,
					},
				}
			case <-ctx.Done():
				break ConsumerLoop
			default:
				if c.client.Closed() {
					break ConsumerLoop
				}
			}
		}
		close(messages)
	}()
	return messages, nil
}

func (c *KafkaConsumer) Close() error {
	c.Lock()
	defer c.Unlock()

	var errorStrs []string
	if c.partitionConsumer != nil {
		if err := c.partitionConsumer.Close(); err != nil {
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
	}
	if err := c.client.Close(); err != nil {
		errorStrs = append(errorStrs, err.Error())
	}
	// drain errors chan
	for err := range c.errors {
		errorStrs = append(errorStrs, err.Error())
	}
	if len(errorStrs) > 0 {
		return fmt.Errorf(
			"(%d) errors while consuming: %s",
			len(errorStrs),
			strings.Join(errorStrs, "\n"))
	}
	return nil
}
