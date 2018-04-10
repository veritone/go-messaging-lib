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

	groupID    string
	topic      string
	partition  int32
	eventChans map[chan messaging.Event]bool
	errors     chan error
}

// Consumer initializes a default consumer client for consuming messages.
// This function uses consumer group and all partitions will be load balanced
func Consumer(topic, groupID string, brokers ...string) (*KafkaConsumer, error) {
	if len(groupID) == 0 {
		return nil, errors.New("must supply groupID to use high-level consumer")
	}
	conf := cluster.NewConfig()
	conf.Version = sarama.V1_0_0_0
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
		eventChans:    make(map[chan messaging.Event]bool),
		errors:        make(chan error, 1),
	}, nil
}

// ConsumerFromPartition initializes a default consumer client for consuming messages
func ConsumerFromPartition(topic string, partition int, brokers ...string) (*KafkaConsumer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_0_0_0
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
		eventChans:     make(map[chan messaging.Event]bool),
		errors:         make(chan error, 1),
	}, nil
}

func (c *KafkaConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}
	messages := make(chan messaging.Event, 1)
	rawMessages := make(chan *sarama.ConsumerMessage, 1)
	// cache this event channel for clean up
	c.eventChans[messages] = true

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
			// using consumer group should automatically commit offset
			if c.groupConsumer != nil {
				c.groupConsumer.MarkOffset(m, "")
			}
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
				close(rawMessages)
				break
			}
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
				close(rawMessages)
				break
			}
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
			}
		}
		close(messages)
	}()
	return messages, nil
}

func (c *KafkaConsumer) Close() error {
	c.Lock()
	defer c.Unlock()
	close(c.errors)
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
