package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

type consumer struct {
	*gKafka.Reader
	*sync.Mutex
	errors chan error
}

// Consumer initializes a default consumer client for consuming messages.
// This function uses consumer group and all paritions will be load balanced
func Consumer(topic, groupID string, brokers ...string) messaging.Consumer {
	r := gKafka.NewReader(gKafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		// Make it synchronous for basic usage
		CommitInterval: 0,
		// Low latency for basic usage
		MaxWait: time.Millisecond * 100,
	})
	return &consumer{r, new(sync.Mutex), make(chan error)}
}

// ConsumerFromParition initializes a default consumer client for consuming messages
func ConsumerFromParition(topic string, parition int, brokers ...string) messaging.Consumer {
	r := gKafka.NewReader(gKafka.ReaderConfig{
		Brokers:   brokers,
		Partition: parition,
		Topic:     topic,
		// Low latency for basic usage
		MaxWait: time.Millisecond * 100,
	})
	return &consumer{r, new(sync.Mutex), make(chan error)}
}

// NewConsumer initializes a consumer client with configurations
func NewConsumer(config *gKafka.ReaderConfig) messaging.Consumer {
	return &consumer{gKafka.NewReader(*config), new(sync.Mutex), make(chan error)}
}

func (c *consumer) Consume(ctx context.Context, ops messaging.OptionCreator) (<-chan interface{}, error) {
	options, ok := ops.Options().(*consumerOptions)
	if !ok {
		return nil, errors.New("invalid option creator, did you use NewConsumerOption or ConsumerGroupOption?")
	}
	// Only set offset when consuming from partition directly
	if len(c.Config().GroupID) == 0 {
		if err := c.SetOffset(options.Offset); err != nil {
			return nil, errors.New("unable to set offset")
		}
	}
	message := make(chan interface{}, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := c.ReadMessage(ctx)
				if err != nil {
					// EOF returns when the client calls Close()
					if err != io.EOF {
						c.errors <- err
					}
					close(c.errors)
					close(message)
					return
				}
				message <- &m
			}
		}
	}()
	return message, nil
}

func (c *consumer) Close() error {
	c.Lock()
	defer c.Unlock()
	// accumulate all errors and report them on close
	var errorStrs []string
	if err := c.Reader.Close(); err != nil {
		errorStrs = append(errorStrs, err.Error())
	}
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
