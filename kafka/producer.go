package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

// Strategy is a type of routing rule
type Strategy string

type producer struct {
	*gKafka.Writer
	*sync.Mutex
	statsUpdater *time.Ticker
}

const (
	// StrategyRoundRobin distributes writes evenly
	StrategyRoundRobin Strategy = "RoundRobin"
	// StrategyLeastBytes distributes writes to nodes with least amount of traffic
	StrategyLeastBytes Strategy = "LeastBytes"
	// StrategyHash distributes writes based on 32-bit FNV-1 Hash function. This
	// guarantees messages with the same key are routed to the same host
	StrategyHash Strategy = "Hash"
)

type ctxKey int

const (
	keyRetry ctxKey = iota
)

// Producer initializes a default producer client for publishing messages
func Producer(topic string, strategy Strategy, brokers ...string) messaging.Producer {
	var balancer gKafka.Balancer
	switch strategy {
	case StrategyRoundRobin:
		balancer = &gKafka.RoundRobin{}
	case StrategyLeastBytes:
		balancer = &gKafka.LeastBytes{}
	default:
		balancer = &gKafka.Hash{}
	}
	w := gKafka.NewWriter(gKafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: balancer,
		// For simple use cases, we send one message per request
		BatchSize: 1,
		// For producing to new topic where partitions haven't been created yet
		RebalanceInterval: time.Second,
	})
	t := monitorProducer(w, time.Second)
	return &producer{w, new(sync.Mutex), t}
}

// NewProducer initializes a new client for publishing messages
func NewProducer(config *gKafka.WriterConfig) messaging.Producer {
	w := gKafka.NewWriter(*config)
	t := monitorProducer(w, time.Second)
	return &producer{w, new(sync.Mutex), t}
}

func (p *producer) Produce(ctx context.Context, msg messaging.Messager) error {
	kafkaMsg, ok := msg.Message().(*Message)
	if !ok {
		return fmt.Errorf("unsupported Kafka message: %s", spew.Sprint(msg))
	}
	err := p.WriteMessages(ctx, gKafka.Message(*kafkaMsg))
	if err != nil {
		// unfortunately, the underlying library does not expose an error value for checking
		// we have to match substring
		if strings.Contains(err.Error(), "failed to find any partitions for topic") {
			// TODO: Could use exponential backoff here
			// Wait a second before retry one last time, it should give Kafka enough time to
			// rebalance and create topic + partition if using Producer() constructor
			if v := ctx.Value(keyRetry); v != nil {
				return errors.New("failed to produce message (run out of retry attempts)")
			}
			time.Sleep(time.Second)
			err = p.Produce(context.WithValue(ctx, keyRetry, true), msg)
		}
	}
	return err
}

func (p *producer) Close() error {
	p.Lock()
	defer p.Unlock()
	p.statsUpdater.Stop()
	return p.Writer.Close()
}
