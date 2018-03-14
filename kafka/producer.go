package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/davecgh/go-spew/spew"

	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

// Strategy is a type of routing rule
type Strategy string

type producer struct {
	*gKafka.Writer
	*sync.Mutex
}

const (
	// StrategyRoundRobin distributes writes evenly
	StrategyRoundRobin Strategy = "RoundRobin"
	// StrategyLeastBytes distributes writes to nodes with least amount of traffic
	StrategyLeastBytes Strategy = "LeastBytes"
	// StrategyHash distributes writes based on 32-bit FNV-1 Hash function. This
	// guarantees messages with the same key are route to the same host
	StrategyHash Strategy = "Hash"
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
	})
	return &producer{w, new(sync.Mutex)}
}

// NewProducer initializes a new client for publishing messages
func NewProducer(config *gKafka.WriterConfig) messaging.Producer {
	return &producer{gKafka.NewWriter(*config), new(sync.Mutex)}
}

func (p *producer) Produce(ctx context.Context, msg messaging.Messager) error {
	kafkaMsg, ok := msg.Message().(*gKafka.Message)
	if !ok {
		return fmt.Errorf("unsupported Kafka message: %s", spew.Sprint(msg))
	}
	return p.WriteMessages(ctx, *kafkaMsg)
}

func (p *producer) Close() error {
	p.Lock()
	defer p.Unlock()
	return p.Writer.Close()
}
