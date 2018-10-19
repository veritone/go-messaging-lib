package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	messaging "github.com/veritone/go-messaging-lib"
)

// Strategy is a type of routing rule
type Strategy string

type producer struct {
	sarama.Client
	asyncProducer sarama.AsyncProducer
	config        *sarama.Config
	topic         string
	*sync.Mutex
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

// Producer initializes a default producer client for publishing messages
func Producer(topic string, strategy Strategy, brokers ...string) (messaging.Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_1_0_0
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 1 * time.Second
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 1 * time.Second

	var balancer sarama.PartitionerConstructor
	switch strategy {
	case StrategyRoundRobin:
		balancer = sarama.NewRoundRobinPartitioner
	case StrategyLeastBytes:
		return nil, errors.New("balancer is not available")
	default:
		balancer = sarama.NewHashPartitioner
	}
	config.Producer.Partitioner = balancer

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return &producer{
		Client:        client,
		asyncProducer: asyncProducer,
		config:        config,
		Mutex:         new(sync.Mutex),
		topic:         topic}, nil
}

//NewProducer initializes a new client for publishing messages
func NewProducer(topic string, config *sarama.Config, brokers ...string) (messaging.Producer, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return &producer{
		Client:        client,
		asyncProducer: asyncProducer,
		config:        config,
		Mutex:         new(sync.Mutex),
		topic:         topic}, nil
}

func (p *producer) Produce(_ context.Context, msg messaging.Messager, _ ...messaging.Event) error {
	kafkaMsg, ok := msg.Message().(*Message)
	if !ok {
		return fmt.Errorf("unsupported Kafka message: %s", spew.Sprint(msg))
	}
	var err error
	p.asyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(kafkaMsg.Key),
		Value: sarama.ByteEncoder(kafkaMsg.Value),
	}
	select {
	case <-p.asyncProducer.Successes():
		break
	case err = <-p.asyncProducer.Errors():
		break
	}
	return err
}

func (p *producer) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.Closed() {
		return nil
	}
	var errorStrs []string
	if err := p.asyncProducer.Close(); err != nil {
		errorStrs = append(errorStrs, err.Error())
	}
	if err := p.Client.Close(); err != nil {
		errorStrs = append(errorStrs, err.Error())
	}

	if len(errorStrs) > 0 {
		return fmt.Errorf(
			"(%d) errors while producing: %s",
			len(errorStrs),
			strings.Join(errorStrs, "\n"))
	}
	return nil
}
