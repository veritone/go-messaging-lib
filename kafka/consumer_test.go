package kafka_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	messaging "github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/kafka"
)

func Test_consumers(t *testing.T) {
	setup(t)
	defer tearDown(t)
	// Start and close simple consumer with oldest offset
	testConsumerFromPartition(t, "t1", kafka.OffsetOldest)
	// Start and close simple consumer on existing topic "t1" with newest offset
	testConsumerFromPartition(t, "t1", kafka.OffsetNewest)

	// Start and close high-level consumer (group)
	testConsumerWithGroup(t, "t2", "g1")
	// Start and close high-level consumer (group) on existing topic "t2"
	testConsumerWithGroup(t, "t2", "g2")
}

func testConsumerWithGroup(t *testing.T, topic, group string) {
	var wg sync.WaitGroup
	wg.Add(1)
	c, err := kafka.Consumer("t1", "g1", kafkaHost)
	assert.NoError(t, err)
	q, err := c.Consume(context.Background(), kafka.ConsumerGroupOption)
	assert.NoError(t, err)
	go func(<-chan messaging.Event) {
		for i := range q {
			spew.Dump(i)
		}
		wg.Done()
	}(q)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	wg.Wait()
}

func testConsumerFromPartition(t *testing.T, topic string, offset int64) {
	var wg sync.WaitGroup
	wg.Add(1)
	c, err := kafka.ConsumerFromPartition("t2", 0, kafkaHost)
	assert.NoError(t, err)
	q, err := c.Consume(context.Background(), kafka.NewConsumerOption(kafka.OffsetOldest))
	assert.NoError(t, err)
	go func(<-chan messaging.Event) {
		for i := range q {
			spew.Dump(i)
		}
		wg.Done()
	}(q)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	wg.Wait()
}

func TestBasicConsumer(t *testing.T) {
	setup(t)
	defer tearDown(t)

	c, err := kafka.Consumer("test_topic", "g1", "kafka1:9092")
	assert.NoError(t, err, "should have a consumer connect to kafka")
	ctx := context.Background()
	q, err := c.Consume(ctx, kafka.ConsumerGroupOption)
	assert.NoError(t, err, "should create a queue to consumer from a consumer group")

	var wg sync.WaitGroup
	wg.Add(1)
	go func(<-chan messaging.Event) {
		for i := range q {
			spew.Dump(i)
		}
		wg.Done()
	}(q)
	time.Sleep(time.Second * 5) // Pretend that consumer is working on different thread
	assert.NoError(t, c.Close(), "should shutdown gracefully")
	wg.Wait()
}

func TestConsumerWithContext(t *testing.T) {
	setup(t)
	defer tearDown(t)

	c, err := kafka.Consumer("topic_with_context", "g1", "kafka1:9092")
	assert.NoError(t, err, "should have a consumer connect to kafka")
	ctx := context.Background()
	ctx, _ = context.WithDeadline(ctx, time.Now().Add(time.Second*3))
	q, err := c.Consume(ctx, kafka.ConsumerGroupOption)
	assert.NoError(t, err, "should create a queue to consumer from a consumer group")

	var wg sync.WaitGroup
	wg.Add(1)
	go func(<-chan messaging.Event) {
		for i := range q {
			spew.Dump(i)
		}
		wg.Done()
	}(q)
	assert.Error(t, c.Close(), "have deadline exceeded error")
	wg.Wait()
}
