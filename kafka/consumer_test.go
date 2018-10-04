package kafka_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/kafka"
)

func Test_consumers(t *testing.T) {
	multiBrokerSetup(t)
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
	c, err := kafka.NewConsumer("t1", "g1", kafka.WithBrokers(kafkaHost))
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
	c, err := kafka.NewConsumerFromPartition("t2", 0, kafka.OffsetOldest, kafkaHost)
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
	multiBrokerSetup(t)
	defer tearDown(t)

	c, err := kafka.NewConsumer("test_topic", "g1", kafka.WithBrokers("kafka1:9093"))
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
	multiBrokerSetup(t)
	defer tearDown(t)

	c, err := kafka.NewConsumer("topic_with_context", "g1", kafka.WithBrokers("kafka1:9093"))
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
	wg.Wait()
	assert.Error(t, c.Close(), "have deadline exceeded error")
}

func TestConsumerManualCommit(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	topic := "topic_TestConsumerManualCommit"
	broker := "kafka1:9093"
	// Produce a message
	producer, err := kafka.Producer(topic, kafka.StrategyRoundRobin, broker)
	assert.NoError(t, err, "should be able to create Producer")

	msg1, err := kafka.NewMessage("test", []byte("test1"))
	assert.NoError(t, err, "should have no error")

	err = producer.Produce(context.TODO(), msg1)
	assert.NoError(t, err, "should have no error")

	// consumer1
	consumerGroupId := "consumerGroup_TestConsumerManualCommit"
	consumer1, err := kafka.NewConsumer(topic, consumerGroupId, kafka.WithBrokers(broker), kafka.WithDisableAutoMark())
	assert.NoError(t, err, "should have no error")

	msgChan1, err := consumer1.Consume(context.TODO(), kafka.ConsumerGroupOption)
	assert.NoError(t, err, "should have no error")

	// Wait for msg
	var msgContent messaging.Event
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func(<-chan messaging.Event) {
		for i := range msgChan1 {
			msgContent = i
			spew.Dump(i)
			break
		}
		wg1.Done()
	}(msgChan1)
	wg1.Wait()

	// consumer2
	consumer2, err := kafka.NewConsumer(topic, consumerGroupId, kafka.WithBrokers(broker))
	assert.NoError(t, err, "should have no error")

	msgChan2, err := consumer2.Consume(context.TODO(), kafka.ConsumerGroupOption)
	assert.NoError(t, err, "should have no error")

	// Consumer2 should receive no message even if it's consuming from the same topic
	// Consumer2 should have a different partition than Consumer1
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func(<-chan messaging.Event) {
		assert.Equal(t, 0, len(msgChan2), "Channel should have no message")
		wg2.Done()
	}(msgChan2)
	wg2.Wait()

	// Close
	err = consumer1.Close()
	assert.NoError(t, err, "should have no error")
	err = consumer2.Close()
	assert.NoError(t, err, "should have no error")

	// Restart consumer1
	consumer1, err = kafka.NewConsumer(topic, consumerGroupId, kafka.WithBrokers(broker))
	assert.NoError(t, err, "should have no error")

	msgChan1, err = consumer1.Consume(context.TODO(), kafka.ConsumerGroupOption)
	assert.NoError(t, err, "should have no error")

	wg1.Add(1)
	go func(<-chan messaging.Event) {
		for i := range msgChan1 {
			assert.Equal(t, msgContent, i, "Message should be original messsage")
			spew.Dump(i)
			break
		}
		wg1.Done()
	}(msgChan1)
	wg1.Wait()

	// Close
	err = consumer1.Close()
	assert.NoError(t, err, "should have no error")
	err = producer.Close()
	assert.NoError(t, err, "should have no error")
}

func TestConsumerInitialOffsetSetToNewest(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	topic := "topic_TestConsumerInitialOffset"
	broker := "kafka1:9093"
	producer, err := kafka.Producer(topic, kafka.StrategyRoundRobin, broker)
	assert.NoError(t, err, "Failed to create Producer")

	// Produce old message
	msgKeyOld := "key_old_message"
	msgValueOld := []byte("value_old_message")
	msgOld, err := kafka.NewMessage(msgKeyOld, msgValueOld)
	assert.NoError(t, err, "Failed to create kafka message")
	err = producer.Produce(context.TODO(), msgOld)
	assert.NoError(t, err, "Failed to produce message to topic")

	// Produce new message
	msgKeyNew := "key_new_message"
	msgValueNew := "value_old_message"
	msgNew, err := kafka.NewMessage(msgKeyNew, []byte(msgValueNew))
	assert.NoError(t, err, "Failed to create kafka message")
	err = producer.Produce(context.TODO(), msgNew)
	assert.NoError(t, err, "Failed to produce message to topic")

	consumerGroupId := "consumerGroup_TestConsumerInitialOffsetSetToNewest"
	consumer, err := kafka.NewConsumer(topic, consumerGroupId, kafka.WithBrokers(broker), kafka.WithInitialOffset(kafka.OffsetNewest))
	assert.NoError(t, err, "Failed to create new consumer")
	msgChan, err := consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
	assert.NoError(t, err, "Failed to consume from topic")

	kafkaEvent := <-msgChan
	kafkaMsg := kafkaEvent.Raw().(*kafka.Message)
	assert.Equal(t, msgKeyNew, string(kafkaMsg.Key))
	assert.Equal(t, msgValueNew, kafkaMsg.Value)

	err = consumer.Close()
	assert.NoError(t, err, "Failed to close consumer")
}

func TestConsumerInitialOffsetSetToOldest(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	topic := "topic_TestConsumerInitialOffset"
	broker := "kafka1:9093"
	producer, err := kafka.Producer(topic, kafka.StrategyRoundRobin, broker)
	assert.NoError(t, err, "Failed to create Producer")

	// Produce old message
	msgKeyOld := "key_old_message"
	msgValueOld := []byte("value_old_message")
	msgOld, err := kafka.NewMessage(msgKeyOld, msgValueOld)
	assert.NoError(t, err, "Failed to create kafka message")
	err = producer.Produce(context.TODO(), msgOld)
	assert.NoError(t, err, "Failed to produce message to topic")

	// Produce new message
	msgKeyNew := "key_new_message"
	msgValueNew := []byte("value_old_message")
	msgNew, err := kafka.NewMessage(msgKeyNew, msgValueNew)
	assert.NoError(t, err, "Failed to create kafka message")
	err = producer.Produce(context.TODO(), msgNew)
	assert.NoError(t, err, "Failed to produce message to topic")

	consumerGroupId := "consumerGroup_TestConsumerInitialOffsetSetToNewest"
	consumer, err := kafka.NewConsumer(topic, consumerGroupId, kafka.WithBrokers(broker), kafka.WithInitialOffset(kafka.OffsetOldest))
	assert.NoError(t, err, "Failed to create new consumer")
	msgChan, err := consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
	assert.NoError(t, err, "Failed to consume from topic")

	kafkaEvent0 := <-msgChan
	kafkaMsg0 := kafkaEvent0.Raw().(*kafka.Message)
	assert.Equal(t, msgKeyOld, string(kafkaMsg0.Key))
	assert.Equal(t, msgValueOld, kafkaMsg0.Value)

	kafkaEvent1 := <-msgChan
	kafkaMsg1 := kafkaEvent1.Raw().(*kafka.Message)
	assert.Equal(t, msgKeyNew, string(kafkaMsg1.Key))
	assert.Equal(t, msgValueNew, kafkaMsg1.Value)

	err = consumer.Close()
	assert.NoError(t, err, "Failed to close consumer")
}
