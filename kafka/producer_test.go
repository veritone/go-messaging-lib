package kafka_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	messaging "github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/kafka"
)

const kafkaHost = "kafka1:9093"

func Test_producer_integration(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)
	testProducer(t, "t1", kafka.StrategyHash) // test with Hash Strategy
	//testProducer(t, "t2", kafka.StrategyLeastBytes) // test with LeastBytes
	testProducer(t, "t3", kafka.StrategyRoundRobin) // test with Round Robin
	testProducer(t, "t1", kafka.StrategyRoundRobin) // test on existing topic

}

func testProducer(t *testing.T, topic string, s kafka.Strategy) {
	p, err := kafka.Producer(topic, s, kafkaHost)
	assert.NoError(t, err, "should be able to create Producer")
	testSendingMessages(t, p)
	assert.NoError(t, p.Close())
}

func testSendingMessages(t *testing.T, p messaging.Producer) {
	var (
		err error
		m   messaging.Messager
	)
	m, err = kafka.NewMessage("", []byte("test_message"))
	assert.NoError(t, err)
	assert.NoError(t, p.Produce(context.TODO(), m), "should not return a error")
	m, err = kafka.NewMessage("test-key", []byte("test_message"))
	assert.NoError(t, err)
	assert.NoError(t, p.Produce(context.TODO(), m), "should not return a error")
	m, err = kafka.NewMessage("", []byte{})
	assert.NoError(t, err)
	assert.NoError(t, p.Produce(context.TODO(), m), "should not return a error")
	m, err = kafka.NewMessage("test-key", []byte{})
	assert.NoError(t, err)
	assert.NoError(t, p.Produce(context.TODO(), m), "should not return a error")
}
