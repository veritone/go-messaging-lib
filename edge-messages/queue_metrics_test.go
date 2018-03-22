package messages

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestQueueMetrics(t *testing.T) {
	// create random QueueMetrics message
	origMsg := RandomQueueMetrics()

	// encode for producing to Kafka
	msg, err := origMsg.ToKafka()
	assert.Nil(t, err)

	// extract Kafka message
	kafkaMsg, ok := msg.Message().(*kafka.Message)
	assert.True(t, ok)

	// decode back to QueueMetrics message
	newMsg, err := ToQueueMetrics(kafkaMsg)
	assert.Nil(t, err)

	// verify decode produced the original msg before encode
	assert.Equal(t, origMsg, newMsg)
}
