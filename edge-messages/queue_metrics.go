package messages

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/veritone/go-messaging-lib"
)

const (
	// QueueMetricsType - string ID of QueueMetrics type
	QueueMetricsType = "queue_metrics"
)

// QueueMetrics - container for raw bytes from a stream
type QueueMetrics struct {
	Key   string // topic
	Value QueueMetricsVal
}

// QueueMetricsVal - container for QueueMetrics value to produce to Kafka
type QueueMetricsVal struct {
	Type          string // QueueMetricsType
	TimestampUTC  int64  // milliseconds since epoch
	Topic         string // Topic for which metrics are being reported
	TopicDepth    int32  // Number of unprocessed messages in topic
	EngineID      string // Engine ID
	ConsumerGroup string // Consumer group for which metrics are being reported
}

// EmptyQueueMetrics - create an empty QueueMetrics message
func EmptyQueueMetrics() QueueMetrics {
	return QueueMetrics{
		Value: QueueMetricsVal{
			Type:         QueueMetricsType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomQueueMetrics - create a QueueMetrics message with random values
func RandomQueueMetrics() QueueMetrics {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	return QueueMetrics{
		Key: uuidv4(),
		Value: QueueMetricsVal{
			Type:          QueueMetricsType,
			TimestampUTC:  int64(time.Now().UnixNano() / 1000),
			Topic:         "test",
			TopicDepth:    rand.Int31(),
			EngineID:      uuidv4(),
			ConsumerGroup: "group1",
		},
	}
}

// ToKafka - encode from QueueMetrics message to Kafka message
func (m QueueMetrics) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToQueueMetrics - decode from Kafka message to QueueMetrics message
func ToQueueMetrics(k *kafka.Message) (QueueMetrics, error) {
	msg := QueueMetrics{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for QueueMetrics: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for QueueMetrics: %v", err)
	}
	return msg, nil
}
