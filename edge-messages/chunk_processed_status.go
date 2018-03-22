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
	// ChunkProcessedStatusType - string ID of ChunkProcessedStatus type
	ChunkProcessedStatusType = "chunk_processed_status"

	// Statuses
	ChunkProcessedStatusSuccess = "SUCCESS"
	ChunkProcessedStatusError   = "ERROR"
)

// ChunkProcessedStatus - container for raw bytes from a stream
type ChunkProcessedStatus struct {
	Key   string // ChunkProcessedStatusType
	Value ChunkProcessedStatusVal
}

// ChunkProcessedStatusVal - container for ChunkProcessedStatus value to produce to Kafka
type ChunkProcessedStatusVal struct {
	Type         string // ChunkProcessedStatusType
	TimestampUTC int64  // milliseconds since epoch
	TaskID       string // Task ID the chunk belongs to
	ChunkUUID    string // UUID of chunk for which status is being reported
	Status       string // Processed status
	ErrorMsg     string // Optional error message in case of ERROR status
	InfoMsg      string // Optional message for anything engine wishes to report
}

// EmptyChunkProcessedStatus - create an empty ChunkProcessedStatus message
func EmptyChunkProcessedStatus() ChunkProcessedStatus {
	return ChunkProcessedStatus{
		Key: ChunkProcessedStatusType,
		Value: ChunkProcessedStatusVal{
			Type:         ChunkProcessedStatusType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomChunkProcessedStatus - create a ChunkProcessedStatus message with random values
func RandomChunkProcessedStatus() ChunkProcessedStatus {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	return ChunkProcessedStatus{
		Key: ChunkProcessedStatusType,
		Value: ChunkProcessedStatusVal{
			Type:         ChunkProcessedStatusType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
			TaskID:       uuidv4(),
			ChunkUUID:    uuidv4(),
			Status:       randomChunkStatus(),
			ErrorMsg:     "something",
			InfoMsg:      "blahblahblah",
		},
	}
}

// ToKafka - encode from ChunkProcessedStatus message to Kafka message
func (m ChunkProcessedStatus) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToChunkProcessedStatus - decode from Kafka message to ChunkProcessedStatus message
func ToChunkProcessedStatus(k *kafka.Message) (ChunkProcessedStatus, error) {
	msg := ChunkProcessedStatus{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for ChunkProcessedStatus: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for ChunkProcessedStatus: %v", err)
	}
	return msg, nil
}

func randomChunkStatus() string {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	switch rand.Intn(2) {
	case 0:
		return ChunkProcessedStatusSuccess
	default:
		return ChunkProcessedStatusError
	}
}
