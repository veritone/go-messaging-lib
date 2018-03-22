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
	// EngineHeartbeatType - string ID of EngineHeartbeat type
	EngineHeartbeatType = "engine_heartbeat"

	// Engine statuses
	EngineStatusRunning = "RUNNING"
	EngineStatusDone    = "DONE"
	EngineStatusFailed  = "FAILED"
)

// EngineHeartbeat - container for raw bytes from a stream
type EngineHeartbeat struct {
	Key   string // Engine instance ID
	Value EngineHeartbeatVal
}

// EngineHeartbeatVal - container for EngineHeartbeat value to produce to Kafka
type EngineHeartbeatVal struct {
	Type             string // EngineHeartbeatType
	TimestampUTC     int64  // milliseconds since epoch
	TaskID           string // Task ID
	TDOID            string // TDO ID
	JobID            string // Job ID
	Count            int32  // The heartbeat number
	Status           string // Status to report
	BytesRead        int64  // Cummulative number of bytes read (for stream engines)
	BytesWritten     int64  // Cummulative number of bytes written (for stream engines)
	MessageCount     int32  // Cummulative number of messages processed (for chunk engines)
	MessageSuccesses int32  // Cummulative number of messages processed successfully (for chunk engines)
	MessageErrors    int32  // Cummulative number of messages processed unsuccessfully (for chunk engines)
	CreatedTime      int64  // Engine instance created time (seconds since epoch)
	UpTime           int32  // Engine instance continuous up time (seconds since CreatedTime)
}

// EmptyEngineHeartbeat - create an empty EngineHeartbeat message
func EmptyEngineHeartbeat() EngineHeartbeat {
	return EngineHeartbeat{
		Value: EngineHeartbeatVal{
			Type:         EngineHeartbeatType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomEngineHeartbeat - create a EngineHeartbeat message with random values
func RandomEngineHeartbeat() EngineHeartbeat {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	return EngineHeartbeat{
		Key: uuidv4(),
		Value: EngineHeartbeatVal{
			Type:             EngineHeartbeatType,
			TimestampUTC:     int64(time.Now().UnixNano() / 1000),
			TaskID:           uuidv4(),
			TDOID:            uuidv4(),
			JobID:            uuidv4(),
			Count:            rand.Int31(),
			Status:           EngineStatusRunning,
			BytesRead:        rand.Int63(),
			BytesWritten:     rand.Int63(),
			MessageCount:     rand.Int31(),
			MessageSuccesses: rand.Int31(),
			MessageErrors:    rand.Int31(),
			CreatedTime:      int64(time.Now().UnixNano() / 1000),
			UpTime:           rand.Int31(),
		},
	}
}

// ToKafka - encode from EngineHeartbeat message to Kafka message
func (m EngineHeartbeat) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToEngineHeartbeat - decode from Kafka message to EngineHeartbeat message
func ToEngineHeartbeat(k *kafka.Message) (EngineHeartbeat, error) {
	msg := EngineHeartbeat{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for EngineHeartbeat: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for EngineHeartbeat: %v", err)
	}
	return msg, nil
}
