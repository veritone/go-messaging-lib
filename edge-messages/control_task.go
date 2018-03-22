package messages

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/veritone/go-messaging-lib"
)

const (
	// ControlTaskType - string ID of ControlTask type
	ControlTaskType = "control_task"

	// Engine control commands
	ControlCommandLaunch = "LAUNCH"
	ControlCommandKill   = "KILL"
)

// ControlTask - container for raw bytes from a stream
type ControlTask struct {
	Key   string // Engine ID
	Value ControlTaskVal
}

// ControlTaskVal - container for ControlTask value to produce to Kafka
type ControlTaskVal struct {
	Type           string // ControlTaskType
	TimestampUTC   int64  // milliseconds since epoch
	TaskID         string // Task ID
	JobID          string // Job ID
	EngineID       string // Engine ID
	EngineImageURI string // Engine Container Image URI
	Payload        string // Payload use to start up engine
	Command        string // Launch or Kill engine
}

// EmptyControlTask - create an empty ControlTask message
func EmptyControlTask() ControlTask {
	return ControlTask{
		Value: ControlTaskVal{
			Type:         ControlTaskType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomControlTask - create a ControlTask message with random values
func RandomControlTask() ControlTask {
	return ControlTask{
		Key: uuidv4(),
		Value: ControlTaskVal{
			Type:           ControlTaskType,
			TimestampUTC:   int64(time.Now().UnixNano() / 1000),
			TaskID:         uuidv4(),
			JobID:          uuidv4(),
			EngineID:       uuidv4(),
			EngineImageURI: "dockerhub.com/" + uuidv4(),
			Payload:        "some payload",
			Command:        ControlCommandLaunch,
		},
	}
}

// ToKafka - encode from ControlTask message to Kafka message
func (m ControlTask) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToControlTask - decode from Kafka message to ControlTask message
func ToControlTask(k *kafka.Message) (ControlTask, error) {
	msg := ControlTask{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for ControlTask: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for ControlTask: %v", err)
	}
	return msg, nil
}
