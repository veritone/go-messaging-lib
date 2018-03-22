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
	// EngineOutputType - string ID of EngineOutput type
	EngineOutputType = "engine_output"
)

// EngineOutput - container for raw bytes from a stream
type EngineOutput struct {
	Key   string // EngineOutputType
	Value EngineOutputVal
}

// EngineOutputVal - container for EngineOutput value to produce to Kafka
type EngineOutputVal struct {
	Type          string // EngineOutputType
	TimestampUTC  int64  // milliseconds since epoch
	TaskID        string // Task ID
	TDOID         string // TDO ID
	JobID         string // Job ID
	MIMEType      string // The MIME type of engine output data
	StartOffsetMs int32  // Offset of start of media chunk from beginning of TDO
	EndOffsetMs   int32  // Offset of end of media chunk from beginning of TDO
	Content       string // Data value of the engine output from engine standard
	Rev           int32  // Revision number that is incremented each time the engine output is updated
	ChunkUUID     string // UUID of this chunk (to distinguish one chunk from another)
}

// EmptyEngineOutput - create an empty EngineOutput message
func EmptyEngineOutput() EngineOutput {
	return EngineOutput{
		Key: EngineOutputType,
		Value: EngineOutputVal{
			Type:         EngineOutputType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomEngineOutput - create a EngineOutput message with random values
func RandomEngineOutput() EngineOutput {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	return EngineOutput{
		Key: EngineOutputType,
		Value: EngineOutputVal{
			Type:          EngineOutputType,
			TimestampUTC:  int64(time.Now().UnixNano() / 1000),
			TaskID:        uuidv4(),
			TDOID:         uuidv4(),
			JobID:         uuidv4(),
			MIMEType:      "application/json",
			StartOffsetMs: rand.Int31(),
			EndOffsetMs:   rand.Int31(),
			Content:       "some content",
			Rev:           rand.Int31(),
			ChunkUUID:     uuidv4(),
		},
	}
}

// ToKafka - encode from EngineOutput message to Kafka message
func (m EngineOutput) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToEngineOutput - decode from Kafka message to EngineOutput message
func ToEngineOutput(k *kafka.Message) (EngineOutput, error) {
	msg := EngineOutput{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for EngineOutput: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for EngineOutput: %v", err)
	}
	return msg, nil
}
