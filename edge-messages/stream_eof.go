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
	// StreamEOFType - string ID of StreamEOF type
	StreamEOFType = "stream_eof"
)

// StreamEOF - container for raw bytes from a stream
type StreamEOF struct {
	Key   string // StreamEOFType
	Value StreamEOFVal
}

// StreamEOFVal - container for StreamEOF value to produce to Kafka
type StreamEOFVal struct {
	Type         string // StreamEOFType
	TimestampUTC int64  // milliseconds since epoch
	TaskID       string // Task ID
	TDOID        string // TDO ID
	JobID        string // Job ID
}

// EmptyStreamEOF - create an empty StreamEOF message
func EmptyStreamEOF() StreamEOF {
	return StreamEOF{
		Key: StreamEOFType,
		Value: StreamEOFVal{
			Type:         StreamEOFType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomStreamEOF - create a StreamEOF message with random values
func RandomStreamEOF() StreamEOF {
	return StreamEOF{
		Key: StreamEOFType,
		Value: StreamEOFVal{
			Type:         StreamEOFType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
			TaskID:       uuidv4(),
			TDOID:        uuidv4(),
			JobID:        uuidv4(),
		},
	}
}

// ToKafka - encode from StreamEOF message to Kafka message
func (m StreamEOF) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToStreamEOF - decode from Kafka message to StreamEOF message
func ToStreamEOF(k *kafka.Message) (StreamEOF, error) {
	msg := StreamEOF{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for StreamEOF: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for StreamEOF: %v", err)
	}
	return msg, nil
}
