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
	// OutputAppendedType - string ID of OutputAppended type
	OutputAppendedType = "output_appended"
)

// OutputAppended - container for raw bytes from a stream
type OutputAppended struct {
	Key   string // JobID
	Value OutputAppendedVal
}

// OutputAppendedVal - container for OutputAppended value to produce to Kafka
type OutputAppendedVal struct {
	Type         string // OutputAppendedType
	TimestampUTC int64  // milliseconds since epoch
	TaskID       string // Task ID
	TDOID        string // TDO ID
	JobID        string // Job ID
}

// EmptyOutputAppended - create an empty OutputAppended message
func EmptyOutputAppended() OutputAppended {
	return OutputAppended{
		Value: OutputAppendedVal{
			Type:         OutputAppendedType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomOutputAppended - create a OutputAppended message with random values
func RandomOutputAppended() OutputAppended {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	jobID := uuidv4()
	return OutputAppended{
		Key: jobID,
		Value: OutputAppendedVal{
			Type:         OutputAppendedType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
			TaskID:       uuidv4(),
			TDOID:        uuidv4(),
			JobID:        jobID,
		},
	}
}

// ToKafka - encode from OutputAppended message to Kafka message
func (m OutputAppended) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToOutputAppended - decode from Kafka message to OutputAppended message
func ToOutputAppended(k *kafka.Message) (OutputAppended, error) {
	msg := OutputAppended{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for OutputAppended: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for OutputAppended: %v", err)
	}
	return msg, nil
}
