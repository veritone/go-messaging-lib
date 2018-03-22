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
	// RawStreamType - string ID of RawStream type
	RawStreamType = "raw_stream"

	// MaxBytes - max number of raw bytes contained in RawStream message
	MaxBytes = 100
)

// RawStream - container for raw bytes from a stream
type RawStream struct {
	Key   int64 // message counter
	Value RawStreamVal
}

// RawStreamVal - container for RawStream value to produce to Kafka
type RawStreamVal struct {
	Type         string // RawStreamType
	TimestampUTC int64  // milliseconds since epoch
	Bytes        []byte // raw bytes
}

// EmptyRawStream - create an empty RawStream message
func EmptyRawStream() RawStream {
	return RawStream{
		Value: RawStreamVal{
			Type:         RawStreamType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomRawStream - create a RawStream message with random values
func RandomRawStream() RawStream {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	bytes := make([]byte, rand.Intn(MaxBytes))
	rand.Read(bytes)
	return RawStream{
		Key: rand.Int63(),
		Value: RawStreamVal{
			Type:         RawStreamType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
			Bytes:        bytes,
		},
	}
}

// ToKafka - encode from RawStream message to Kafka message
func (m RawStream) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToRawStream - decode from Kafka message to RawStream message
func ToRawStream(k *kafka.Message) (RawStream, error) {
	msg := RawStream{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for RawStream: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for RawStream: %v", err)
	}
	return msg, nil
}
