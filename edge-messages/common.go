package messages

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
	"github.com/veritone/go-messaging-lib"
)

// ValidMessageTypes - array contains all valid message types for Edge messages
// Note: new message types should be added here, ordering doesn't matter
var ValidMessageTypes = [...]string{
	RawStreamType,
	StreamEOFType,
	MediaChunkType,
	EngineOutputType,
	OutputAppendedType,
	EngineHeartbeatType,
	ControlTaskType,
	QueueMetricsType,
	ChunkProcessedStatusType,
}

// messager - implements Message() interface using kafka-go concrete implementation
type messager struct {
	message *kafka.Message
}

func (k *messager) Message() interface{} {
	return k.message
}

// newMessage - creates new message suitable for producing to Kafka
func newMessage(key interface{}, value interface{}) (messaging.Messager, error) {
	// encode key
	keyBytes, err := encode(key)
	if err != nil {
		return nil, fmt.Errorf("cannot convert message key to bytes %v", err)
	}

	// encode value
	valBytes, err := encode(value)
	if err != nil {
		return nil, fmt.Errorf("cannot convert message value to bytes %v", err)
	}

	return &messager{&kafka.Message{
		Time:  time.Now(),
		Key:   keyBytes,
		Value: valBytes,
	}}, nil
}

// encode - encode any payload to []byte suitable for producing to Kafka
func encode(payload interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// msgTypeOnly - temp struct for getting/testing message type
type msgTypeOnly struct {
	Type string
}

// GetMsgType - given a Kafka message, determine what type of Edge message it is
func GetMsgType(k *kafka.Message) (string, error) {
	m := msgTypeOnly{}
	buf := bytes.NewBuffer(k.Value)
	err := gob.NewDecoder(buf).Decode(&m)
	if err != nil {
		return "", fmt.Errorf("error getting type for message: msg = %v, err = %v", *k, err)
	}
	return m.Type, nil
}

func uuidv4() string {
	id, err := uuid.NewV4()
	if err != nil {
		return "12345678-90ab-cdef-1234-567809ab"
	}
	return id.String()
}
