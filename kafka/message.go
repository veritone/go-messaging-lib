package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"

	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

type messager struct {
	message *gKafka.Message
}

func (k *messager) Message() interface{} {
	return k.message
}

// NewMessage creates message that is publishable. Client should pass in []byte as payload
// for better write throughput
func NewMessage(key string, payload interface{}) (messaging.Messager, error) {
	var (
		msg []byte
		err error
	)
	switch v := payload.(type) {
	case []byte:
		msg = v
	default:
		msg, err = getBytes(v)
		if err != nil {
			return nil, fmt.Errorf("cannot convert message to bytes %v", err)
		}
	}
	return &messager{&gKafka.Message{
		Key:   []byte(key),
		Value: msg,
	}}, nil
}

func getBytes(payload interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type consumerOptions struct {
	Partition int32
	Offset    int64
}

// NewConsumerOption specifies consumer policies
func NewConsumerOption(partition int32, offset int64) messaging.OptionCreator {
	return &consumerOptions{partition, offset}
}

// ConsumerGroupOption is the default for Consumer Group. In this configuration,
// partition and offset are ignored since they are automatically managed by kafka
var ConsumerGroupOption messaging.OptionCreator = &consumerOptions{}

func (o *consumerOptions) Options() interface{} {
	return o
}
