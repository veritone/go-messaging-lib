package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

// OffsetOldest lets consumers to retrieve the oldest possible message
const OffsetOldest = -1

// OffsetNewest lets consumers to retrieve the newest possible message
const OffsetNewest = -2

type event struct {
	*gKafka.Message
}

func (e *event) Payload() []byte {
	return e.Value
}

func (e *event) Metadata() map[string]interface{} {
	return map[string]interface{}{
		"key":       e.Key,
		"offset":    e.Offset,
		"partition": e.Partition,
		"time":      e.Time,
		"topic":     e.Topic,
	}
}

func (e *event) Raw() interface{} {
	return e.Message
}

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
		Time:  time.Now(),
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
	Offset int64
}

// NewConsumerOption specifies consumer policies. Pass in either OffsetOldest, OffsetNewest,
// or specific offset that you want to consumer from
func NewConsumerOption(offset int64) messaging.OptionCreator {
	return &consumerOptions{offset}
}

// ConsumerGroupOption is the default for Consumer Group. In this configuration,
// partition and offset are ignored since they are automatically managed by kafka
var ConsumerGroupOption messaging.OptionCreator = &consumerOptions{}

func (o *consumerOptions) Options() interface{} {
	return o
}
