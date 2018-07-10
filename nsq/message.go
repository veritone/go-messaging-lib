package nsq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
)

type messager struct {
	raw     interface{}
	payload []byte
	topic   string
}

func (k *messager) Message() interface{} {
	return k
}

// NewMessage creates message that is publishable. Client should pass in []byte as payload
// for better write throughput
func NewMessage(topic string, message interface{}) (messaging.Messager, error) {
	var (
		payload []byte
		err     error
	)
	switch v := message.(type) {
	case proto.Message:
		payload, err = proto.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("cannot convert protobuf message to bytes %v", err)
		}
	case []byte:
		payload = v
	default:
		payload, err = getBytes(v)
		if err != nil {
			return nil, fmt.Errorf("cannot convert message to bytes %v", err)
		}
	}
	return &messager{
		topic:   topic,
		payload: payload,
		raw:     message,
	}, nil
}

func getBytes(payload interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type ConsumerOptions struct {
	AutoFinish bool
}

// NsqConsumerOption is the default for NSQ
var DefaultOptions messaging.OptionCreator = &ConsumerOptions{}

func (o *ConsumerOptions) Options() interface{} {
	return o
}

type NsqEvent struct {
	topic   string
	channel string
	*gnsq.Message
}

func (e *NsqEvent) Requeue(delay time.Duration) {
	requeueCount.WithLabelValues(e.topic, "consumer", e.channel, messaging.NameFromEvent(e.Body)).Inc()
	e.Message.Requeue(delay)
}

func (e *NsqEvent) Finish() {
	messagesProcessed.WithLabelValues(e.topic, "consumer", e.channel, messaging.NameFromEvent(e.Body)).Inc()
	e.Message.Finish()
}

func (e *NsqEvent) Payload() []byte {
	return e.Body
}

func (e *NsqEvent) Metadata() map[string]interface{} {
	return map[string]interface{}{
		"timestamp":   e.Timestamp,
		"NSQDAddress": e.NSQDAddress,
		"id":          e.ID,
		"attemps":     e.Attempts,
	}
}

func (e *NsqEvent) Raw() interface{} {
	return e
}
