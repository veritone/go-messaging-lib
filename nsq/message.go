package nsq

import (
	"bytes"
	"encoding/gob"
	"fmt"

	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
)

type messager struct {
	payload []byte
	topic   string
}

func (k *messager) Message() interface{} {
	return k
}

// NewMessage creates message that is publishable. Client should pass in []byte as payload
// for better write throughput
func NewMessage(topic string, payload interface{}) (messaging.Messager, error) {
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
	return &messager{
		topic:   topic,
		payload: msg,
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

type consumerOptions struct {
	AutoAck bool
}

// NsqConsumerOption is the default for NSQ
var NsqConsumerOption messaging.OptionCreator = &consumerOptions{}

func (o *consumerOptions) Options() interface{} {
	return o
}

type NsqEvent struct {
	*gnsq.Message
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
	return e.Message
}