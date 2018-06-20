package pubsub

import (
	"encoding/json"
	"fmt"

	ps "cloud.google.com/go/pubsub"
	"github.com/golang/protobuf/proto"
	messaging "github.com/veritone/go-messaging-lib"
)

type messager struct {
	payload []byte
	raw     interface{}
}

func (k *messager) Message() interface{} {
	return k
}

// NewMessage creates message that is publishable. Client should pass in []byte as payload
// for better write throughput
func NewMessage(payload interface{}) (messaging.Messager, error) {
	var (
		msg []byte
		err error
	)
	switch v := payload.(type) {
	case proto.Message:
		msg, err = proto.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("cannot convert protobuf message to bytes %v", err)
		}
	case []byte:
		msg = v
	default:
		msg, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("cannot convert message to bytes %v", err)
		}
	}
	return &messager{msg, payload}, nil
}

type PubSubConsumerOptions struct {
	ManualAck bool
}

func (o *PubSubConsumerOptions) Options() interface{} {
	return o
}

type event struct {
	*ps.Message
}

func (e *event) Payload() []byte {
	return e.Data
}

func (e *event) Metadata() map[string]interface{} {
	return map[string]interface{}{
		"timestamp":  e.PublishTime,
		"attributes": e.Attributes,
		"id":         e.ID,
	}
}

func (e *event) Raw() interface{} {
	return e.Message
}
