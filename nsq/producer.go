package nsq

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	gnsq "github.com/nsqio/go-nsq"
	"github.com/veritone/core-messages/generated/go/events"
	messaging "github.com/veritone/go-messaging-lib"
	"go.uber.org/zap"
)

type NsqProducer struct {
	nsqp *gnsq.Producer
	*messaging.Tracer
	messaging.Logger
}

// NewProducer returns an nsq producer. This is a light wrapper for Producer constructor that
// aims for backward compatibility with old `go-messaging` repo`
func NewProducer(config *Config) (*NsqProducer, error) {
	conf := gnsq.NewConfig()
	conf.MaxInFlight = config.MaxInFlight
	p, err := gnsq.NewProducer(config.Nsqd, conf)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{
		nsqp:   p,
		Tracer: messaging.MustAddTracer("", ""),
		Logger: messaging.MustAddLogger(config.LogLevel),
	}, p.Ping()
}

func Producer(host string) (*NsqProducer, error) {
	config := gnsq.NewConfig()
	p, err := gnsq.NewProducer(host, config)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{
		nsqp:   p,
		Tracer: messaging.MustAddTracer("", ""),
		Logger: messaging.MustAddLogger("info"),
	}, p.Ping()
}

func (p *NsqProducer) Produce(_ context.Context, m messaging.Messager, from ...messaging.Event) error {
	msg, ok := m.Message().(*messager)
	if !ok {
		return fmt.Errorf("unsupported nsq message: %s", spew.Sprint(msg))
	}
	var payload []byte
	var eventName string
	vtEvent := p.Decorate(msg.raw, msg.payload)
	if vtEvent != nil {
		if len(from) > 0 {
			fromEvent := &events.VtEvent{}
			payload = from[0].Payload()
			err := proto.Unmarshal(payload, fromEvent)
			if err == nil {
				p.Tracer.Trace(vtEvent, fromEvent)
			} else {
				p.Warn("message cannot be unmarshal into vtEvent", zap.ByteString("payload", payload))
				p.Tracer.Trace(vtEvent, nil)
			}
		} else {
			p.Tracer.Trace(vtEvent, nil)
		}
	} else {
		p.Debug("message cannot be decorated", zap.ByteString("message", msg.payload))
	}

	if newPayload, err := proto.Marshal(vtEvent); err == nil {
		payload = newPayload
		eventName = messaging.NameFromEvent(newPayload)
	} else {
		p.Debug("unable to marshal protobuf payload",
			zap.String("data", spew.Sprint(vtEvent)),
			zap.Error(err))
		payload = msg.payload
	}
	p.Debug("sending message",
		zap.String("topic", msg.topic),
		zap.ByteString("payload", payload))
	err := p.nsqp.Publish(msg.topic, msg.payload)
	messagesProcessed.WithLabelValues(msg.topic, "producer", "", eventName).Inc()
	bytesProcessed.WithLabelValues(msg.topic, "producer", "", eventName).Add(float64(len(msg.payload)))
	if err != nil {
		errorsCount.WithLabelValues(msg.topic, "producer", "", eventName).Inc()
	}
	return err
}

func (p *NsqProducer) Close() error {
	p.nsqp.Stop()
	return nil
}
