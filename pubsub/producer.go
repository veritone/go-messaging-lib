package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	ps "cloud.google.com/go/pubsub"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	events "github.com/veritone/core-messages/generated/go/events"
	messaging "github.com/veritone/go-messaging-lib"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

type PubSubProducer struct {
	*ps.Topic
	*ps.Client
	*messaging.Tracer
	messaging.Logger
}

func NewProducer(ctx context.Context, topic, projectId, credentialFile string) (*PubSubProducer, error) {
	c, err := ps.NewClient(ctx,
		projectId,
		option.WithCredentialsFile(credentialFile))
	if err != nil {
		return nil, fmt.Errorf("error creating Producer Client %v", err)
	}
	t := c.TopicInProject(topic, projectId)
	tExist, err := t.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("error checking for topic exsistence %v", err)
	}
	if !tExist {
		t, err = c.CreateTopic(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("error creating topic %s, %v", topic, err)
		}
	}
	return &PubSubProducer{
		Topic:  t,
		Client: c,
		Logger: messaging.MustAddLogger(""),
		Tracer: messaging.MustAddTracer("", ""),
	}, nil
}

func (p *PubSubProducer) Produce(ctx context.Context, m messaging.Messager, from ...messaging.Event) error {
	msg, ok := m.Message().(*messager)
	if !ok {
		return errors.New("message is not compatible")
	}
	var payload []byte
	var eventName string
	vtEvent := p.Decorate(msg.raw, msg.payload)
	if vtEvent != nil {
		if len(from) > 0 {
			fromEvent := &events.VtEvent{}
			muhPayload := from[0].Payload()
			err := proto.Unmarshal(muhPayload, fromEvent)
			if err == nil {
				p.Tracer.Trace(vtEvent, fromEvent)
			} else {
				p.Warn("message cannot be unmarshal into vtEvent",
					zap.ByteString("payload", muhPayload))
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
	p.Debug("sending message", zap.String("event", eventName), zap.ByteString("payload", payload))
	result := p.Topic.Publish(ctx, &ps.Message{
		Data: payload,
	})
	messageLatest.WithLabelValues("producer", p.ID(), "", eventName).Set(float64(time.Now().UnixNano()) / 1e9)
	messagesProcessed.WithLabelValues("producer", p.ID(), "", eventName).Inc()
	bytesProcessed.WithLabelValues("producer", p.ID(), "", eventName).Add(float64(len(msg.payload)))
	_, err := result.Get(ctx)
	if err != nil {
		errorsCount.WithLabelValues("producer", p.ID(), "", eventName).Inc()
		errorLatest.WithLabelValues("producer", p.ID(), "", eventName).Set(float64(time.Now().UnixNano()) / 1e9)
	}
	return err
}

func (p *PubSubProducer) Close() error {
	p.Topic.Stop()
	return p.Client.Close()
}
