package nsq

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
)

type NsqProducer struct {
	nsqp *gnsq.Producer
}

// NewProducer returns an nsq producer. This is a light wrapper for Producer constructor that
// aims for backward compatibility with old `go-messaging` repo`
func NewProducer(config *Config) (*NsqProducer, error) {
	return Producer(config.Nsqd)
}

func Producer(host string) (*NsqProducer, error) {
	config := gnsq.NewConfig()
	p, err := gnsq.NewProducer(host, config)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{p}, p.Ping()
}

func (p *NsqProducer) Produce(_ context.Context, m messaging.Messager) error {
	msg, ok := m.Message().(*messager)
	if !ok {
		return fmt.Errorf("unsupported nsq message: %s", spew.Sprint(msg))
	}
	return p.nsqp.Publish(msg.topic, msg.payload)
}

func (p *NsqProducer) Close() error {
	p.nsqp.Stop()
	return nil
}
