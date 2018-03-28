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

func Producer(host string) *NsqProducer {
	config := gnsq.NewConfig()
	p, err := gnsq.NewProducer(host, config)
	if err != nil {
		return nil
	}
	return &NsqProducer{p}
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
