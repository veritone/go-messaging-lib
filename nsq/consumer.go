package nsq

import (
	"context"
	"errors"

	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
)

type NsqConsumer struct {
	nsqc        *gnsq.Consumer
	nsqds       []string
	nsqlookupds []string
}

// NewConsumer returns an nsq consumer. This is a light wrapper for Consumer constructor that
// aims for backward compatibility with old `go-messaging` repo`
func NewConsumer(topic, channel string, config *Config) (*NsqConsumer, error) {
	conf := gnsq.NewConfig()
	conf.MaxInFlight = config.MaxInFlight
	c, err := gnsq.NewConsumer(topic, channel, conf)
	if err != nil {
		return nil, err
	}
	if config.Nsqd == "" && len(config.Nsqlookupds) == 0 {
		return nil, errors.New("must supply either nsqd or nsqlookup addresses")
	}
	return &NsqConsumer{c, []string{config.Nsqd}, config.Nsqlookupds}, nil
}

func Consumer(topic, channel string, nsqds, nsqlookupds []string) (*NsqConsumer, error) {
	conf := gnsq.NewConfig()
	c, err := gnsq.NewConsumer(topic, channel, conf)
	if err != nil {
		return nil, err
	}
	if len(nsqds) == 0 && len(nsqlookupds) == 0 {
		return nil, errors.New("must supply either nsqd or nsqlookup addresses")
	}
	return &NsqConsumer{c, nsqds, nsqlookupds}, nil
}

func (c *NsqConsumer) Consume(_ context.Context, _ messaging.OptionCreator) (<-chan messaging.Event, error) {
	msgs := make(chan messaging.Event, 1)
	c.nsqc.AddHandler(gnsq.HandlerFunc(func(m *gnsq.Message) error {
		msgs <- &event{m}
		return nil
	}))
	if len(c.nsqds) > 0 {
		if err := c.nsqc.ConnectToNSQDs(c.nsqds); err != nil {
			return nil, err
		}
	} else if len(c.nsqlookupds) > 0 {
		if err := c.nsqc.ConnectToNSQLookupds(c.nsqlookupds); err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

func (c *NsqConsumer) Close() error {
	c.nsqc.Stop()
	return nil
}
