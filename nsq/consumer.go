package nsq

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/davecgh/go-spew/spew"

	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
	"go.uber.org/zap"
)

const (
	// NsqlookupdDNS defines the default DNS for nsqlookupd
	NsqlookupdDNS = "nsqlookupd.service.consul"
	NsqlookupPort = 4161
)

type NsqConsumer struct {
	nsqc        *gnsq.Consumer
	nsqds       []string
	nsqlookupds []string
	topic       string
	channel     string
	*messaging.Tracer
	messaging.Logger
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

	if config.NsqlookupdDiscovery {
		config.Nsqlookupds, err = nsqlookupdsFromDNS()
		if err != nil {
			return nil, errors.New("unable to discover nsqlookupds")
		}
	}

	if config.TLS {
		// Using set instead of creating a new tls.Config because of complex handling of each property
		// See: https://github.com/nsqio/go-nsq/blob/master/config.go#L401
		if err := conf.Set("tls_insecure_skip_verify", !config.TLSVerification); err != nil {
			return nil, err
		}

		if config.TLSRootCAFile != nil {
			if err := conf.Set("tls_root_ca_file", config.TLSRootCAFile); err != nil {
				return nil, err
			}
		}

		if config.TLSCert != nil {
			if err := conf.Set("tls_cert", config.TLSCert); err != nil {
				return nil, err
			}

		}

		if config.TLSKey != nil {
			if err := conf.Set("tls_key", config.TLSKey); err != nil {
				return nil, err
			}
		}

		if config.TLSMinVersion != nil {
			if err := conf.Set("tls_min_version", config.TLSMinVersion); err != nil {
				return nil, err
			}
		}
	}

	return &NsqConsumer{
		nsqc:        c,
		nsqds:       []string{config.Nsqd},
		nsqlookupds: config.Nsqlookupds,
		topic:       topic,
		channel:     channel,
		Tracer:      messaging.MustAddTracer("", ""),
		Logger:      messaging.MustAddLogger(config.LogLevel),
	}, nil
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

	return &NsqConsumer{
		nsqc:        c,
		nsqds:       nsqds,
		nsqlookupds: nsqlookupds,
		topic:       topic,
		channel:     channel,
		Tracer:      messaging.MustAddTracer("", ""),
		Logger:      messaging.MustAddLogger("info"),
	}, nil
}

func (c *NsqConsumer) Consume(_ context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	nsqOpts, ok := opts.(*ConsumerOptions)
	if !ok {
		return nil, fmt.Errorf("consumer option is invalid, %s", spew.Sprint(opts))
	}
	msgs := make(chan messaging.Event, 1)
	c.nsqc.AddHandler(gnsq.HandlerFunc(func(m *gnsq.Message) error {
		event := &NsqEvent{
			topic:   c.topic,
			channel: c.channel,
			Message: m,
		}
		msgs <- event
		c.Debug("received message",
			zap.Uint16("attempts", m.Attempts),
			zap.ByteString("payload", m.Body))
		bytesProcessed.WithLabelValues(c.topic, "consumer", c.channel, messaging.NameFromEvent(m.Body)).Add(float64(len(m.Body)))
		if nsqOpts.AutoFinish {
			event.Finish()
		}
		return nil
	}))

	if len(c.nsqlookupds) > 0 {
		if err := c.nsqc.ConnectToNSQLookupds(c.nsqlookupds); err != nil {
			return nil, err
		}
	} else if len(c.nsqds) > 0 {
		if err := c.nsqc.ConnectToNSQDs(c.nsqds); err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

func (c *NsqConsumer) Close() error {
	c.nsqc.Stop()
	return nil
}

func nsqlookupdsFromDNS() ([]string, error) {
	addrs := []string{}

	ips, err := net.LookupIP(NsqlookupdDNS)
	if err != nil {
		return addrs, err
	}

	for _, ip := range ips {
		addr := fmt.Sprintf("%s:%d", ip, NsqlookupPort)
		addrs = append(addrs, addr)
	}

	return addrs, nil
}
