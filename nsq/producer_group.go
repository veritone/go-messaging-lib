package nsq

import (
	gnsq "github.com/nsqio/go-nsq"
	messaging "github.com/veritone/go-messaging-lib"
	"math/rand"
)

type NsqProducerGroup struct {
	nsqps []*gnsq.Producer
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

// NewProducer returns a group of nsq producers. This is a light wrapper for Producer constructor
// This allows for finer control over which nsqd to produce to
func NewProducerGroup(config *Config) (*NsqProducerGroup, error) {
	conf := gnsq.NewConfig()
	conf.MaxInFlight = config.MaxInFlight

	if len(config.Nsqlookupds) == 0 {
		return nil, errors.New("no nsqlookupds in config")
	}


	
	p, err := gnsq.NewProducer(config.Nsqd, conf)
	if err != nil {
		return nil, err
	}

	producers = []*gnsq.Producer
	
	return &NsqProducer{
		nsqp:   p,
		Tracer: messaging.MustAddTracer("", ""),
		Logger: messaging.MustAddLogger(config.LogLevel),
	}, p.Ping()
}

func ProducerGroup(host string) (*NsqProducer, error) {
	
}

func (p *NsqProducerGroup) Produce(_ context.Context, m messaging.Messager, from ...messaging.Event) error {
	nsqd := nsqps[rand.Intn(len(nsqps))]
	nsqd.produce()
}

func (p *NsqProducerGroup) Close() error {
	for _, nsqp := range nsqps {
		nsqp.Stop()
	}
	return nil
}

func (r *NsqProducerGroup) queryLookupd(Nsqlookupds []string) []string {
	retries := 0
retry:
	var data lookupResp
	err := gnsq.apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			r.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	return nsqdAddrs
}
