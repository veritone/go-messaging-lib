package nsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	messaging "github.com/veritone/go-messaging-lib"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"
)

type NsqProducerGroup struct {
	nsqProducers []*NsqProducer
	*messaging.Tracer
	messaging.Logger
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

type wrappedResp struct {
	Status     string      `json:"status_txt"`
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data"`
}

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

// NewProducer returns a group of nsq producers. This is a light wrapper for Producer constructor
// This allows for finer control over which nsqd to produce to
func NewProducerGroup(config *Config) (*NsqProducerGroup, error) {
	if len(config.Nsqlookupds) == 0 {
		return nil, errors.New("no nsqlookupds in config")
	}

	var producers []*NsqProducer
	ProducerGroup := &NsqProducerGroup{
		nsqProducers: producers,
		Tracer:       messaging.MustAddTracer("", ""),
		Logger:       messaging.MustAddLogger(config.LogLevel),
	}

	nsqds := ProducerGroup.queryLookupd(config.Nsqlookupds)

	for _, nsqd := range nsqds {
		config.Nsqd = nsqd
		p, err := NewProducer(config)
		if err != nil {
			continue
		}
		ProducerGroup.nsqProducers = append(ProducerGroup.nsqProducers, p)
	}

	if len(producers) == 0 {
		if len(config.Nsqlookupds) == 0 {
			return nil, errors.New("cannot connect to any nsqd")
		}
	}

	return &NsqProducerGroup{
		nsqProducers: producers,
		Tracer:       messaging.MustAddTracer("", ""),
		Logger:       messaging.MustAddLogger(config.LogLevel),
	}, nil
}

func (p *NsqProducerGroup) Produce(ctx context.Context, m messaging.Messager, from ...messaging.Event) error {
	nsqd := p.nsqProducers[rand.Intn(len(p.nsqProducers))]
	err := nsqd.Produce(ctx, m)
	return err
}

func (p *NsqProducerGroup) Close() error {
	for _, producer := range p.nsqProducers {
		producer.nsqp.Stop()
	}
	return nil
}

func (p *NsqProducerGroup) queryLookupd(Nsqlookupds []string) []string {
	retries := 0
	i := 0
retry:
	var data lookupResp
	for ; i < len(Nsqlookupds); i++ {
		endpoint := Nsqlookupds[i]
		err := apiRequestNegotiate("GET", endpoint, nil, &data)
		if err != nil {
			p.Error("error querying nsqlookupd ", zap.ByteString("endpoint", endpoint))
			retries++
			if retries < 3 {
				p.Info("retrying with next nsqlookupd")
				goto retry
			}
			return []string{}
		}
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

func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

func apiRequestNegotiate(method string, endpoint string, body io.Reader, ret interface{}) error {
	httpclient := &http.Client{Transport: newDeadlineTransport(2 * time.Second)}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("got response %s %q", resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}

	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		return json.Unmarshal(respBody, ret)
	}

	wResp := &wrappedResp{
		Data: ret,
	}

	if err = json.Unmarshal(respBody, wResp); err != nil {
		return err
	}

	// wResp.StatusCode here is equal to resp.StatusCode, so ignore it
	return nil
}
