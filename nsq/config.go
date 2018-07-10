package nsq

import "time"

// Config is the superset of configuration options for all messaging queues.
// this is the exact copy of old `go-messaging` repo
type Config struct {
	Name                string   `json:"name" yaml:"name"`
	Nsqd                string   `json:"nsqd" yaml:"nsqd"`
	NsqLogDir           string   `json:"nsqLogDirectory" yaml:"nsqLogDirectory"`
	Nsqlookupds         []string `json:"nsqlookupds" yaml:"nsqlookupds"`
	NsqlookupdDiscovery bool     `json:"nsqlookupdDiscovery" yaml:"nsqlookupdDiscovery"`
	MaxInFlight         int      `json:"maxInFlight" yaml:"maxInFlight"`
	ConcurrentHandlers  int      `json:"concurrentHandlers" yaml:"concurrentHandlers"`
	MsgTimeout          int      `json:"msgTimeout" yaml:"msgTimeout"` // seconds
	MsgTimeoutDuration  time.Duration

	// New fields
	LogLevel string `json:"logLevel" yaml:"logLevel"`
}
