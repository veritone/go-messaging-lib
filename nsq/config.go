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
	MsgTimeoutDuration  time.Duration
	MsgTimeout          int      `json:"msgTimeout" yaml:"msgTimeout"`           // seconds
	TLS                 bool     `json:"tls" yaml:"tls"`                         // Bool enable TLS negotiation
	TLSVerification     bool     `json:"tlsVerification" yaml:"tlsVerification"` // Bool indicates whether this client should verify server certificates
	TLSRootCAFile       *string  `json:"tlsRootCAFile" yaml:"tlsRootCAFile"`     // String path to file containing root CA
	TLSCert             *string  `json:"tlsCert" yaml:"tlsCert"`                 // String path to file containing public key for certificate
	TLSKey              *string  `json:"tlsKey" yaml:"tlsKey"`                   // String path to file containing private key for certificate
	TLSMinVersion       *string  `json:"tlsMinVersion" yaml:"tlsMinVersion"`     // String indicating the minimum version of tls acceptable ('ssl3.0', 'tls1.0', 'tls1.1', 'tls1.2')

	// New fields
	LogLevel string `json:"logLevel" yaml:"logLevel"`
}
