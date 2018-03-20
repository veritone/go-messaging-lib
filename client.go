package messaging

import (
	"context"
	"io"
)

// Manager provides basic administrative functions for the messaging system
type Manager interface {
	ListTopics(context.Context) (interface{}, error)
	CreateTopics(context.Context, ...string) error
	DeleteTopics(context.Context, ...string) error
	io.Closer
}

// Producer defines functions of a producer/publisher
type Producer interface {
	Produce(context.Context, Messager) error
	io.Closer
}

// Messager defines a contract for creating a compatible message type
type Messager interface {
	Message() interface{}
}

// Consumer defines functions of a consumer/subscriber
type Consumer interface {
	Consume(context.Context, OptionCreator) (<-chan interface{}, error)
	io.Closer
}

// OptionCreator defines a contract for making metadata/options for consumer
type OptionCreator interface {
	Options() interface{}
}

// StreamReader reads stuff from a stream
type StreamReader interface {
	io.ReadCloser
}

// StreamWriter writes things to a stream
type StreamWriter interface {
	io.WriteCloser
}
