package kafka

import (
	messaging "github.com/veritone/go-messaging-lib"
)

// This interface is for kafka usage specifically. This is to give further details on kafka consumer's specific implementations

// ClientConsumer defines interface for kafka consumer
type ClientConsumer interface {
	messaging.Consumer
	MarkOffset(messaging.Event, string) error
}
