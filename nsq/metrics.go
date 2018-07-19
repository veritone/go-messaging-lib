package nsq

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesProcessed    *prometheus.CounterVec
	messagesProcessed *prometheus.CounterVec
	errorsCount       *prometheus.CounterVec
	requeueCount      *prometheus.CounterVec
)

func init() {
	bytesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messaging_nsq_message_processed_bytes",
			Help: "How many bytes processed, partitioned by topic, type (consumer/producer), channel, and eventName.",
		},
		[]string{"topic", "type", "channel", "event_name"},
	)

	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messaging_nsq_finished_total",
			Help: "How many messages marked as finish, partitioned by topic, type (consumer/producer), channel, and eventName.",
		},
		[]string{"topic", "type", "channel", "event_name"},
	)

	errorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messaging_nsq_errors_total",
			Help: "How many errors encountered so far, partitioned by topic, type (consumer/producer), channel, and eventName.",
		},
		[]string{"topic", "type", "channel", "event_name"},
	)

	requeueCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messaging_nsq_requeue_total",
			Help: "How many messages got requeued, partitioned by topic, type (consumer/producer), channel, and eventName.",
		},
		[]string{"topic", "type", "channel", "event_name"},
	)

	prometheus.MustRegister(bytesProcessed, messagesProcessed, errorsCount, requeueCount)
}
