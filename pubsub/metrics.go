package pubsub

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesProcessed    *prometheus.CounterVec
	messagesProcessed *prometheus.CounterVec
	errorsCount       *prometheus.CounterVec

	messageLatest *prometheus.GaugeVec
	errorLatest   *prometheus.GaugeVec
)

func init() {
	bytesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_message_processed_bytes",
			Help: "How many bytes processed, partitioned by type (consumer/producer), topic, and subscription.",
		},
		[]string{"type", "topic", "subscription", "event"},
	)
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_messages_total",
			Help: "How many messages processed, partitioned by type (consumer/producer), topic, and subscription.",
		},
		[]string{"type", "topic", "subscription", "event"},
	)
	errorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_errors_total",
			Help: "How many errors encountered so far, partitioned by type (consumer/producer), topic, and subscription.",
		},
		[]string{"type", "topic", "subscription", "event"},
	)
	messageLatest = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pubsub_message_latest_timestamp",
			Help: "Last timestamp of a message, partitioned by type (consumer/producer), topic, and subscription.",
		},
		[]string{"type", "topic", "subscription", "event"},
	)
	errorLatest = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pubsub_error_latest_timestamp",
			Help: "Last error encounterd, partitioned by type (consumer/producer), topic, and subscription.",
		},
		[]string{"type", "topic", "subscription", "event"},
	)
	prometheus.MustRegister(bytesProcessed, messagesProcessed, errorsCount, messageLatest, errorLatest)
}
