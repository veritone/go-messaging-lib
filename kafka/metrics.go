package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	gKafka "github.com/segmentio/kafka-go"
)

var (
	bytesProcessed    *prometheus.CounterVec
	messagesProcessed *prometheus.CounterVec
	errorsCount       *prometheus.CounterVec

	offsetMetrics *prometheus.GaugeVec
	lagMetrics    *prometheus.GaugeVec
)

func init() {
	bytesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_message_processed_bytes",
			Help: "How many bytes processed, partitioned by topic, type (consumer/writer), and partition.",
		},
		[]string{"topic", "type", "partition"},
	)
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "How many messages processed, partitioned by topic, type (consumer/writer), and partition.",
		},
		[]string{"topic", "type", "partition"},
	)
	errorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_errors_total",
			Help: "How many errors encountered so far, partitioned by topic, type (consumer/writer), and partition.",
		},
		[]string{"topic", "type", "partition"},
	)

	offsetMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_offsets_total",
			Help: "Current offset - partitioned by topic, type (consumer/writer), and partition.",
		},
		[]string{"topic", "type", "partition"},
	)
	lagMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_lags_total",
			Help: "Current lag (how far behind) - partitioned by topic, type (comsumer/writer), and partition.",
		},
		[]string{"topic", "type", "partition"},
	)

	prometheus.MustRegister(bytesProcessed, messagesProcessed, errorsCount, offsetMetrics, lagMetrics)
}

// monitorConsumer exposes metric
func monitorConsumer(r *gKafka.Reader, updateInterval time.Duration) (ticker *time.Ticker) {
	ticker = time.NewTicker(updateInterval)
	go func() {
		for range ticker.C {
			s := r.Stats()
			bytesProcessed.WithLabelValues(s.Topic, "consumer", s.Partition).Add(float64(s.Bytes))
			messagesProcessed.WithLabelValues(s.Topic, "consumer", s.Partition).Add(float64(s.Messages))
			errorsCount.WithLabelValues(s.Topic, "consumer", s.Partition).Add(float64(s.Errors))

			offsetMetrics.WithLabelValues(s.Topic, "consumer", s.Partition).Set(float64(s.Offset))
			lagMetrics.WithLabelValues(s.Topic, "consumer", s.Partition).Set(float64(s.Lag))
		}
	}()
	return
}

// monitorProducer exposes metric
func monitorProducer(w *gKafka.Writer, updateInterval time.Duration) (ticker *time.Ticker) {
	ticker = time.NewTicker(updateInterval)
	go func() {
		for range ticker.C {
			s := w.Stats()
			bytesProcessed.WithLabelValues(s.Topic, "producer", "").Add(float64(s.Bytes))
			messagesProcessed.WithLabelValues(s.Topic, "producer", "").Add(float64(s.Messages))
			errorsCount.WithLabelValues(s.Topic, "producer", "").Add(float64(s.Errors))
		}
	}()
	return
}
