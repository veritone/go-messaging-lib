package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesProcessed    *prometheus.CounterVec
	messagesProcessed *prometheus.CounterVec
	messagesMarked    *prometheus.CounterVec
	errorsCount       *prometheus.CounterVec

	offsetMetrics *prometheus.GaugeVec
	lagMetrics    *prometheus.GaugeVec

	adminLatency *prometheus.GaugeVec
	adminErr     *prometheus.CounterVec
)

const (
	adminMethodLabel = "method"
	adminErrLabel    = "error"
)

func init() {
	bytesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_message_processed_bytes",
			Help: "How many bytes processed, partitioned by topic, type (consumer/writer), groupId, and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "How many messages processed, partitioned by topic, type (consumer/writer), groupId, and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)
	messagesMarked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_marked",
			Help: "How many messages marked, partitioned by topic, type (consumer/writer), groupId, and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)
	errorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_errors_total",
			Help: "How many errors encountered so far, partitioned by topic, type (consumer/writer), groupId, and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)

	offsetMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_offsets_total",
			Help: "Current offset - partitioned by topic, type (consumer/writer), groupId,  and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)
	lagMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_lags_total",
			Help: "Current lag (how far behind) - partitioned by topic, type (comsumer/writer), groupId, and partition.",
		},
		[]string{"topic", "type", "group", "partition"},
	)

	adminLatency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_admin_latency",
			Help: "Admin request latency",
		},
		[]string{adminMethodLabel},
	)

	adminErr = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_admin_error",
			Help: "Admin error count",
		},
		[]string{adminMethodLabel, adminErrLabel},
	)

	prometheus.MustRegister(bytesProcessed, messagesProcessed, errorsCount, offsetMetrics, lagMetrics, adminLatency, adminErr)
}

// describes metric info to emit
type requestMetric struct {
	method    string
	startTime time.Time
	err       *error
}

func (metricObj *requestMetric) emit() {
	adminLatency.WithLabelValues(metricObj.method).Set(float64(time.Now().Sub(metricObj.startTime) / time.Millisecond))

	if *metricObj.err != nil {
		adminErr.WithLabelValues(metricObj.method, (*metricObj.err).Error()).Inc()
	}
}
