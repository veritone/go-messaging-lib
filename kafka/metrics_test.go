package kafka_test

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/veritone/go-messaging-lib/kafka"
)

const (
	endpoint = "http://localhost:8000/metrics"
)

func init() {
	kafka.SetLogger(log.New(os.Stdout, "[sarama] ", log.LstdFlags))

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe("localhost:8000", nil)
	}()
}

func TestEmitRequestMetrics(t *testing.T) {
	t.Run("testCreateTopics", testCreateTopics)
	t.Run("testDeleteTopics", testDeleteTopics)
}

func testCreateTopics(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	admin, err := kafka.Manager(kafkaHost)
	assert.Nil(t, err, "There should be no error")

	opts := kafka.CreateTopicOptions{
		NumPartitions:     2000,
		ReplicationFactor: 1,
		Timeout:           1 * time.Millisecond,
	}
	topic := "topic_test"
	err = admin.CreateTopics(context.Background(), opts, topic)

	resp, err := http.Get(endpoint)
	assert.Nil(t, err, "There should be no error")
	assert.Equal(t, 200, resp.StatusCode, "Statuscode should be 200")

	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err, "There should be no error")

	body := string(bodyBytes)

	expectedMetric1 := "kafka_admin_latency{"
	expectedMetric2 := "kafka_admin_error{"
	expectedMethod := "method=\"CreateTopics\""
	assert.Contains(t, body, expectedMetric1)
	assert.Contains(t, body, expectedMetric2)
	assert.Contains(t, body, expectedMethod)
}

func testDeleteTopics(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	admin, err := kafka.Manager(kafkaHost)
	assert.Nil(t, err, "There should be no error")

	topic := "topic_test"
	admin.DeleteTopics(context.Background(), topic)

	resp, err := http.Get(endpoint)
	assert.Nil(t, err, "There should be no error")
	assert.Equal(t, 200, resp.StatusCode, "Statuscode should be 200")

	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err, "There should be no error")

	body := string(bodyBytes)

	expectedMetric1 := "kafka_admin_latency{"
	expectedMetric2 := "kafka_admin_error{"
	expectedMethod := "method=\"DeleteTopics\""
	assert.Contains(t, body, expectedMetric1)
	assert.Contains(t, body, expectedMetric2)
	assert.Contains(t, body, expectedMethod)
}
