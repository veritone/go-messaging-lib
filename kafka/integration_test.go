// +build integration

package kafka_test

import (
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/veritone/go-messaging-lib/kafka"
)

// Run test by `go test -v -tags=integration -run ^TestConsumerConnect`

// Test whether consumer try to connect to all brokers. Verify by log
func TestConsumerConnect(t *testing.T) {
	kafka.SetLogger(log.New(os.Stdout, "[sarama] ", log.LstdFlags))
	multiBrokerSetup(t)
	defer tearDown(t)

	var brokers []string
	port := 9093
	for i := 0; i < 15; i++ {
		brokers = append(brokers, strconv.FormatInt(int64(i), 10))
		brokers[i] = "kafka" + strconv.FormatInt(int64(i), 10) + ":" + strconv.FormatInt(int64(port), 10)
		port++
	}
	consumer, err := kafka.NewConsumer("t1", "g1", kafka.WithBrokers(brokers...))
	assert.Nil(t, err, "should have no error")
	err = consumer.Close()
	assert.Nil(t, err, "should have no error")
}
