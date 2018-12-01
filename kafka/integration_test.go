// +build integration

package kafka_test

import (
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/veritone/go-messaging-lib/kafka"
)

// Test whether consumer try to connect to all brokers. Verify by log
func Test_ConsumerConnect(t *testing.T) {
	kafka.SetLogger(log.New(os.Stdout, "[sarama] ", log.LstdFlags))
	multiBrokerSetup(t)
	defer tearDown(t)

	for j := 0; j < 10; j++ {
		var brokers []string
		firstPort := 9093
		for i := 0; i < 15; i++ {
			brokers = append(brokers, strconv.FormatInt(int64(i), 10))
			brokers[i] = "kafka" + strconv.FormatInt(int64(i), 10) + ":" + strconv.FormatInt(int64(firstPort), 10)
			firstPort++
		}
		kafka.NewConsumer("t1", "g1", kafka.WithBrokers(brokers...))
	}
}
