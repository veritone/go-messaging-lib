package kafka_test

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/fuzzdota/wfi"
	"github.com/stretchr/testify/assert"
	"github.com/veritone/go-messaging-lib/kafka"
)

func Test_manager(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, "create_topic_test_1", "create_topic_test_2")
	if err != nil {
		log.Panic(err)
	}
	res, err := m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	val, ok := res.(kafka.ListTopicsResponse)
	if !ok {
		Err(t, errors.New("invalid response from ListTopics"))
	}
	spew.Dump(val)
	p1, ok := val["create_topic_test_1"][""]
	assert.Equal(t, true, ok, "topic 'create_topic_test_1' should exist")
	assert.Equal(t, 5, len(p1), "number of partitions for 'create_topic_test_1' should be 5")

	p2, ok := val["create_topic_test_2"][""]
	assert.Equal(t, true, ok, "topic 'create_topic_test_2' should exist")
	assert.Equal(t, 5, len(p2), "number of partitions for 'create_topic_test_1' should be 5")
	Err(t, m.Close())
}

func multiBrokerSetup(t *testing.T) {
	logs, err := wfi.UpWithLogs("./test", "docker-compose.kafka.yaml")
	if err != nil {
		t.Error(err)
	}
	log1R, log1W := io.Pipe()
	log2R, log2W := io.Pipe()
	log3R, log3W := io.Pipe()

	go func() {
		defer log1W.Close()
		defer log2W.Close()
		defer log3W.Close()

		mw := io.MultiWriter(log1W, log2W, log3W)
		io.Copy(mw, logs)
	}()

	waitForIt := func(phrase string, l io.Reader, wg *sync.WaitGroup) {
		txt, err := wfi.Find(phrase, l, time.Second*20)
		if err != nil {
			t.Errorf(`"broker cannot connect within 20s, %v`, err)
		}
		log.Println("Broker started:", txt)
		wg.Done()
		ioutil.ReadAll(l)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go waitForIt("[KafkaServer id=1] started", log1R, &wg)
	go waitForIt("[KafkaServer id=2] started", log2R, &wg)
	go waitForIt("[KafkaServer id=3] started", log3R, &wg)
	wg.Wait()
}