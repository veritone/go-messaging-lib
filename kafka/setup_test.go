package kafka_test

import (
	"io"
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/fuzzdota/wfi"
)

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

func setup(t *testing.T) {
	logs, err := wfi.UpWithLogs("./test", "docker-compose.kafka.yaml")
	if err != nil {
		t.Error(err)
	}
	if _, err = wfi.Find("connected to kafka1:9093", logs, time.Second*20); err != nil {
		t.Errorf(`"connected to kafka1:9093" phrase should show up within 20s, %v`, err)
	}
}

func tearDown(t *testing.T) {
	wfi.Down("./test", "docker-compose.kafka.yaml")
}

func Err(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}
