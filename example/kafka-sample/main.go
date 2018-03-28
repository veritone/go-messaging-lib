package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	messaging "github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/kafka"
)

// HelpMessage displays help command
const HelpMessage = `                                                                                                                               
_         ___  _                                 _      
| |_  ___ |  _|| |_  ___    ___  ___  _____  ___ | | ___ 
| '_|| .'||  _|| '_|| .'|  |_ -|| .'||     || . || || -_|
|_,_||__,||_|  |_,_||__,|  |___||__,||_|_|_||  _||_||___|
                                            |_|          

The purpose of this service is to provide guidance on how to properly ultize 
go-messaging-lib features and showcase real integration and benchmarking with kafka framework

`

var consumers []messaging.Consumer

func main() {

	portPtr := flag.String("p", "8080", "http port")
	flag.Parse()

	fmt.Println(HelpMessage)
	http.HandleFunc("/", intro)
	http.HandleFunc("/pub", kafkaMiddleware(pub))
	http.HandleFunc("/sub", kafkaMiddleware(sub))
	http.HandleFunc("/admin/topics", kafkaMiddleware(list))
	http.HandleFunc("/admin/create", kafkaMiddleware(createTopics))
	http.HandleFunc("/bench-pub", kafkaMiddleware(benchPub))
	http.HandleFunc("/shutdown", kafkaMiddleware(shutdown))
	http.Handle("/metrics", promhttp.Handler())
	log.Panic(http.ListenAndServe(":"+*portPtr, nil))

}

func intro(rw http.ResponseWriter, r *http.Request) {
	if _, err := rw.Write([]byte(HelpMessage)); err != nil {
		log.Panic(err)
	}
	rw.WriteHeader(http.StatusOK)
}

func kafkaMiddleware(f http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		log.Println(r.URL.String())
		q := r.URL.Query()
		kafkaHost := q.Get("kafka_host")
		kafkaPort := q.Get("kafka_port")
		if len(kafkaHost) == 0 {
			q.Set("kafka_host", "kafka1")
		}
		if len(kafkaPort) == 0 {
			q.Set("kafka_port", "9092")
		}
		r.URL.RawQuery = q.Encode()
		f(rw, r)
	}
}

func benchPub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	durationParam := q.Get("duration")
	rateParam := q.Get("rate")
	threadCountParam := q.Get("thread")

	// Default values
	duration := time.Second * 60
	threadCount := 4 // Typical 4 core machine
	rate := time.Millisecond

	if len(durationParam) > 0 {
		parsedDuration, err := time.ParseDuration(durationParam)
		if err == nil {
			duration = parsedDuration
		}
	}

	if len(rateParam) > 0 {
		parsedDuration, err := time.ParseDuration(rateParam)
		if err == nil {
			rate = parsedDuration
		}
	}

	parsedCC, err := strconv.Atoi(threadCountParam)
	if err == nil && parsedCC > 0 {
		threadCount = parsedCC
	}

	producer := kafka.Producer(topic, kafka.StrategyRoundRobin, "kafka1:9092")
	var wg sync.WaitGroup

	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			asyncProduce(producer, duration, rate)
			wg.Done()
		}()
	}
	wg.Wait()
	_, err = rw.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func asyncProduce(producer messaging.Producer, duration, rate time.Duration) {
	timer := time.NewTimer(duration)
	ticker := time.NewTicker(rate)
	fakeMsg := make([]byte, 1e3) // 1KB
	var msg messaging.Messager
	var e error
	msg, e = kafka.NewMessage(
		"",
		fakeMsg)
	if e != nil {
		log.Panic(e)
	}
ProducerLoop:
	for {
		select {
		case <-ticker.C:
			e = producer.Produce(context.Background(), msg)
			if e != nil {
				log.Panic(e)
			}
			log.Println("sent " + time.Now().String())
		case <-timer.C:
			break ProducerLoop
		}
	}
	err := producer.Close()
	if err != nil {
		log.Panic(err)
	}
}

func pub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	message := q.Get("message")

	producer := kafka.Producer(topic, kafka.StrategyRoundRobin, "kafka1:9092")
	msg, err := kafka.NewMessage("", []byte(message))
	if err != nil {
		log.Panic(err)
	}
	err = producer.Produce(context.TODO(), msg)
	if err != nil {
		log.Panic(err)
	}
	err = producer.Close()
	if err != nil {
		log.Panic(err)
	}
	_, err = rw.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

// sub subscribes to a topic and optionally consumer group
func sub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	partition := q.Get("partition")
	group := q.Get("group")
	var (
		consumer messaging.Consumer
		err      error
		queue    <-chan messaging.Event
	)
	if len(group) == 0 {
		p, _ := strconv.Atoi(partition)
		consumer, err = kafka.ConsumerFromParition(topic, p, "kafka1:9092")
		if err != nil {
			log.Panic(err)
		}
		queue, err = consumer.Consume(context.TODO(), kafka.NewConsumerOption(kafka.OffsetNewest))
		if err != nil {
			log.Panic(err)
		}
		log.Printf("consuming from partition %d\n", p)
	} else {
		consumer, err = kafka.Consumer(topic, group, "kafka1:9092")
		if err != nil {
			log.Panic(err)
		}
		queue, err = consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("consuming from group %s\n", group)
	}
	consumers = append(consumers, consumer)
	_, err = rw.Write([]byte("started a consumer"))
	if err != nil {
		log.Panic(err)
	}
	for item := range queue {
		log.Printf("ok: (%s) (%#v) (%T)\n", item.Payload(), item.Metadata(), item.Raw())
	}
}

// list topics and metadata
func list(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")
	spew.Dump(host, port)
	manager, err := kafka.Manager(host + ":" + port)
	if err != nil {
		log.Panic(err)
	}
	data, err := manager.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	if err = manager.Close(); err != nil {
		log.Panic(err)
	}

	// the library exposes ListTopicsResponse type for casting
	if v, ok := data.(kafka.ListTopicsResponse); ok {
		log.Println("got ListTopicsResponse")
		_ = v
		// You can either cast it and access the data here directly e.g
		// 	info = v[topic_name][group_name][partition_number]
		// or just dump it as json like below
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Panic(err)
	}
	_, err = rw.Write(jsonData)
	if err != nil {
		log.Panic(err)
	}
}

func createTopics(rw http.ResponseWriter, r *http.Request) {
	// ----------- Setup -----------------------
	q := r.URL.Query()
	partition, err := strconv.Atoi(q.Get("partition"))
	if err != nil {
		partition = 1
	}
	replicationFactor, err := strconv.Atoi(q.Get("replication"))
	if err != nil {
		replicationFactor = 1
	}
	topic := q.Get("topic")
	if len(topic) == 0 {
		rw.Write([]byte("cannot have empty topic"))
		return
	}

	// ------- Create Topics -----------------
	m, err := kafka.Manager("kafka1:9092")
	if err != nil {
		log.Panic(err)
	}
	if err = m.CreateTopics(context.Background(), kafka.CreateTopicOptions{
		NumPartitions:     int32(partition),
		ReplicationFactor: int16(replicationFactor),
	}, topic); err != nil {
		log.Panic(err)
	}
	if err = m.Close(); err != nil {
		log.Panic(err)
	}
	rw.Write([]byte("createTopic OK"))
}

func shutdown(rw http.ResponseWriter, r *http.Request) {
	for _, consumer := range consumers {
		err := consumer.Close()
		if err != nil {
			log.Println("error closing out consumer:", err)
		} else {
			log.Println("consumer closed")
		}
	}
	consumers = nil
}
