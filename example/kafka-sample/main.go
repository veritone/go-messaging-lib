package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
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

Examples:
 * Publish message:
	> localhost:8080/pub?topic=test&message=example&kafka_port=9093

 * Subscribe to a topic:
	> localhost:8080/sub?topic=test&group=cg_test&kafka_port=9093
	> localhost:8080/sub?topic=test&partition=1&kafka_port=9093

 * List topics and lags:
 	> localhost:8080/admin/topics?kafka_port=9093

 * List topics and groups (lite version):
 	> localhost:8080/admin/topics-lite?kafka_port=9093

 * Create topics with partitions and replicas:
	> localhost:8080/admin/create?topic=new_topic&partition=2&replication=2&kafka_port=9093

 * Delete consumer groups:
	> localhost:8080

 * Clean up existing consumer groups
	> localhost:8080/shutdown

 * Metrics
	> localhost:8080/metrics
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
	http.HandleFunc("/admin/topics-lite", kafkaMiddleware(listLite))
	http.HandleFunc("/admin/create", kafkaMiddleware(createTopics))
	http.HandleFunc("/admin/delete/cg", kafkaMiddleware(deleteConsumerGroups))
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
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")

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

	producer, err := kafka.Producer(topic, kafka.StrategyRoundRobin, host+":"+port)
	if err != nil {
		log.Panic(err)
	}
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
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")

	producer, err := kafka.Producer(topic, kafka.StrategyHash, host+":"+port)
	if err != nil {
		log.Panic(err)
	}
	msg, err := kafka.NewMessage(strconv.Itoa(rand.Intn(50)), []byte(message))
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
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")
	var (
		consumer messaging.Consumer
		err      error
		queue    <-chan messaging.Event
	)
	if len(group) == 0 {
		p, _ := strconv.Atoi(partition)
		consumer, err = kafka.ConsumerFromPartition(topic, p, host+":"+port)
		if err != nil {
			log.Panic(err)
		}
		queue, err = consumer.Consume(context.TODO(), kafka.NewConsumerOption(kafka.OffsetNewest))
		if err != nil {
			log.Panic(err)
		}
		log.Printf("consuming from partition %d\n", p)
	} else {
		consumer, err = kafka.Consumer(topic, group, host+":"+port)
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
	log.Println("message queue has been closed")
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
	v, ok := data.(kafka.ListTopicsResponse)
	if ok {
		log.Println("got ListTopicsResponse")
		delete(v, "__consumer_offsets")
		// You can either cast it and access the data here directly e.g
		// 	info = v[topic_name][group_name][partition_number]
		// or just dump it as json like below
	}
	jsonData, err := json.Marshal(v)
	if err != nil {
		log.Panic(err)
	}
	_, err = rw.Write(jsonData)
	if err != nil {
		log.Panic(err)
	}
}

// list topics and metadata
func listLite(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")
	spew.Dump(host, port)
	manager, err := kafka.Manager(host + ":" + port)
	if err != nil {
		log.Panic(err)
	}
	topics, groups, err := manager.ListTopicsLite(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	if err = manager.Close(); err != nil {
		log.Panic(err)
	}

	type Response struct {
		Topics []string
		Groups []string
	}

	v := &Response{
		Topics: topics,
		Groups: groups,
	}

	jsonData, err := json.Marshal(v)
	if err != nil {
		log.Panic(err)
	}
	_, err = rw.Write(jsonData)
	if err != nil {
		log.Panic(err)
	}
}

func deleteConsumerGroups(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")
	groups := q.Get("groups")
	cgroups := strings.Split(groups, ",")
	manager, err := kafka.Manager(host + ":" + port)
	if err != nil {
		log.Panic(err)
	}
	err = manager.DeleteConsumerGroups(true, cgroups...)
	if err != nil {
		log.Panic(err)
	}
	if err = manager.Close(); err != nil {
		log.Panic(err)
	}
}

func createTopics(rw http.ResponseWriter, r *http.Request) {
	// ----------- Setup -----------------------
	q := r.URL.Query()
	host := q.Get("kafka_host")
	port := q.Get("kafka_port")
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
	m, err := kafka.Manager(host + ":" + port)
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
