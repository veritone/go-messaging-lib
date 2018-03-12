package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	_ "net/http/pprof"

	"flag"

	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafkaGo "github.com/segmentio/kafka-go"
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
	http.HandleFunc("/bench-pub", kafkaMiddleware(benchPub))
	http.HandleFunc("/bench-sub", kafkaMiddleware(benchSub))
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
			q.Set("kafka_host", "localhost")
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
	duration := time.Second * 60

	if len(durationParam) > 0 {
		parsedDuration, err := time.ParseDuration(durationParam)
		if err == nil {
			duration = parsedDuration
		}
	}

	producer := kafka.Producer(topic, kafka.StrategyRoundRobin, "kafka1:9092")
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			asyncProduce(producer, duration)
			wg.Done()
		}()
	}
	wg.Wait()
	_, err := rw.Write([]byte("OK"))
	if err != nil {
		log.Panic(err)
	}
}

func asyncProduce(producer messaging.Producer, duration time.Duration) {
	timer := time.NewTimer(duration)
	ticker := time.NewTicker(time.Millisecond * 500)
	fakeMsg := make([]byte, 10)
ProducerLoop:
	for {
		select {
		case <-ticker.C:
			var msg messaging.Messager
			var e error
			msg, e = kafka.NewMessage(
				"",
				fakeMsg)
			if e != nil {
				log.Panic(e)
			}
			e = producer.Produce(context.Background(), msg)
			if e != nil {
				log.Panic(e)
			}
		case <-timer.C:
			break ProducerLoop
			// default:
			// 	var msg messaging.Messager
			// 	var e error
			// 	msg, e = kafka.NewMessage(
			// 		"",
			// 		fakeMsg)
			// 	if e != nil {
			// 		log.Panic(e)
			// 	}
			// 	e = producer.Produce(context.Background(), msg)
			// 	if e != nil {
			// 		log.Panic(e)
			// 	}
		}
	}
	err := producer.Close()
	if err != nil {
		log.Panic(err)
	}
}

func benchSub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")

	consumer := kafka.Consumer(topic, "", "kafka1:9092")
	consumers = append(consumers, consumer)
	queue, err := consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
	if err != nil {
		log.Panic(err)
	}
	for item := range queue {
		_, ok := item.(*kafkaGo.Message)
		if !ok {
			spew.Dump(item)
			// If not your type, either ignore or forward to another queue
		}
	}
	_, err = rw.Write([]byte("started a bench-consumer"))
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

func sub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	group := q.Get("group")
	consumer := kafka.Consumer(topic, group, "kafka1:9092")
	consumers = append(consumers, consumer)
	queue, err := consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
	if err != nil {
		log.Panic(err)
	}
	_, err = rw.Write([]byte("started a consumer"))
	if err != nil {
		log.Panic(err)
	}
	for item := range queue {
		v, ok := item.(*kafkaGo.Message)
		if ok {
			log.Printf("ok: (%d) (%s) (%s)\n", v.Offset, v.Value, v.Time)
		} else {
			log.Println("NOT OK")
			// spew.Dump(item)
			// If not your type, either ignore or forward to another queue
		}
		time.Sleep(time.Second)
	}
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
}
