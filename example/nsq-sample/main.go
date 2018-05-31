package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/davecgh/go-spew/spew"

	"flag"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	messaging "github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/nsq"
)

// HelpMessage displays help command
const HelpMessage = `    
 
|￣￣￣￣￣￣|
| NSQ Sample |
|____________|
(\__/) || 
(•ㅅ•) || 
/ 　 づ  
The purpose of this service is to provide guidance on how to properly ultize 
go-messaging-lib features and showcase real integration and benchmarking with nsq eventing system
`

var consumers []messaging.Consumer

func main() {

	portPtr := flag.String("p", "8080", "http port")
	flag.Parse()

	fmt.Println(HelpMessage)
	http.HandleFunc("/", intro)
	http.HandleFunc("/pub", mw(pub))
	http.HandleFunc("/sub", mw(sub))
	http.HandleFunc("/shutdown", mw(shutdown))
	http.Handle("/metrics", promhttp.Handler())
	log.Panic(http.ListenAndServe(":"+*portPtr, nil))

}

func intro(rw http.ResponseWriter, r *http.Request) {
	if _, err := rw.Write([]byte(HelpMessage)); err != nil {
		log.Panic(err)
	}
	rw.WriteHeader(http.StatusOK)
}

func mw(f http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		log.Println(r.URL.String())
		q := r.URL.Query()
		host := q.Get("nsq_host")
		port := q.Get("nsq_port")
		if len(host) == 0 {
			q.Set("nsq_host", "localhost")
		}
		if len(port) == 0 {
			q.Set("nsq_port", "4150")
		}
		r.URL.RawQuery = q.Encode()
		f(rw, r)
	}
}

func pub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	message := q.Get("message")

	producer, err := nsq.Producer("localhost:4150")
	if err != nil {
		log.Panic(err)
	}
	msg, err := nsq.NewMessage(topic, []byte(message))
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

// sub subscribes to a topic and channel
func sub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	channel := q.Get("channel")
	var (
		consumer messaging.Consumer
		err      error
		queue    <-chan messaging.Event
	)
	consumer, err = nsq.Consumer(topic, channel, nil, []string{"localhost:4161"})
	if err != nil {
		log.Panic(err)
	}
	queue, err = consumer.Consume(context.TODO(), nsq.NsqConsumerOption)
	if err != nil {
		log.Panic(err)
	}
	consumers = append(consumers, consumer)
	_, err = rw.Write([]byte("started a consumer"))
	if err != nil {
		log.Panic(err)
	}
	for item := range queue {
		log.Printf("Payload: %s\n", item.Payload())
		log.Printf("Metadata: %#v\n", item.Metadata())
		log.Printf("Rawdata: %s\n", spew.Sdump(item.Raw()))
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
