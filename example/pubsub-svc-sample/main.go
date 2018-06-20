package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	events "github.com/veritone/core-messages/generated/go/events"
	"github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/pubsub"

	"flag"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HelpMessage displays help command
const HelpMessage = `    
 
|￣￣￣￣￣￣￣￣￣|
| PubSub Sample |
|_______________|
(\__/) || 
(•ㅅ•) || 
/ 　 づ  
The purpose of this service is to provide guidance on how to properly ultize 
go-messaging-lib features and showcase real integration and benchmarking with pubsub eventing system
`

var consumers []messaging.Consumer

func main() {

	portPtr := flag.String("p", "8080", "http port")
	flag.Parse()

	fmt.Println(HelpMessage)
	http.HandleFunc("/", intro)
	http.HandleFunc("/pub", pub)
	http.HandleFunc("/sub", sub)
	http.HandleFunc("/shutdown", shutdown)
	http.Handle("/metrics", promhttp.Handler())
	log.Panic(http.ListenAndServe(":"+*portPtr, nil))

}

func intro(rw http.ResponseWriter, r *http.Request) {
	if _, err := rw.Write([]byte(HelpMessage)); err != nil {
		log.Panic(err)
	}
	rw.WriteHeader(http.StatusOK)
}

func pub(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	//message := q.Get("message")

	producer, err := pubsub.NewProducer(
		context.Background(),
		topic,
		"test", //"ngo-home-automation",
		"/Users/home/Desktop/my_cred.json")
	if err != nil {
		log.Panic(err)
	}
	msgObj := &events.ExampleEvent{
		FirstName: "Sam",
		LastName:  "Ngo",
	}
	msg, err := pubsub.NewMessage(msgObj)
	if err != nil {
		log.Panic(err)
	}
	err = producer.Produce(context.Background(), msg)
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
	sub := q.Get("subscription")
	var (
		consumer messaging.Consumer
		err      error
		queue    <-chan messaging.Event
	)
	consumer, err = pubsub.NewConsumer(context.Background(),
		topic,
		sub,
		"test", //"ngo-home-automation",
		"/Users/home/Desktop/my_cred.json")
	if err != nil {
		log.Panic(err)
	}
	queue, err = consumer.Consume(context.Background(), &pubsub.PubSubConsumerOptions{})
	if err != nil {
		log.Panic(err)
	}
	consumers = append(consumers, consumer)
	_, err = rw.Write([]byte("started a consumer"))
	if err != nil {
		log.Panic(err)
	}

	for {
		<-queue
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
