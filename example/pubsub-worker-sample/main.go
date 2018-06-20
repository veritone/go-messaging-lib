package main

import (
	"context"
	"log"
	_ "net/http/pprof"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	events "github.com/veritone/core-messages/generated/go/events"
	"github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/pubsub"

	"flag"
)

// HelpMessage displays help command
const HelpMessage = `    
 
|￣￣￣￣￣￣￣￣￣￣￣￣￣|
| PubSub Worker Sample |
|______________________|
(\__/) || 
(•ㅅ•) || 
/ 　 づ  
The purpose of this service is to provide guidance on how to properly ultize 
go-messaging-lib features and showcase real integration and benchmarking with nsq eventing system
`

var (
	consumerTopic        string
	consumerSubscription string
	producerTopic        string
	fakeTimer            int
	id                   int
)

func main() {

	consumerTopicPtr := flag.String("ct", "consumer_topic", "")
	consumerSubscriptionPtr := flag.String("cs", "consumer_subscription", "")
	producerTopicPtr := flag.String("pt", "consumer_topic", "")
	fakeTimerPtr := flag.Int("d", 3, "duration to wait before producing message in Seconds")
	idPtr := flag.Int("id", 1, "example id [1,4]")
	flag.Parse()

	consumerTopic = *consumerTopicPtr
	consumerSubscription = *consumerSubscriptionPtr
	producerTopic = *producerTopicPtr
	fakeTimer = *fakeTimerPtr
	id = *idPtr

	log.Println("Started...")
	sub()
}

func pub(prev messaging.Event) {
	producer, err := pubsub.NewProducer(
		context.Background(),
		producerTopic,
		"test",
		"/Users/home/Desktop/my_cred.json")
	if err != nil {
		log.Panic(err)
	}
	msg, err := pubsub.NewMessage(createMessage())
	if err != nil {
		log.Panic(err)
	}
	err = producer.Produce(context.Background(), msg, prev)
	if err != nil {
		log.Panic(err)
	}
	err = producer.Close()
	if err != nil {
		log.Panic(err)
	}
}

func createMessage() proto.Message {
	var msgObj proto.Message
	switch id {
	case 1:
		msgObj = &events.ExampleOne{
			Data: "ID1",
		}
	case 2:
		msgObj = &events.ExampleTwo{
			Number: 123,
		}
	case 3:
		msgObj = &events.ExampleThree{
			Boolean: true,
		}
	default:
		msgObj = &events.ExampleFour{
			Data:    "ID4",
			Number:  4,
			Boolean: false,
		}
	}
	return msgObj
}

func sub() {
	var (
		consumer messaging.Consumer
		err      error
		queue    <-chan messaging.Event
	)
	consumer, err = pubsub.NewConsumer(context.Background(),
		consumerTopic,
		consumerSubscription,
		"test",
		"/Users/home/Desktop/my_cred.json")
	if err != nil {
		log.Panic(err)
	}
	queue, err = consumer.Consume(context.Background(), &pubsub.PubSubConsumerOptions{})
	if err != nil {
		log.Panic(err)
	}
	for item := range queue {
		log.Printf("Payload: %s\n", item.Payload())
		log.Printf("Metadata: %#v\n", item.Metadata())
		log.Printf("Rawdata: %s\n", spew.Sdump(item.Raw()))
		go func(messaging.Event) {
			time.Sleep(time.Second * time.Duration(fakeTimer))
			pub(item)
		}(item)
	}
}
