package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/veritone/go-messaging-lib/kafka"
)

func main() {
	host, _ := os.LookupEnv("KAFKA")
	log.Println("host:", host)
	manager, err := kafka.Manager(host + ":9092")
	if err != nil {
		log.Panic(err)
	}
	data, err := manager.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}

	// the library exposes ListTopicsResponse type for casting
	v, ok := data.(kafka.ListTopicsResponse)
	if ok {
		log.Println("got ListTopicsResponse")
		_ = v
		// You can either cast it and access the data here directly e.g
		// 	info = v[topic_name][group_name][partition_number]
		// or just dump it as json like below
	}
	log.Println("list topic done")
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("%s", jsonData)
	if err = manager.Close(); err != nil {
		log.Panic(err)
	}
}
