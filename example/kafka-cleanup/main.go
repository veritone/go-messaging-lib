package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/veritone/go-messaging-lib/kafka"
)

func main() {
	host, _ := os.LookupEnv("KAFKA")
	debugFlag, _ := os.LookupEnv("DEBUG")
	debug := debugFlag != ""
	kafkaHost := host + ":9092"
	manager, err := kafka.Manager(kafkaHost)
	if err != nil {
		log.Panic(err)
	}
	topics, _, err := manager.ListTopicsLite(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	twoHoursAgo := time.Now().Add(-2*time.Hour).Unix() * 1000
	now := time.Now().Unix() * 1000
	// log.Println(twoHoursAgo, " now:", time.Now().Unix()*1000)
	toBeDeleted := []string{}
	remains := []string{}
	var wg sync.WaitGroup

	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			partitions, err := manager.Partitions(topic)
			if err != nil {
				log.Panic(err)
			}
			expiredTopic := true
			for _, p := range partitions {
				if !expiredTopic {
					break
				}
				offsetPrev, err := manager.LatestOffset(topic, p, twoHoursAgo)
				if err != nil {
					// doesnt exist
					if debug {
						log.Println(err, spew.Sdump(p, topic))
					}
					continue
				}
				offsetCurr, err := manager.LatestOffset(topic, p, now)
				if err != nil {
					// doesnt exist
					if debug {
						log.Println(err, spew.Sdump(p, topic))
					}
					continue
				}
				if offsetCurr-offsetPrev != 0 {
					expiredTopic = false
				}
				if debug {
					log.Println("OK:", offsetCurr-offsetPrev, p, topic)
				}
				// topic is still in use
			}
			if expiredTopic {
				toBeDeleted = append(toBeDeleted, topic)
			} else {
				remains = append(remains, topic)
			}
			wg.Done()
		}(topic)
	}
	wg.Wait()
	for _, v := range toBeDeleted {
		fmt.Println(v)
	}
	if debug {
		fmt.Println("=============== REMAINING TOPICS ===============")
		for _, v := range remains {
			fmt.Println(v)
		}
	}
}
