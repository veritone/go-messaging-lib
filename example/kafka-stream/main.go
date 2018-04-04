package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/veritone/go-messaging-lib/kafka"
)

// HelpMessage displays help command
const HelpMessage = `                                                                                                                               
__ __  ___   ____ __ __  ___      __  ______ ____   ____  ___  ___  ___
|| // // \\ ||    || // // \\    (( \ | || | || \\ ||    // \\ ||\\//||
||<<  ||=|| ||==  ||<<  ||=||     \\    ||   ||_// ||==  ||=|| || \/ ||
|| \\ || || ||    || \\ || ||    \_))   ||   || \\ ||___ || || ||    ||


The purpose of this service is to provide guidance on how to properly ultize 
go-messaging-lib features and showcase real integration and benchmarking with kafka framework

`

var (
	kafkaHost = "kafka1:9092"
)

func main() {
	fmt.Println(HelpMessage)
	http.HandleFunc("/", intro)
	http.HandleFunc("/read", read)
	http.HandleFunc("/write", write)
	http.Handle("/metrics", promhttp.Handler())
	log.Panic(http.ListenAndServe(":8080", nil))

}

func intro(rw http.ResponseWriter, r *http.Request) {
	if _, err := rw.Write([]byte(HelpMessage)); err != nil {
		log.Panic(err)
	}
	rw.WriteHeader(http.StatusOK)
}

func write(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	pwd, _ := os.Getwd()
	txt, _ := ioutil.ReadFile(pwd + "/test.txt")
	p := kafka.Producer(topic, kafka.StrategyRoundRobin, "kafka1:9092")
	writer, err := kafka.NewStreamWriter(p, "")
	if err != nil {
		log.Panic(err)
	}
	n, err := writer.Write(txt)
	if err != nil {
		log.Panic(err)
	}
	if n < len(txt) {
		log.Println("incomplete stream")
	}
	if err := writer.Close(); err != nil {
		log.Panic(err)
	}
	rw.Write([]byte("OK"))
}

func read(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")

	c, err := kafka.Consumer(topic, "g1", "kafka1:9092")
	if err != nil {
		log.Panic(err)
	}
	reader, err := kafka.NewStreamReader(c, kafka.ConsumerGroupOption)
	if err != nil {
		log.Panic(err)
	}

	content := make([]byte, 0)
	totalBytes := 0
	for err == nil {
		chunk := make([]byte, 1000)
		var n int
		if n, err = reader.Read(chunk); n > 0 {
			content = append(content, chunk...)
			totalBytes += n
		}
	}
	rw.Write([]byte(fmt.Sprintf("read %d byte(s)\n%s", totalBytes, content)))
	if err == kafka.ErrStreamExitTimedout {
		log.Println(err)
	}
	if err := reader.Close(); err != nil {
		log.Panic(err)
	}

}
