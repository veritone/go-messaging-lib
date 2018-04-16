# go-messaging-lib

## Overview

This project provides simple interfaces to interact with Veritone's core eventing system. There are two main patterns currently supported by this library:

* Pub-Sub
* Streaming

Depending on the underlying eventing system, user should use the corresponding package to initialize the its client. The supported systems are Kafka and NSQ.

## Goals

* Offers simple setup and just work out of the box.
* Provides consistent interfaces that should work for various eventing systems (Kafka, RabbitMQ, NSQ, etc.), thus preventing major breaking changes.
* Provides multiple examples to jump start.
* Handles edge cases and difficult technical requirements behind the scene.
* Exposes monitoring statistics with prometheus.

## Usage

Please see the [instructions](example/README.md)

## Basic Operations:

### Create Producer

```go
message, err := json.Marshal(data)
if err != nil {
    // handle error
}
producer := Producer(topic, kafka.StrategyRoundRobin, "localhost:9092")
msg, err := NewMessage("hash_key", message)
if err != nil {
    // handle error
}
err = producer.Produce(context.TODO(), msg)
if err != nil {
    // handle error
}
err = producer.Close()
if err != nil {
    // handle error
}
```

### Create Consumer

```go
consumer, err = kafka.Consumer("topic_name", "consumer_group_name", "localhost:9092")
if err != nil {
    // handle error
}
queue, err = consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
if err != nil {
    // handle error
}
for item := range queue {
    log.Printf("Received: (%s) (%#v) (%T)\n", item.Payload(), item.Metadata(), item.Raw())
}
```

## Notes

This repo is still a WIP. It's not yet suitable for production use.
