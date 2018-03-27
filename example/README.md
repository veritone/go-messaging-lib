# Examples

## Dependencies

Before using any of the examples, you will need to have a Kafka cluster set up. This repository contains a simple `docker-compose` file that can spin up or down a full stack that managages a single-node kafka cluster. Run:

### For Kafka

```bash
cd $GOPATH/src/github.com/veritone/go-messaging-lib/example
docker-compose -f docker-compose.kafka.yaml up -d
```

### For NSQ

```bash
cd $GOPATH/src/github.com/veritone/go-messaging-lib/example
docker-compose -f docker-compose.nsq.yaml up -d
```

Go to:

* localhost:9001: `kafka-manager` instance provides Admin tools and statistics for Kafka broker.
* localhost:8000: `kafka-topic-ui` instance provides Admin tools and statistics for Kafka topics.
* localhost:8004: `zookeeper-web-ui` instance provides Admin tools and statistics for Zookeeper.

## kafka-sample

Kafka-sample service provides common use cases of the eventing system. Start server by running:

```bash
cd $GOPATH/src/github.com/veritone/go-messaging-lib/example/kafka-sample/
govend
go run main.go -p <port>
```

## kafka-stream

Kafka-sample service provides an example for using the streaming interface to process a text file. Start server by running:

```bash
cd $GOPATH/src/github.com/veritone/go-messaging-lib/example/kafka-stream/
govend
go run main.go
```
