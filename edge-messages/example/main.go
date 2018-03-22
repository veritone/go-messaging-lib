package main

import (
	"context"
	"log"

	"github.com/davecgh/go-spew/spew"
	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/veritone/go-messaging-lib"
	"github.com/veritone/go-messaging-lib/edge-messages"
	"github.com/veritone/go-messaging-lib/kafka"
)

func main() {
	topic := "test"
	produceToKafka(topic, createMessages())
	consumeFromKafka(topic)
}

func createMessages() []messaging.Messager {
	var msgs []messaging.Messager

	for _, msgType := range messages.ValidMessageTypes {
		var k messaging.Messager
		var err error
		switch msgType {
		case messages.RawStreamType:
			m := messages.RandomRawStream()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.StreamEOFType:
			m := messages.RandomStreamEOF()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.MediaChunkType:
			m := messages.RandomMediaChunk()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.EngineOutputType:
			m := messages.RandomEngineOutput()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.OutputAppendedType:
			m := messages.RandomOutputAppended()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.EngineHeartbeatType:
			m := messages.RandomEngineHeartbeat()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.ControlTaskType:
			m := messages.RandomControlTask()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.QueueMetricsType:
			m := messages.RandomQueueMetrics()
			spew.Dump(m)
			k, err = m.ToKafka()
		case messages.ChunkProcessedStatusType:
			m := messages.RandomChunkProcessedStatus()
			spew.Dump(m)
			k, err = m.ToKafka()
		}
		if err != nil {
			log.Panic(err)
		}
		//spew.Dump(k)
		msgs = append(msgs, k)
	}
	return msgs
}

func produceToKafka(topic string, msgs []messaging.Messager) {
	brokers := "localhost:9092"
	producer := kafka.Producer(topic, kafka.StrategyRoundRobin, brokers)

	for _, msg := range msgs {
		err := producer.Produce(context.TODO(), msg)
		if err != nil {
			log.Panic(err)
		}

		k, ok := msg.Message().(*kafkaGo.Message)
		if !ok {
			log.Panic(err)
		}

		msgType, err := messages.GetMsgType(k)
		if err != nil {
			log.Panic(err)
		}

		log.Printf("Produced successful: msgType = %s, topic = %s\n", msgType, topic)
	}
}

func consumeFromKafka(topic string) {
	group := "group1"
	brokers := "localhost:9092"
	consumer := kafka.Consumer(topic, group, brokers)
	queue, err := consumer.Consume(context.TODO(), kafka.ConsumerGroupOption)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Consuming from: topic = %s, group = %s\n", topic, group)

	for i := 0; i < len(messages.ValidMessageTypes); i++ {
		// Consume a message
		item := <-queue
		log.Println("Got new message")
		k, ok := item.(*kafkaGo.Message)
		if !ok {
			log.Panic("Failed to get inner Kafka message")
		}
		spew.Dump(k)

		// Get message type to decode to original Edge message
		msgType, err := messages.GetMsgType(k)
		if err != nil {
			log.Panicf("Failed to get message type: %v", err)
		}
		log.Println(msgType)

		switch msgType {
		case messages.RawStreamType:
			v, err := messages.ToRawStream(k)
			if err != nil {
				log.Panicf("Failed to decode to RawStream msg: %v", err)
			}
			spew.Dump(v)
		case messages.StreamEOFType:
			v, err := messages.ToStreamEOF(k)
			if err != nil {
				log.Panicf("Failed to decode to StreamEOF msg: %v", err)
			}
			spew.Dump(v)
		case messages.MediaChunkType:
			v, err := messages.ToMediaChunk(k)
			if err != nil {
				log.Panicf("Failed to decode to MediaChunk msg: %v", err)
			}
			spew.Dump(v)
		case messages.EngineOutputType:
			v, err := messages.ToEngineOutput(k)
			if err != nil {
				log.Panicf("Failed to decode to EngineOutput msg: %v", err)
			}
			spew.Dump(v)
		case messages.OutputAppendedType:
			v, err := messages.ToOutputAppended(k)
			if err != nil {
				log.Panicf("Failed to decode to OutputAppended msg: %v", err)
			}
			spew.Dump(v)
		case messages.EngineHeartbeatType:
			v, err := messages.ToEngineHeartbeat(k)
			if err != nil {
				log.Panicf("Failed to decode to EngineHeartbeat msg: %v", err)
			}
			spew.Dump(v)
		case messages.ControlTaskType:
			v, err := messages.ToControlTask(k)
			if err != nil {
				log.Panicf("Failed to decode to ControlTask msg: %v", err)
			}
			spew.Dump(v)
		case messages.QueueMetricsType:
			v, err := messages.ToQueueMetrics(k)
			if err != nil {
				log.Panicf("Failed to decode to QueueMetrics msg: %v", err)
			}
			spew.Dump(v)
		case messages.ChunkProcessedStatusType:
			v, err := messages.ToChunkProcessedStatus(k)
			if err != nil {
				log.Panicf("Failed to decode to ChunkProcessedStatus msg: %v", err)
			}
			spew.Dump(v)
		default:
			log.Panic("Unknown message type: " + msgType)
		}
	}
}
