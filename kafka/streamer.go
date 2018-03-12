package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	gKafka "github.com/segmentio/kafka-go"
	messaging "github.com/veritone/go-messaging-lib"
)

type streamWriter struct {
	messaging.Producer
	Key string
}

type streamReader struct {
	messaging.OptionCreator
	Consumer messaging.Consumer
	Config   *gKafka.ReaderConfig
	Steam    <-chan interface{}
	buf      []byte
}

var (
	ErrStreamExitTimedout = errors.New("reader stream is closed based on timeout setting")
	ErrInvalidMessageType = errors.New("reader stream received incompatible message type")
)

// NewStreamWriter creates a stream writer by wrapping a kafka producer
func NewStreamWriter(p messaging.Producer, key string) (messaging.StreamWriter, error) {
	return &streamWriter{
		Producer: p,
		Key:      key,
	}, nil
}

// NewStreamReader creates a stream reader by wrapping a kafka consumer
func NewStreamReader(c messaging.Consumer, options messaging.OptionCreator) (messaging.StreamReader, error) {
	messages, err := c.Consume(context.TODO(), options)
	if err != nil {
		return nil, err
	}
	return &streamReader{
		Consumer:      c,
		OptionCreator: options,
		Steam:         messages,
	}, nil
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	copied, done := s.fillFromPendingBuffer(p)
	if done {
		return n, nil
	}
	// TODO: make this configurable
	timer := time.NewTimer(time.Second * 2)
ConsumerLoop:
	for {
		select {
		case item := <-s.Steam:
			v, ok := item.(*gKafka.Message)
			if ok {
				fmt.Printf("Offset:%d\n", v.Offset)
				n := copy(p[copied:], v.Value)
				copied += n
				if n < len(v.Value) {
					s.buf = v.Value[n:]
					return len(p), nil
				}
				timer.Reset(time.Second * 2)
			} else {
				err = ErrInvalidMessageType
				break ConsumerLoop
			}
		case <-timer.C:
			err = ErrStreamExitTimedout
			break ConsumerLoop
		}
	}
	return copied, err
}

func (s *streamReader) fillFromPendingBuffer(p []byte) (int, bool) {
	if len(p) == 0 {
		return 0, true
	}
	n := copy(p, s.buf)
	s.buf = s.buf[:0]
	return n, false

}

func (s *streamWriter) Write(p []byte) (int, error) {
	const chunkSize int = 1e6 // 1MB
	var (
		err error
		i   = 0
		max = len(p)
	)
	for i+chunkSize < max {
		i, err = s.appendMessage(p, i, chunkSize)
		if err != nil {
			return i, err
		}
	}
	if i+1 < max {
		i, err = s.appendMessage(p, i, max-i)
		if err != nil {
			return i, err
		}
	}
	return i, nil
}

func (s *streamWriter) appendMessage(p []byte, start, size int) (index int, err error) {
	msg, err := NewMessage(s.Key, p[start:start+size])
	if err != nil {
		return start, err
	}
	err = s.Produce(context.TODO(), msg)
	if err != nil {
		return start, err
	}
	return start + size, nil
}

func (s *streamReader) Close() error {
	return s.Consumer.Close()
}

func (s *streamWriter) Close() error {
	return s.Producer.Close()
}
