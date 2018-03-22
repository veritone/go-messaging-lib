package messages

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/veritone/go-messaging-lib"
)

const (
	// MediaChunkType - string ID of MediaChunk type
	MediaChunkType = "media_chunk"
)

// MediaChunk - container for raw bytes from a stream
type MediaChunk struct {
	Key   string // MediaChunkType
	Value MediaChunkVal
}

// MediaChunkVal - container for MediaChunk value to produce to Kafka
type MediaChunkVal struct {
	Type          string // MediaChunkType
	TimestampUTC  int64  // milliseconds since epoch
	TaskID        string // Task ID
	TDOID         string // TDO ID
	JobID         string // Job ID
	ChunkIndex    int32  // Index of this chunk in the entire TDO
	StartOffsetMs int32  // Offset of start of media chunk from beginning of TDO
	EndOffsetMs   int32  // Offset of end of media chunk from beginning of TDO
	CacheURI      string // Location of media chunk in cache
	Content       string // Data value of media chunk
	TaskPayload   string // Task payload for engine
	ChunkUUID     string // UUID of this chunk (to distinguish one chunk from another)
}

// EmptyMediaChunk - create an empty MediaChunk message
func EmptyMediaChunk() MediaChunk {
	return MediaChunk{
		Key: MediaChunkType,
		Value: MediaChunkVal{
			Type:         MediaChunkType,
			TimestampUTC: int64(time.Now().UnixNano() / 1000),
		},
	}
}

// RandomMediaChunk - create a MediaChunk message with random values
func RandomMediaChunk() MediaChunk {
	rand.Seed(int64(time.Now().UnixNano() / 1000))
	return MediaChunk{
		Key: MediaChunkType,
		Value: MediaChunkVal{
			Type:          MediaChunkType,
			TimestampUTC:  int64(time.Now().UnixNano() / 1000),
			TaskID:        uuidv4(),
			TDOID:         uuidv4(),
			JobID:         uuidv4(),
			ChunkIndex:    rand.Int31(),
			StartOffsetMs: rand.Int31(),
			EndOffsetMs:   rand.Int31(),
			CacheURI:      "s3://media_chunk/something",
			Content:       "some content",
			TaskPayload:   "some payload",
			ChunkUUID:     uuidv4(),
		},
	}
}

// ToKafka - encode from MediaChunk message to Kafka message
func (m MediaChunk) ToKafka() (messaging.Messager, error) {
	return newMessage(m.Key, m.Value)
}

// ToMediaChunk - decode from Kafka message to MediaChunk message
func ToMediaChunk(k *kafka.Message) (MediaChunk, error) {
	msg := MediaChunk{}
	// Decode key
	keyBuf := bytes.NewBuffer(k.Key)
	err := gob.NewDecoder(keyBuf).Decode(&msg.Key)
	if err != nil {
		return msg, fmt.Errorf("error decoding Key for MediaChunk: %v", err)
	}
	// Decode value
	valBuf := bytes.NewBuffer(k.Value)
	err = gob.NewDecoder(valBuf).Decode(&msg.Value)
	if err != nil {
		return msg, fmt.Errorf("error decoding Value for MediaChunk: %v", err)
	}
	return msg, nil
}
