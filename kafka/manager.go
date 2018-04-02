package kafka

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	messaging "github.com/veritone/go-messaging-lib"
)

type KafkaManager struct {
	single sarama.Client
	multi  cluster.Client
}

// Manager creates a simple Kafka Manager with default config to perform administrative tasks
func Manager(hosts ...string) (*KafkaManager, error) {
	c := sarama.NewConfig()
	// default version
	c.Version = sarama.V0_10_2_0
	clusterC := cluster.NewConfig()
	clusterC.Version = sarama.V0_10_2_0
	s, err := sarama.NewClient(hosts, c)
	if err != nil {
		return nil, err
	}
	m, err := cluster.NewClient(hosts, clusterC)
	if err != nil {
		return nil, err
	}
	return &KafkaManager{
		single: s,
		multi:  *m,
	}, nil
}

func (m *KafkaManager) ListTopics(_ context.Context) (interface{}, error) {
	// TODO: reduce the complexity and use concurrency
	e := m.multi.RefreshMetadata()
	if e != nil {
		return nil, e
	}
	groups := make(map[string]bool)
	for _, b := range m.multi.Brokers() {
		err := b.Open(m.single.Config())
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = b.Close()
		}()
		res, err := b.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return nil, err
		}
		for k, v := range res.Groups {
			// take only groups marked as 'consumer'
			if v == "consumer" {
				groups[k] = true
			}
		}
	}

	response := ListTopicsResponse{}
	topics, err := m.multi.Topics()
	if err != nil {
		return nil, err
	}
	for _, t := range topics {
		if _, ok := response[t]; !ok {
			response[t] = TopicInfo{}
		}
		partitions, err := m.multi.Partitions(t)
		if err != nil {
			return nil, err
		}
		for _, pID := range partitions {
			availableOffset, err := m.multi.GetOffset(t, pID, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			oldestOffset, err := m.multi.GetOffset(t, pID, sarama.OffsetOldest)
			if err != nil {
				return nil, err
			}
			for g := range groups {
				if _, ok := response[t][g]; !ok {
					response[t][g] = GroupInfo{}
				}
				if _, ok := response[t][g][pID]; !ok {
					response[t][g][pID] = &PartitionInfo{}
				}
				if err := m.multi.RefreshCoordinator(g); err != nil {
					return nil, err
				}
				offsetManager, err := sarama.NewOffsetManagerFromClient(g, m.single)
				if err != nil {
					return nil, err
				}
				defer func() {
					_ = offsetManager.Close()
				}()
				partitionOffsetManager, err := offsetManager.ManagePartition(t, pID)
				if err != nil {
					return nil, err
				}
				defer func() {
					_ = partitionOffsetManager.Close()
				}()
				consumerOffset, meta := partitionOffsetManager.NextOffset()
				if consumerOffset == m.multi.Config().Consumer.Offsets.Initial && len(meta) == 0 {
					//log.Print("INVALID")
					continue
				}
				response[t][g][pID].Start = availableOffset
				response[t][g][pID].End = oldestOffset
				response[t][g][pID].Offset = consumerOffset
				response[t][g][pID].Lag = availableOffset - consumerOffset
			}
		}
	}
	return response, nil
}

// ListTopicsResponse is a map of TopicInfo
type ListTopicsResponse map[string]TopicInfo

// TopicInfo is a map of GroupInfo
type TopicInfo map[string]GroupInfo

// GroupInfo is a map of PartitionInfo
type GroupInfo map[int32]*PartitionInfo

// PartitionInfo contains metadata for a given partition
type PartitionInfo struct {
	Start  int64
	End    int64
	Offset int64
	Lag    int64
}

func (m *KafkaManager) CreateTopics(_ context.Context, opts messaging.OptionCreator, topics ...string) error {
	v, ok := opts.Options().(CreateTopicOptions)
	if !ok {
		return errors.New("incompatible options, did you use CreateTopicOptions?")
	}

	brokers := m.multi.Client.Brokers()
	if len(brokers) == 0 {
		return errors.New("cannot find any broker to create topic")
	}
	t := &sarama.CreateTopicsRequest{}
	t.Timeout = time.Second * 10
	t.TopicDetails = make(map[string]*sarama.TopicDetail)
	for _, topic := range topics {
		t.TopicDetails[topic] = &sarama.TopicDetail{
			NumPartitions:     v.NumPartitions,
			ReplicationFactor: v.ReplicationFactor,
			ConfigEntries:     v.ConfigEntries,
			ReplicaAssignment: v.ReplicaAssignment,
		}
	}
	err := brokers[0].Open(m.single.Config())
	if err != nil {
		return err
	}
	var connected bool
	for !connected && err == nil {
		// Wait for connection since Open does not block synchronously.
		// TODO: should have a channel here
		connected, err = brokers[0].Connected()
	}
	res, err := brokers[0].CreateTopics(t)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, err := range res.TopicErrors {
		if err.Err != sarama.ErrNoError {
			buf.WriteString(err.Err.Error() + ",")
		}
	}
	if buf.Len() > 0 {
		return errors.New(buf.String())
	}
	return nil
}
func (m *KafkaManager) DeleteTopics(_ context.Context, topics ...string) error {
	return errors.New("not yet implemented")
}

func (m *KafkaManager) Close() error {
	if err := m.single.Close(); err != nil {
		return err
	}
	if err := m.multi.Close(); err != nil {
		return err
	}
	return nil
}

// CreateTopicOptions is an options that will be applied to topic creation.
// The properties are idential to sarama.TopicDetail
type CreateTopicOptions struct {
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	ConfigEntries     map[string]*string
}

// Options returns the compatible options for creating topics
func (c CreateTopicOptions) Options() interface{} {
	return c
}
