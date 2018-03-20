package kafka

import (
	"bytes"
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	messaging "github.com/veritone/go-messaging-lib"
)

type manager struct {
	single sarama.Client
	multi  cluster.Client
}

func Manager(host string) (messaging.Manager, error) {
	c := sarama.NewConfig()
	// default version
	c.Version = sarama.V0_10_2_0
	clusterC := cluster.NewConfig()
	clusterC.Version = sarama.V0_10_2_0
	s, err := sarama.NewClient([]string{host}, c)
	if err != nil {
		return nil, err
	}
	m, err := cluster.NewClient([]string{host}, clusterC)
	if err != nil {
		return nil, err
	}
	return &manager{
		single: s,
		multi:  *m,
	}, nil
}

func (m *manager) ListTopics(_ context.Context) (interface{}, error) {
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

func (m *manager) CreateTopics(_ context.Context, topics ...string) error {
	brokers := m.multi.Client.Brokers()
	if len(brokers) == 0 {
		return errors.New("cannot find any broker to create topic")
	}
	t := &sarama.CreateTopicsRequest{}
	t.TopicDetails = make(map[string]*sarama.TopicDetail)
	for _, topic := range topics {
		t.TopicDetails[topic] = &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}
	res, err := brokers[0].CreateTopics(t)
	if err != nil {
		return err
	}
	if len(res.TopicErrors) > 0 {
		var buf bytes.Buffer
		for _, err := range res.TopicErrors {
			buf.WriteString(*err.ErrMsg + ",")
		}
		return errors.New(buf.String())
	}
	return nil
}
func (m *manager) DeleteTopics(_ context.Context, topics ...string) error {
	return errors.New("not yet implemented")
}

func (m *manager) Close() error {
	if err := m.single.Close(); err != nil {
		return err
	}
	if err := m.multi.Close(); err != nil {
		return err
	}
	return nil
}
