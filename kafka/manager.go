package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
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
	c.Version = sarama.V1_0_0_0
	clusterC := cluster.NewConfig()
	clusterC.Version = sarama.V1_0_0_0
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
	e := m.single.RefreshMetadata()
	if e != nil {
		return nil, e
	}
	groups := make(map[string]bool)
	for _, b := range m.single.Brokers() {
		err := connectBroker(b, m.single.Config())
		if err != nil {
			return nil, err
		}
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
	topics, err := m.single.Topics()
	if err != nil {
		return nil, err
	}
	// catch panic if it happens in one of the go routines
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}
		}
	}()

	// if we have topics, add an empty group to return data
	// even when there isn't any consumer group
	if len(topics) > 0 {
		groups[""] = true
	}

	res := make(chan *PartitionInfoContainer)
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(topics))
		for _, t := range topics {
			go func(t string) {
				defer wg.Done()
				m.perTopic(t, groups, res)
			}(t)
		}
		wg.Wait()
		close(res)
	}()

	for item := range res {
		if _, ok := response[item.Topic]; !ok {
			response[item.Topic] = TopicInfo{}
		}
		if _, ok := response[item.Topic][item.GroupID]; !ok {
			response[item.Topic][item.GroupID] = GroupInfo{}
		}
		if _, ok := response[item.Topic][item.GroupID][item.Partition]; !ok {
			response[item.Topic][item.GroupID][item.Partition] = item.PartitionInfo
		}
	}
	return response, nil
}

func (m *KafkaManager) perTopic(t string, groups map[string]bool, response chan<- *PartitionInfoContainer) {
	partitions, err := m.single.Partitions(t)
	if err != nil {
		log.Panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, pID := range partitions {
		go func(pID int32) {
			m.perPartition(t, pID, groups, response)
			wg.Done()
		}(pID)
	}
	wg.Wait()
}

func (m *KafkaManager) perPartition(t string, pID int32, groups map[string]bool, response chan<- *PartitionInfoContainer) {
	availableOffset, err := m.single.GetOffset(t, pID, sarama.OffsetNewest)
	if err != nil {
		log.Panic(err)
	}
	oldestOffset, err := m.single.GetOffset(t, pID, sarama.OffsetOldest)
	if err != nil {
		log.Panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(len(groups))
	for g := range groups {
		go func(g string) {
			defer wg.Done()
			m.perGroup(t, g, pID, availableOffset, oldestOffset, response)
		}(g)
	}
	wg.Wait()
}

func (m *KafkaManager) perGroup(t, g string, pID int32, availableOffset, oldestOffset int64, response chan<- *PartitionInfoContainer) {
	if err := m.single.RefreshCoordinator(g); err != nil {
		log.Panic(err)
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(g, m.single)
	if err != nil {
		log.Panic(err)
	}
	defer offsetManager.Close()
	partitionOffsetManager, err := offsetManager.ManagePartition(t, pID)
	if err != nil {
		log.Panic(err)
	}
	defer partitionOffsetManager.Close()
	consumerOffset, _ := partitionOffsetManager.NextOffset()
	res := &PartitionInfoContainer{
		PartitionInfo: &PartitionInfo{
			Start:  availableOffset,
			End:    oldestOffset,
			Offset: consumerOffset,
			Lag:    availableOffset - consumerOffset,
		},
		Topic:     t,
		GroupID:   g,
		Partition: pID,
	}
	response <- res
}

func connectBroker(broker *sarama.Broker, config *sarama.Config) error {
	if ok, err := broker.Connected(); ok && err == nil {
		return nil
	}
	if err := broker.Open(config); err != nil {
		return err
	}
	connected, err := broker.Connected()
	if err != nil {
		return err
	}
	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}
	return nil
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

type PartitionInfoContainer struct {
	Topic     string
	GroupID   string
	Partition int32
	*PartitionInfo
}

func (m *KafkaManager) CreateTopics(_ context.Context, opts messaging.OptionCreator, topics ...string) error {
	v, ok := opts.Options().(CreateTopicOptions)
	if !ok {
		return errors.New("incompatible options, did you use CreateTopicOptions?")
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

	controllerBroker, err := m.single.Controller()
	if err != nil {
		return err
	}
	connected, err := controllerBroker.Connected()
	if err != nil {
		return fmt.Errorf("cannot query for broker connection status, %v", err)
	}
	if !connected {
		err = controllerBroker.Open(m.single.Config())
		if err != nil {
			return err
		}
	}
	res, err := controllerBroker.CreateTopics(t)
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
	controllerBroker, err := m.single.Controller()
	if err != nil {
		return err
	}
	err = connectBroker(controllerBroker, m.single.Config())
	if err != nil {
		return err
	}
	res, err := controllerBroker.DeleteTopics(&sarama.DeleteTopicsRequest{
		Topics:  topics,
		Timeout: time.Second * 5,
	})
	if err != nil {
		return err
	}
	if len(res.TopicErrorCodes) > 0 {
		var buf bytes.Buffer
		for t, v := range res.TopicErrorCodes {
			if v != sarama.ErrNoError {
				buf.WriteString(fmt.Sprintf("unable to delete topic (%s) err %s\n", t, v.Error()))
			}
		}
		if buf.Len() > 0 {
			return errors.New(buf.String())
		}
	}
	return nil
}

// AddPartitions increases the partition count for a set of topics
func (m *KafkaManager) AddPartitions(_ context.Context, req TopicPartitionRequest) error {
	controllerBroker, err := m.single.Controller()
	if err != nil {
		return err
	}
	err = connectBroker(controllerBroker, m.single.Config())
	if err != nil {
		return err
	}

	topics, err := m.single.Topics()
	if err != nil {
		return err
	}
	topicLookup := make(map[string]bool)
	for _, t := range topics {
		topicLookup[t] = true
	}

	input := make(map[string]*sarama.TopicPartition)
	for topic, pCount := range req {
		if _, exist := topicLookup[topic]; !exist {
			return ErrInvalidTopic
		}
		// Calling Partitions on non-existent topic will create the topic
		// which we don't want
		partitions, e := m.single.Partitions(topic)
		if e != nil {
			return e
		}
		if len(partitions) == pCount {
			return ErrSamePartitionCount
		}
		if len(partitions) > pCount {
			return ErrInvalidPartitionCount
		}
		input[topic] = &sarama.TopicPartition{
			Count: int32(pCount),
		}
	}
	res, err := controllerBroker.CreatePartitions(&sarama.CreatePartitionsRequest{
		Timeout:         time.Second * 5,
		ValidateOnly:    false,
		TopicPartitions: input,
	})
	if err != nil {
		return err
	}
	if len(res.TopicPartitionErrors) > 0 {
		var buf bytes.Buffer
		for t, v := range res.TopicPartitionErrors {
			if v.Err != sarama.ErrNoError {
				buf.WriteString(fmt.Sprintf("unable to add partitions to topic (%s) err %s\n", t, v.Err.Error()))
			}
		}
		if buf.Len() > 0 {
			return errors.New(buf.String())
		}
	}
	return nil
}

func (m *KafkaManager) closeBrokers(brokers []*sarama.Broker) {
	for _, b := range m.single.Brokers() {
		connected, err := b.Connected()
		if !connected {
			continue
		}
		if err != nil {
			log.Printf("unable to determine broker connection state %s\n", b.Addr())
			continue
		}
		if err := b.Close(); err != nil {
			log.Printf("error closing broker %s %v\n", b.Addr(), err)
		}
	}
}

func (m *KafkaManager) Close() error {
	m.closeBrokers(m.single.Brokers())
	if err := m.single.Close(); err != nil {
		return err
	}

	m.closeBrokers(m.multi.Brokers())
	if err := m.multi.Close(); err != nil {
		return err
	}
	return nil
}

// TopicPartitionRequest lets Kafka manager know which topic to modify
// and the target number of partitions it should have.
// key = topic name, value = target number of partitions
type TopicPartitionRequest map[string]int

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
