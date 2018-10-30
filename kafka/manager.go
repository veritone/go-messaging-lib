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
	single          sarama.Client
	multi           cluster.Client
	timeOutDuration time.Duration
}

// Manager creates a simple Kafka Manager with default config to perform administrative tasks
func Manager(hosts ...string) (*KafkaManager, error) {
	c := sarama.NewConfig()
	c.Admin.Timeout = 30 * time.Second

	// default version
	c.Version = sarama.V1_1_0_0

	clusterC := cluster.NewConfig()
	clusterC.Version = sarama.V1_1_0_0
	clusterC.Admin.Timeout = 30 * time.Second

	s, err := sarama.NewClient(hosts, c)
	if err != nil {
		return nil, err
	}

	// Make sure we are able to reach the controller
	_, err = s.Controller()
	if err != nil {
		return nil, err
	}

	m, err := cluster.NewClient(hosts, clusterC)
	if err != nil {
		return nil, err
	}

	return &KafkaManager{
		single:          s,
		multi:           *m,
		timeOutDuration: 30 * time.Second,
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
		// This logic is to filter out consumer groups that do not work on the current topic {item.Topic}
		// item.Offset, in this case, returns -1 until the consumer group is assigned to work on the topic.
		// We do not apply this logic to the empty group {""} since we'd like to display number of partitions for
		// the current topic.
		if item.Offset < 0 && item.GroupID != "" {
			continue
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

// ListTopicsLite is a fast version of ListTopics. It returns a list of topics
// and a list of consumer groups for all brokers.
func (m *KafkaManager) ListTopicsLite(_ context.Context) ([]string, []string, error) {
	e := m.single.RefreshMetadata()
	if e != nil {
		return nil, nil, e
	}
	groups := make([]string, 0)
	groupMap := make(map[string]bool)
	for _, b := range m.single.Brokers() {
		err := connectBroker(b, m.single.Config())
		if err != nil {
			return nil, nil, err
		}
		// Get all groups managed by current broker
		res, err := b.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return nil, nil, err
		}
		for k, v := range res.Groups {
			if _, exist := groupMap[k]; exist {
				continue
			}
			// take only groups marked as 'consumer'
			if v == "consumer" {
				groupMap[k] = true
				groups = append(groups, k)
			}
		}
	}

	topics, err := m.single.Topics()
	return topics, groups, err
}

// GetPartitionInfo retrieves information for all partitions that are associated with the given consumer_group:topic
// non-exisiting topic will be created automatically.
func (m *KafkaManager) GetPartitionInfo(topic, consumerGroup string, withRefresh bool) ([]*PartitionInfoContainer, error) {
	if withRefresh {
		e := m.single.RefreshMetadata()
		if e != nil {
			return nil, e
		}
	}

	// catch panic if it happens in one of the go routines
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}
		}
	}()
	responses := make(chan *PartitionInfoContainer)
	go func() {
		m.perTopic(topic, map[string]bool{
			consumerGroup: true,
		}, responses)
		close(responses)
	}()
	results := []*PartitionInfoContainer{}
	for r := range responses {
		results = append(results, r)
	}
	return results, err
}

func (m *KafkaManager) Partitions(topic string) ([]int32, error) {
	return m.single.Partitions(topic)
}

func (m *KafkaManager) LatestOffset(topic string, partition int32, timeInMs int64) (int64, error) {
	return m.single.GetOffset(topic, partition, timeInMs)
}

func (m *KafkaManager) perTopic(t string, groups map[string]bool, response chan<- *PartitionInfoContainer) {
	partitions, err := m.single.Partitions(t)
	if err != nil {
		log.Panic(err)
	}
	for _, pID := range partitions {
		m.perPartition(t, pID, groups, response)
	}
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
	for g := range groups {
		m.perGroup(t, g, pID, availableOffset, oldestOffset, response)
	}

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
	var lag int64
	switch true {
	case oldestOffset == availableOffset: // no work to be done
		lag = 0
	case consumerOffset == -1 && availableOffset > 0: // no consumer group with some work
		lag = availableOffset
	case consumerOffset == -1 && availableOffset == 0: // no consumer group and no work
		lag = 0
	default:
		lag = availableOffset - consumerOffset
	}
	res := &PartitionInfoContainer{
		PartitionInfo: &PartitionInfo{
			Start:  oldestOffset,
			End:    availableOffset,
			Offset: consumerOffset,
			Lag:    lag,
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
	if v.Timeout == time.Duration(0) {
		v.Timeout = m.timeOutDuration
	}
	t.Timeout = v.Timeout

	t.Version = 1
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

	res, err := controllerBroker.DeleteTopics(&sarama.DeleteTopicsRequest{
		Topics:  topics,
		Timeout: m.timeOutDuration,
		Version: 1,
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
	defer controllerBroker.Close()

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
		Timeout:         m.timeOutDuration,
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

// DeleteConsumerGroups removes consumer groups from kafka brokers. Error will be thrown
// if consumer groups have active consumer(s).
func (m *KafkaManager) DeleteConsumerGroups(withRefresh bool, groups ...string) error {
	var errMsg string
	brokerCGMap := make(map[int32][]string)
	for _, g := range groups {
		if withRefresh {
			if err := m.single.RefreshCoordinator(g); err != nil {
				return fmt.Errorf("cannot find coordinator for cg %s, %+v", g, err)
			}
		}
		b, err := m.single.Coordinator(g)
		if err != nil {
			return fmt.Errorf("cannot find coordinator for cg %s, %+v", g, err)
		}
		_, found := brokerCGMap[b.ID()]
		if found {
			brokerCGMap[b.ID()] = append(brokerCGMap[b.ID()], g)
		} else {
			brokerCGMap[b.ID()] = []string{g}
		}
	}
	for _, b := range m.single.Brokers() {
		v, found := brokerCGMap[b.ID()]
		if !found {
			continue
		}
		err := connectBroker(b, m.single.Config())
		if err != nil {
			return err
		}
		res, err := b.DeleteGroups(&sarama.DeleteGroupsRequest{
			Groups: v,
		})
		if err != nil {
			return err
		}

		for k, v := range res.GroupErrorCodes {
			if v == sarama.ErrNoError {
				continue
			}
			errMsg += fmt.Sprintf("%s (id: %s), ", v.Error(), k)
		}

	}
	if errMsg != "" {
		return errors.New(errMsg)
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
	Timeout           time.Duration
}

// Options returns the compatible options for creating topics
func (c CreateTopicOptions) Options() interface{} {
	return c
}
