package kafka_test

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/stretchr/testify/assert"
	"github.com/veritone/go-messaging-lib/kafka"
)

func Test_manager(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, "create_topic_test_1", "create_topic_test_2")
	if err != nil {
		log.Panic(err)
	}
	res, err := m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	val, ok := res.(kafka.ListTopicsResponse)
	if !ok {
		Err(t, errors.New("invalid response from ListTopics"))
	}
	spew.Dump(val)
	p1, ok := val["create_topic_test_1"][""]
	assert.Equal(t, true, ok, "topic 'create_topic_test_1' should exist")
	assert.Equal(t, 5, len(p1), "number of partitions for 'create_topic_test_1' should be 5")

	p2, ok := val["create_topic_test_2"][""]
	assert.Equal(t, true, ok, "topic 'create_topic_test_2' should exist")
	assert.Equal(t, 5, len(p2), "number of partitions for 'create_topic_test_1' should be 5")
	Err(t, m.Close())
}

func Test_reusable_manager(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, "create_topic_test_1", "create_topic_test_2")
	if err != nil {
		log.Panic(err)
	}
	res, err := m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	for i := 0; i < 1000; i++ {
		_, ok := res.(kafka.ListTopicsResponse)
		if !ok {
			Err(t, errors.New("invalid response from ListTopics"))
		}
	}
	Err(t, m.Close())
}

func TestManagerDeleteTopics(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, "create_topic_test_1",
		"create_topic_test_2",
		"create_topic_test_3",
		"create_topic_test_4")
	if err != nil {
		log.Panic(err)
	}
	res, err := m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	topics, _ := res.(kafka.ListTopicsResponse)
	assert.NotEmpty(t, topics["create_topic_test_1"], "create_topic_test_1 should be created")
	assert.NotEmpty(t, topics["create_topic_test_2"], "create_topic_test_2 should be created")
	assert.NotEmpty(t, topics["create_topic_test_3"], "create_topic_test_3 should be created")

	log.Println("Test delete single topic ")
	err = m.DeleteTopics(context.TODO(), "create_topic_test_2")
	if err != nil {
		log.Panic(err)
	}
	res, err = m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	topics, _ = res.(kafka.ListTopicsResponse)
	assert.NotEmpty(t, topics["create_topic_test_1"], "create_topic_test_1 should exist")
	assert.Empty(t, topics["create_topic_test_2"], "create_topic_test_2 should be deleted")
	assert.NotEmpty(t, topics["create_topic_test_3"], "create_topic_test_3 should exist")

	log.Println("Test delete multiple topics")
	err = m.DeleteTopics(context.TODO(), "create_topic_test_1", "create_topic_test_3")
	if err != nil {
		log.Panic(err)
	}
	res, err = m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	topics, _ = res.(kafka.ListTopicsResponse)
	assert.Empty(t, topics["create_topic_test_1"], "create_topic_test_1 should be deleted")
	assert.Empty(t, topics["create_topic_test_2"], "create_topic_test_2 should be deleted")
	assert.Empty(t, topics["create_topic_test_3"], "create_topic_test_3 should be deleted")

	Err(t, m.Close())
}

func TestManagerAddPartitions(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, "create_topic_test_1",
		"create_topic_test_2",
		"create_topic_test_3",
		"create_topic_test_4")
	if err != nil {
		log.Panic(err)
	}
	res, err := m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	topics, _ := res.(kafka.ListTopicsResponse)
	assert.Equal(t, 2, len(topics["create_topic_test_1"][""]), "create_topic_test_1 topic should have 2 partitions")
	assert.Equal(t, 2, len(topics["create_topic_test_2"][""]), "create_topic_test_2 topic should have 2 partitions")
	assert.Equal(t, 2, len(topics["create_topic_test_3"][""]), "create_topic_test_3 topic should have 2 partitions")
	assert.Equal(t, 2, len(topics["create_topic_test_4"][""]), "create_topic_test_4 topic should have 2 partitions")

	err = m.AddPartitions(context.TODO(), kafka.TopicPartitionRequest{
		"create_topic_test_1": 10,
		"create_topic_test_2": 5,
	})
	assert.NoError(t, err, "AddPartitions should succeed")

	err = m.AddPartitions(context.TODO(), kafka.TopicPartitionRequest{
		"create_topic_test_3": 2,
	})
	assert.EqualError(t, err, kafka.ErrSamePartitionCount.Error(), "error for same partition count")

	err = m.AddPartitions(context.TODO(), kafka.TopicPartitionRequest{
		"create_topic_test_4": 0,
	})
	assert.EqualError(t, err, kafka.ErrInvalidPartitionCount.Error(), "error for removing partitions")

	err = m.AddPartitions(context.TODO(), kafka.TopicPartitionRequest{
		"create_topic_test_invalid": 2,
	})
	assert.EqualError(t, err, kafka.ErrInvalidTopic.Error(), "topic does not exist")

	res, err = m.ListTopics(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	topics, _ = res.(kafka.ListTopicsResponse)
	assert.Equal(t, 10, len(topics["create_topic_test_1"][""]), "create_topic_test_1 topic should have 10 partitions")
	assert.Equal(t, 5, len(topics["create_topic_test_2"][""]), "create_topic_test_2 topic should have 5 partitions")
	Err(t, m.Close())
}

func TestManagerListTopicsLite(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	err = m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, "create_topic_test_1",
		"create_topic_test_2",
		"create_topic_test_3",
		"create_topic_test_4")
	if err != nil {
		log.Panic(err)
	}

	c1, _ := kafka.Consumer("create_topic_test_1", "g1", "localhost:9093", "localhost:9094", "localhost:9095")
	c1.Consume(context.TODO(), kafka.ConsumerGroupOption)
	c2, _ := kafka.Consumer("create_topic_test_2", "g2", "localhost:9093", "localhost:9094", "localhost:9095")
	c2.Consume(context.TODO(), kafka.ConsumerGroupOption)
	c3, err := kafka.Consumer("create_topic_test_3", "g3", "localhost:9093", "localhost:9094", "localhost:9095")
	c3.Consume(context.TODO(), kafka.ConsumerGroupOption)
	c4, err := kafka.Consumer("create_topic_test_1", "g4", "localhost:9093", "localhost:9094", "localhost:9095")
	c4.Consume(context.TODO(), kafka.ConsumerGroupOption)

	p1, _ := kafka.Producer("create_topic_test_1", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	p2, _ := kafka.Producer("create_topic_test_2", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	p3, _ := kafka.Producer("create_topic_test_3", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	p4, _ := kafka.Producer("create_topic_test_4", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	msg, _ := kafka.NewMessage("", []byte("test message"))
	p1.Produce(context.TODO(), msg)
	p2.Produce(context.TODO(), msg)
	p3.Produce(context.TODO(), msg)
	p4.Produce(context.TODO(), msg)

	topics, consumerGroups, err := m.ListTopicsLite(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, 5, len(topics), "5 topics should be created, including __consumer_offets")
	log.Println(spew.Sprint(topics))
	assert.Equal(t, 4, len(consumerGroups), "4 consumer groups should be created")
	log.Println(spew.Sprint(consumerGroups))
	Err(t, m.Close())
}

func TestManagerGetPartitionInfo(t *testing.T) {
	multiBrokerSetup(t)
	defer func() {
		// Throw a breakpoint here for troubleshooting
		// Teardown can cause EOF for consumer. Comment it out if run into issue while testing
		tearDown(t)
	}()
	m, err := kafka.Manager("localhost:9093", "localhost:9094", "localhost:9095")
	if err != nil {
		log.Panic(err)
	}
	m.CreateTopics(context.TODO(), kafka.CreateTopicOptions{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, "create_topic_test_1",
		"create_topic_test_2",
		"create_topic_test_3",
		"create_topic_test_4")

	p1, _ := kafka.Producer("create_topic_test_1", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	msg, _ := kafka.NewMessage("", make([]byte, 999000))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Close())

	c4, _ := kafka.Consumer("create_topic_test_1", "g4", "localhost:9093")
	q, _ := c4.Consume(context.TODO(), kafka.ConsumerGroupOption)
	// consume only 2/3 messages
	for i := 0; i < 6; i++ {
		<-q
	}
	Err(t, c4.Close())

	p1, _ = kafka.Producer("create_topic_test_1", kafka.StrategyRoundRobin, "localhost:9093", "localhost:9094", "localhost:9095")
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Produce(context.TODO(), msg))
	Err(t, p1.Close())

	// // Offset commits every one second by default
	// // https://github.com/Shopify/sarama/blob/master/config.go#L238
	// time.Sleep(time.Second * 2)
	results, err := m.GetPartitionInfo("create_topic_test_1", "g4", true)
	if err != nil {
		log.Panic(err)
	}

	assert.Equal(t, int64(1), results[0].PartitionInfo.Lag, "5 topics should be created, including __consumer_offets")
	assert.Equal(t, int64(1), results[1].PartitionInfo.Lag, "5 topics should be created, including __consumer_offets")
	m.Close()
}

func TestManagerDeleteConsumerGroups(t *testing.T) {
	multiBrokerSetup(t)
	defer tearDown(t)

	brokers := []string{"kafka1:9093", "localhost:9094", "localhost:9095"}

	makeProducer := func(wg *sync.WaitGroup, topic string, brokers ...string) {
		p, _ := kafka.Producer(topic, kafka.StrategyRoundRobin, brokers...)
		m, _ := kafka.NewMessage("", []byte("test payload"))
		e := p.Produce(context.TODO(), m)
		Err(t, e)
		wg.Done()
	}
	var wgP sync.WaitGroup
	wgP.Add(3)
	go makeProducer(&wgP, "tpoic_test_1", brokers...)
	go makeProducer(&wgP, "tpoic_test_2", brokers...)
	go makeProducer(&wgP, "tpoic_test_3", brokers...)
	wgP.Wait()

	makeConsumer := func(wg *sync.WaitGroup, topic, group string, brokers ...string) {
		c, _ := kafka.Consumer(topic, group, brokers...)
		c.Consume(context.TODO(), kafka.ConsumerGroupOption)
		time.Sleep(2 * time.Second)
		c.Close()
		time.Sleep(2 * time.Second)
		wg.Done()
	}
	var wgC sync.WaitGroup
	wgC.Add(5)
	go makeConsumer(&wgC, "topic_test_1", "g1", brokers...)
	go makeConsumer(&wgC, "topic_test_1", "g2", brokers...)
	go makeConsumer(&wgC, "topic_test_1", "g3", brokers...)
	go makeConsumer(&wgC, "topic_test_2", "g4", brokers...)
	go makeConsumer(&wgC, "topic_test_3", "g5", brokers...)
	wgC.Wait()

	m, _ := kafka.Manager("kafka1:9093")
	topics, groups, err := m.ListTopicsLite(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	spew.Dump(topics, groups)
	err = m.DeleteConsumerGroups(true, "g3", "g4", "g5")
	if err != nil {
		log.Panic(err)
	}
	_, groups, err = m.ListTopicsLite(context.TODO())
	if err != nil {
		log.Panic(err)
	}
	for _, g := range groups {
		switch g {
		case "g3", "g4", "g5":
			assert.Fail(t, "groups are not remove properly", spew.Sdump(groups))
		}
	}
}
