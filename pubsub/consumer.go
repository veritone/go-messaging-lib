package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	ps "cloud.google.com/go/pubsub"
	messaging "github.com/veritone/go-messaging-lib"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

type PubSubConsumer struct {
	*ps.Client
	*ps.Topic
	*ps.Subscription
	messaging.Logger
	*messaging.Tracer
	subsriptionID string
	topicID       string
	projectID     string
	errors        chan error
}

func NewConsumer(ctx context.Context, topic, subId, projectId, credentialFile string) (*PubSubConsumer, error) {
	var (
		t   *ps.Topic
		sub *ps.Subscription
		err error
	)

	c, err := ps.NewClient(
		ctx,
		projectId,
		option.WithCredentialsFile(credentialFile))
	if err != nil {
		return nil, fmt.Errorf("error creating Consumer Client %v", err)
	}
	t = c.TopicInProject(topic, projectId)
	tExist, err := t.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("error checking for topic exsistence %v", err)
	}
	if !tExist {
		t, err = c.CreateTopic(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("error creating topic %s, %v", topic, err)
		}
	}
	sub = c.Subscription(subId)
	exist, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("error checking for subscription existence %v", err)
	}
	if exist {
		config, e := sub.Config(ctx)
		if e != nil {
			return nil, fmt.Errorf("error retrieving subscription configuration %v", err)
		}
		if t.String() != config.Topic.String() {
			return nil, fmt.Errorf("subscription exists but tied to a different topic %s", config.Topic.String())
		}
	} else {
		sub, err = c.CreateSubscription(ctx, subId, ps.SubscriptionConfig{
			Topic: t,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating subscription %s, %v", subId, err)
		}
	}
	return &PubSubConsumer{
		Client:        c,
		Topic:         t,
		Subscription:  sub,
		errors:        make(chan error, 100),
		topicID:       topic,
		subsriptionID: subId,
		Logger:        messaging.MustAddLogger(""),
		Tracer:        messaging.MustAddTracer("", ""),
	}, nil
}

func (c *PubSubConsumer) Consume(ctx context.Context, opts messaging.OptionCreator) (<-chan messaging.Event, error) {
	options, ok := opts.Options().(*PubSubConsumerOptions)
	if !ok {
		return nil, errors.New("options should be created with *pubsub.PubSubConsumerOptions")
	}
	msgs := make(chan messaging.Event, 1)
	go func() {
		err := c.Subscription.Receive(ctx, func(ctx context.Context, m *ps.Message) {
			msgs <- &event{m}
			if !options.ManualAck {
				m.Ack()
			}
			fields := append([]zap.Field{
				zap.String("ID", m.ID),
				zap.Time("PublishTime", m.PublishTime),
				zap.ByteString("Data", m.Data)}, messaging.MapToLogger(m.Attributes)...)
			c.Debug("received from queue", fields...)
			eventName := messaging.NameFromEvent(m.Data)
			messageLatest.WithLabelValues("consumer", c.Topic.ID(), c.Subscription.ID(), eventName).Set(float64(time.Now().UnixNano()) / 1e9)
			messagesProcessed.WithLabelValues("consumer", c.Topic.ID(), c.Subscription.ID(), eventName).Inc()
			bytesProcessed.WithLabelValues("consumer", c.Topic.ID(), c.Subscription.ID(), eventName).Add(float64(len(m.Data)))
		})
		errorsCount.WithLabelValues("consumer", c.Topic.ID(), c.Subscription.ID(), "").Inc()
		errorLatest.WithLabelValues("consumer", c.Topic.ID(), c.Subscription.ID(), "").Set(float64(time.Now().UnixNano()) / 1e9)
		c.errors <- err
		close(msgs)
	}()
	return msgs, nil
}

func (c *PubSubConsumer) Close() error {
	for e := range c.errors {
		c.Error(e.Error())
	}
	return c.Client.Close()
}
