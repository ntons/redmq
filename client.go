package redmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type Client interface {
	// CreateTopic creates topic explicitly
	CreateTopic(context.Context, *TopicOptions) error

	// DeteleTopic deletes topic
	DeleteTopics(context.Context, ...string) error

	// ListTopics lists all existed topics
	ListTopics(context.Context) ([]string, error)

	// CreateProducer Creates the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(context.Context, *ProducerOptions) (Producer, error)

	// Subscribe Creates a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(context.Context, *ConsumerOptions) (Consumer, error)

	// CreateReader Creates a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(context.Context, *ReaderOptions) (Reader, error)

	// Close Closes the Client and free associated resources
	Close()
}

var _ Client = (*client)(nil)

type client struct {
	redis.Cmdable
}

func NewClient(rdb redis.Cmdable) Client {
	c := &client{
		Cmdable: rdb,
	}
	return c
}

func (c *client) CreateTopic(ctx context.Context, opts *TopicOptions) (err error) {
	_, err = c.createTopic(ctx, opts)
	return
}

func (c *client) DeleteTopics(ctx context.Context, topics ...string) (err error) {
	if len(topics) == 0 {
		return
	}
	err = c.Del(ctx, topicRelatedKeys(topics...)...).Err()
	return
}

func (c *client) ListTopics(ctx context.Context) (topics []string, err error) {
	keys, err := c.Keys(ctx, topicMetaKey("*")).Result()
	if err != nil {
		return
	}
	for _, key := range keys {
		if topic, ok := parseTopicMetaKey(key); ok {
			topics = append(topics, topic)
		}
	}
	return
}

func (c *client) CreateProducer(ctx context.Context, opts *ProducerOptions) (_ Producer, err error) {
	if _, err = c.getTopicMeta(ctx, opts.Topic, opts.getTopicOptions()); err != nil {
		return
	}
	return newProducer(c.Cmdable, opts)
}

func (c *client) Subscribe(ctx context.Context, opts *ConsumerOptions) (_ Consumer, err error) {
	if _, err = c.getTopicMeta(ctx, opts.Topic, opts.getTopicOptions()); err != nil {
		return
	}
	return newConsumer(ctx, c.Cmdable, opts)
}

func (c *client) CreateReader(ctx context.Context, opts *ReaderOptions) (_ Reader, err error) {
	if _, err = c.getTopicMeta(ctx, opts.Topic, nil); err != nil {
		return
	}
	return newReader(c.Cmdable, opts)
}

func (c *client) Close() {
}

func (c *client) createTopic(ctx context.Context, opts *TopicOptions) (_ *topicMeta, err error) {
	if opts.Topic == "" {
		return nil, fmt.Errorf("Topic must be specified")
	}

	tm := newTopicMeta(opts)

	b, err := json.Marshal(tm)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal topic options")
	}

	ok, err := c.SetNX(ctx, topicMetaKey(opts.Topic), b, 0).Result()
	if err != nil {
		return
	}
	if !ok {
		return nil, TopicAlreadyExistsError
	}

	return tm, nil
}

// if opts not nil, create topic if not found automatically
func (c *client) getTopicMeta(ctx context.Context, topic string, opts *TopicOptions) (_ *topicMeta, err error) {
	var tm *topicMeta
	if opts != nil {
		opts.Topic = topic
		if tm, err = c.createTopic(ctx, opts); err != nil {
			if err != TopicAlreadyExistsError {
				return
			}
			err = nil
		}
	} else {
		var b []byte
		if b, err = c.Get(ctx, topicMetaKey(topic)).Bytes(); err != nil {
			if err == redis.Nil {
				err = TopicNotFoundError
			}
			return
		}
		tm = &topicMeta{}
		if err = json.Unmarshal(b, tm); err != nil {
			return
		}
	}
	return tm, nil
}
