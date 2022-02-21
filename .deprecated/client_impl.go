package redmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	redis.Cmdable
}

func NewClient(rdb redis.Cmdable) Client {
	client := &clientImpl{
		Cmdable: rdb,
	}
	return client
}

func (client *clientImpl) CreateTopic(ctx context.Context, opts *TopicOptions) (err error) {
	if opts.Topic == "" {
		return fmt.Errorf("Topic must be specified")
	}

	b, err := json.Marshal(newTopicMeta(opts))
	if err != nil {
		return fmt.Errorf("failed to marshal topic meta")
	}

	pipe := client.Pipeline()

	r := pipe.HSetNX(ctx, topicMetaKey(opts.Topic), topicMetaField, b)

	ts := time.Now().UnixNano() / 1e6
	pipe.HSet(ctx, topicMetaKey(opts.Topic), "topic-idle-since", ts)
	pipe.HSet(ctx, topicMetaKey(opts.Topic), "topic-shrink-at", ts)

	if _, err = pipe.Exec(ctx); err != nil {
		return
	}

	if !r.Val() {
		return TopicAlreadyExistsError
	}

	return
}

func (client *clientImpl) DeleteTopics(ctx context.Context, topics ...string) (err error) {
	return client.Del(ctx, topicRelatedKeys(topics...)...).Err()
}

func (client *clientImpl) ListTopics(ctx context.Context) (topics []string, err error) {
	keys, err := client.Keys(ctx, topicMetaKey("*")).Result()
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

func (client *clientImpl) CreateProducer(ctx context.Context, opts ProducerOptions) (_ Producer, err error) {
	if err = client.ensureTopic(ctx, opts.Topic, opts.getTopicOptions()); err != nil {
		return
	}
	return newProducer(client.Cmdable, &opts)
}

func (client *clientImpl) Subscribe(ctx context.Context, opts ConsumerOptions) (_ Consumer, err error) {
	if err = client.ensureTopic(ctx, opts.Topic, opts.getTopicOptions()); err != nil {
		return
	}
	return newConsumer(ctx, client.Cmdable, &opts)
}

func (client *clientImpl) CreateReader(ctx context.Context, opts ReaderOptions) (_ Reader, err error) {
	if err = client.ensureTopic(ctx, opts.Topic, nil); err != nil {
		return
	}
	return newReader(client.Cmdable, &opts)
}

func (client *clientImpl) Close() {
}

func (client *clientImpl) Shrink(ctx context.Context) (err error) {
	return client.Eval(ctx, luaShrink, []string{}).Err()
}

// if opts not nil, create topic if not found automatically
func (client *clientImpl) ensureTopic(ctx context.Context, topic string, opts *TopicOptions) error {
	if opts != nil {
		opts.Topic = topic
		if err := client.CreateTopic(ctx, opts); err != nil && err != TopicAlreadyExistsError {
			return err
		}
	} else {
		n, err := client.Exists(ctx, topicMetaKey(topic)).Result()
		if err != nil {
			return err
		}
		if n == 0 {
			return TopicNotFoundError
		}
	}
	return nil
}
