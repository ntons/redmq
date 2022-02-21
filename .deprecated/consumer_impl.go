package redmq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/log-go"
)

var _ Consumer = (*consumerImpl)(nil)

type consumerImpl struct {
	redis.Cmdable
	*ConsumerOptions

	pendingReceived bool

	lastRetryTime time.Time
}

func newConsumer(ctx context.Context, rdb redis.Cmdable, opts *ConsumerOptions) (_ Consumer, err error) {
	if len(opts.Topic) == 0 {
		return nil, fmt.Errorf("Consumer Topic must be specified")
	}
	if len(opts.Name) == 0 {
		return nil, fmt.Errorf("Consumer Name must be specified")
	}
	if len(opts.GroupID) == 0 {
		return nil, fmt.Errorf("Consumer GroupID must be specified")
	}

	consumer := &consumerImpl{
		Cmdable:         rdb,
		ConsumerOptions: opts,
	}

	ts := time.Now().UnixNano() / 1e6
	pipe := consumer.Pipeline()
	pipe.HSet(ctx, topicMetaKey(consumer.Topic()), "topic-idle-since", ts)
	pipe.HSet(ctx, topicMetaKey(consumer.Topic()), groupIdleSinceField(consumer.GroupID()), ts)
	pipe.XGroupCreateMkStream(ctx, topicKey(consumer.Topic()), consumer.GroupID(), consumer.InitalialPosition)

	if _, err = pipe.Exec(ctx); err != nil {
		if !strings.HasSuffix(
			strings.TrimSpace(err.Error()),
			"BUSYGROUP Consumer Group name already exists",
		) {
			return nil, fmt.Errorf("redmq.Consumer: failed to create consumer group: %w", err)
		}
		err = nil // already exists
	}

	// Consumer will be created on receiving automatically
	return consumer, nil
}

func (consumer *consumerImpl) Topic() string { return consumer.ConsumerOptions.Topic }

func (consumer *consumerImpl) Name() string { return consumer.ConsumerOptions.Name }

func (consumer *consumerImpl) GroupID() string { return consumer.ConsumerOptions.GroupID }

func (consumer *consumerImpl) Unsubscribe(ctx context.Context) (err error) {
	ts := time.Now().UnixNano() / 1e6
	pipe := consumer.Pipeline()
	pipe.HSet(ctx, topicMetaKey(consumer.Topic()), topicIdleSinceField, ts)
	pipe.HSet(ctx, topicMetaKey(consumer.Topic()), groupIdleSinceField(consumer.GroupID()), ts)
	pipe.XGroupDelConsumer(ctx, topicKey(consumer.Topic()), consumer.GroupID(), consumer.Name())
	if _, err = pipe.Exec(ctx); err != nil {
		return
	}
	return
}

func (consumer *consumerImpl) recvPending(ctx context.Context, count int) (_ []redis.XMessage, err error) {
	if consumer.pendingReceived {
		return
	}

	r, err := consumer.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumer.GroupID(),
		Consumer: consumer.Name(),
		Streams:  []string{topicKey(consumer.Topic()), PositionBegin},
		Count:    int64(count),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redmq.Consumer: failed to read pending message: %w", err)
	}

	var x []redis.XMessage
	for _, s := range r {
		x = append(x, s.Messages...)
	}
	if len(x) < count {
		consumer.pendingReceived = true
	}
	return x, nil
}

func (consumer *consumerImpl) recvRetry(ctx context.Context, count int) (_ []redis.XMessage, err error) {
	now := time.Now()
	if now.Sub(consumer.lastRetryTime) < consumer.MinRetryInterval {
		return
	}
	consumer.lastRetryTime = now

	r, err := consumer.Eval(
		ctx,
		luaRetryDeadLetter,
		[]string{topicKey(consumer.Topic())},
		consumer.GroupID(),
		consumer.Name(),
		int64(consumer.DeliveryTimeout/time.Millisecond),
		consumer.MaxDeadLetterQueueLen,
		consumer.MaxRetries,
		count,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("redmq.Consumer: failed to retry dead letter: %w", err)
	}

	var x []redis.XMessage
	for _, a := range r.([]interface{}) {
		a1 := a.([]interface{})
		m := redis.XMessage{
			ID:     a1[0].(string),
			Values: make(map[string]interface{}),
		}
		a2 := a1[1].([]interface{})
		for i := 1; i < len(a2); i += 2 {
			m.Values[a2[i-1].(string)] = a2[i]
		}
		x = append(x, m)
	}

	return x, nil
}

func (consumer *consumerImpl) recvNormal(ctx context.Context, count int, block time.Duration) ([]redis.XMessage, error) {
	r, err := consumer.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumer.GroupID(),
		Consumer: consumer.Name(),
		Streams:  []string{topicKey(consumer.Topic()), ">"},
		Count:    int64(count),
		Block:    block,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, fmt.Errorf("redmq.Consumer: failed to read message: %w", err)
		} else {
			return nil, nil
		}
	}

	var x []redis.XMessage
	for _, s := range r {
		x = append(x, s.Messages...)
	}
	if len(x) < count {
		consumer.pendingReceived = true
	}
	return x, nil
}

func (consumer *consumerImpl) Receive(ctx context.Context, count int, block time.Duration) (_ []ConsumerMessage, err error) {
	var (
		t = MessageTypePending
		a []redis.XMessage
	)
	if a, err = consumer.recvPending(ctx, count); err != nil {
		return
	}
	if len(a) == 0 {
		t = MessageTypeRetry
		if a, err = consumer.recvRetry(ctx, count); err != nil {
			return
		}
	}
	if len(a) == 0 {
		t = MessageTypeNormal
		if a, err = consumer.recvNormal(ctx, count, block); err != nil {
			return
		}
	}

	r := make([]ConsumerMessage, 0)
	for _, e := range a {
		if e.Values == nil {
			log.Warnf("redmq.Consumer: nil message found, topic=%v, id=%v", consumer.Topic(), e.ID)
			continue
		}
		m, err := parseMessage(t, consumer.Topic(), e.ID, e.Values)
		if err != nil {
			log.Warnf("redmq.Consumer: bad message found, topic=%v, id=%v, %v", consumer.Topic(), e.ID, err)
			continue
		}
		r = append(r, ConsumerMessage{consumer, m})
	}

	return r, nil
}

func (consumer *consumerImpl) Chan(ctx context.Context, count int) <-chan ConsumerMessage {
	ch := make(chan ConsumerMessage, count)
	go func() {
		defer func() { close(ch) }()
		for {
			arr, err := consumer.Receive(ctx, count, 10*time.Second)
			if err != nil {
				log.Warnf("redmq.Consumer: failed to receive: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			for _, m := range arr {
				ch <- ConsumerMessage{consumer, m}
			}
		}
	}()
	return ch
}

func (consumer *consumerImpl) Ack(ctx context.Context, m Message) (err error) {
	return consumer.AckID(ctx, m.ID())
}

func (consumer *consumerImpl) AckID(ctx context.Context, id string) (err error) {
	if err = consumer.XAck(ctx, topicKey(consumer.Topic()), consumer.GroupID(), id).Err(); err != nil {
		return fmt.Errorf("redmq.Consumer: failed to acknowledge message: %w", err)
	}
	return
}

func (consumer *consumerImpl) Close() {}

// 死信、重试
// 首先要明白，只要投递超时，即认为该消息为死信。
// 重试是一种死信处理机制，给死信复活的机会。
// 在超过重试次数之后，采用兜底策略扔进死信队列。
type DeadLetterPolicy struct {
	// Acknowledge timeout duration
	DeliveryTimeout time.Duration

	// Max length of Dead Letter Queue
	MaxDeadLetterQueueLen int64

	// Max retries before put into DLQ
	// By default, 0, which disables retry
	MaxRetries int32

	// Min retry check interval
	MinRetryInterval time.Duration
}

type ConsumerOptions struct {
	// Specify the topic this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topic string

	// Specify a list of topics this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	//Topics []string

	// AutoCreateTopic specify creating topic if not exists
	AutoCreateTopic bool

	TopicOptions

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	//SubscriptionName string
	GroupID string

	//
	InitalialPosition string

	// Set the consumer name.
	Name string

	// Dead letter and retry policy
	DeadLetterPolicy
}

func (o *ConsumerOptions) getTopicOptions() *TopicOptions {
	if o.AutoCreateTopic {
		return &o.TopicOptions
	} else {
		return nil
	}
}
