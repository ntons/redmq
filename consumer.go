package redmq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/log-go"
)

type ConsumerMessage struct {
	Consumer
	Message
}

func (cm *ConsumerMessage) Ack(ctx context.Context) error {
	return cm.Consumer.Ack(ctx, cm.Message)
}

type Consumer interface {
	// Topic returns the subscribed topic
	Topic() string

	// GroupID returns the subscription group id
	GroupID() string

	// Name returns the name of consumer.
	Name() string

	// Unsubscribe the consumer
	Unsubscribe(context.Context) error

	// Receive a single message.
	// This calls blocks until a message is available.
	Receive(context.Context, int) ([]Message, error)

	// Chan returns a channel to consume messages from
	Chan(context.Context, int) <-chan ConsumerMessage

	// Ack the consumption of a single message
	Ack(context.Context, Message) error

	// AckID the consumption of a single message, identified by its MessageID
	AckID(context.Context, string) error

	// Close the consumer and stop the broker to push more messages
	Close()
}

type consumer struct {
	redis.Cmdable
	*ConsumerOptions

	pendingReceived bool
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
	c := &consumer{
		Cmdable:         rdb,
		ConsumerOptions: opts,
	}
	// Create group
	if err = c.XGroupCreate(ctx, topicKey(c.Topic()), c.GroupID(), c.InitalialPosition).Err(); err != nil {
		if !strings.HasPrefix(strings.TrimLeft(err.Error(), " "), "BUSYGROUP") {
			return nil, fmt.Errorf("redmq.Consumer: failed to create consumer group: %v", err)
		}
		err = nil // already exists
	}
	return c, nil
}

func (c *consumer) Topic() string { return c.ConsumerOptions.Topic }

func (c *consumer) Name() string { return c.ConsumerOptions.Name }

func (c *consumer) GroupID() string { return c.ConsumerOptions.GroupID }

func (c *consumer) Unsubscribe(ctx context.Context) (err error) {
	if err = c.XGroupDelConsumer(ctx, topicKey(c.Topic()), c.GroupID(), c.Name()).Err(); err != nil {
		return
	}
	return
}

func (c *consumer) recvPending(ctx context.Context, batchSize int) (_ []redis.XStream, err error) {
	if c.pendingReceived {
		return
	}
	args := &redis.XReadGroupArgs{
		Group:    c.GroupID(),
		Consumer: c.Name(),
		Streams:  []string{topicKey(c.Topic()), "0"},
		Count:    int64(batchSize),
	}
	res, err := c.XReadGroup(ctx, args).Result()
	if err != nil {
		return nil, fmt.Errorf("redmq.Consumer: failed to read pending message: %v", err)
	}
	if len(res) == 0 {
		c.pendingReceived = true
	}
	return res, nil
}

func (c *consumer) recvRetry(ctx context.Context, batchSize int) (_ []redis.XStream, err error) {
	if c.DeadLetterPolicy == nil {
		return
	}

	// TODO policy for retry and dlq process

	v, err := c.Eval(
		ctx,
		luaRetryDeadLetter,
		[]string{topicKey(c.Topic())},
		c.GroupID(),
		c.Name(),
		c.DeliveryTimeout,
		c.MaxDeadLetterQueueLen,
		c.MaxRetries,
		batchSize,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("redmq.Consumer: failed to retry dead letter: %v", err)
	}

	r := redis.XStream{
		Stream: topicKey(c.Topic()),
	}
	for _, a := range v.([]interface{}) {
		a1 := a.([]interface{})
		m := redis.XMessage{
			ID:     a1[0].(string),
			Values: make(map[string]interface{}),
		}
		a2 := a1[1].([]interface{})
		for i := 1; i < len(a2); i += 2 {
			m.Values[a2[i-1].(string)] = a2[i]
		}
		r.Messages = append(r.Messages, m)
	}

	return []redis.XStream{r}, nil
}

func (c *consumer) recvNormal(ctx context.Context, batchSize int) ([]redis.XStream, error) {
	args := &redis.XReadGroupArgs{
		Group:    c.GroupID(),
		Consumer: c.Name(),
		Streams:  []string{topicKey(c.Topic()), ">"},
		Count:    int64(batchSize),
	}
	if deadline, ok := ctx.Deadline(); ok {
		args.Block = time.Until(deadline)
	} else {
		args.Block = 0
	}
	return c.XReadGroup(ctx, args).Result()
}

func (c *consumer) Receive(ctx context.Context, batchSize int) (_ []Message, err error) {
	var (
		t = MessageTypePending
		a []redis.XStream
	)
	if a, err = c.recvPending(ctx, batchSize); err != nil {
		return
	}
	if len(a) == 0 {
		t = MessageTypeRetry
		if a, err = c.recvRetry(ctx, batchSize); err != nil {
			return
		}
	}
	if len(a) == 0 {
		t = MessageTypeNormal
		if a, err = c.recvNormal(ctx, batchSize); err != nil {
			return
		}
	}

	r := make([]Message, 0, len(a))
	for _, e1 := range a {
		for _, e2 := range e1.Messages {
			if e2.Values == nil {
				log.Warnf("redmq.Consumer: nil message found, topic=%v, id=%v", c.Topic(), e2.ID)
				continue
			}
			m, err := parseMessage(t, c.Topic(), e2.ID, e2.Values)
			if err != nil {
				log.Warnf("redmq.Consumer: bad message found, topic=%v, id=%v, %v", c.Topic(), e2.ID, err)
				continue
			}
			r = append(r, m)
		}
	}

	return r, nil
}

func (c *consumer) Chan(ctx context.Context, batchSize int) <-chan ConsumerMessage {
	ch := make(chan ConsumerMessage, batchSize)
	go func() {
		defer func() { close(ch) }()
		for {
			arr, err := c.Receive(ctx, batchSize)
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
				ch <- ConsumerMessage{c, m}
			}
		}
	}()
	return ch
}

func (c *consumer) Ack(ctx context.Context, m Message) (err error) {
	return c.AckID(ctx, m.ID())
}

func (c *consumer) AckID(ctx context.Context, id string) (err error) {
	if err = c.XAck(ctx, topicKey(c.Topic()), c.GroupID(), id).Err(); err != nil {
		return fmt.Errorf("redmq.Consumer: failed to acknowledge message: %v", err)
	}
	return
}

func (c *consumer) Close() {
}

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

	*TopicOptions

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	//SubscriptionName string
	GroupID string

	//
	InitalialPosition string

	// Set the consumer name.
	Name string

	// Dead letter and retry policy
	*DeadLetterPolicy
}

func (o *ConsumerOptions) getTopicOptions() *TopicOptions {
	if o.AutoCreateTopic {
		return o.TopicOptions
	}
	return nil
}

var (
	// KEYS={
	//   1:Stream,
	// }
	// ARGV={
	//   1:Group,
	//   2:Consumer,
	//   3:MinIdleTime aka DeliveryTimeout,
	//   4:MaxDeadLetterQueueLen,
	//   5:MaxRetries,
	//   6:MaxClaims,
	// }
	//
	// XPENDING-1 O(1)      summary only
	// XPENDING-2 O(N)
	// XADD       O(1)
	// XACK       O(1)
	// XDEL       O(1)
	// XCLAIM     O(logN)
	luaRetryDeadLetter = `
local K,G,A2,A3,A4,A5,A6=KEYS[1],ARGV[1],ARGV[2],tonumber(ARGV[3]),tonumber(ARGV[4]),tonumber(ARGV[5]),tonumber(ARGV[6])
local r=redis.call("XPENDING",K,G)
local a=redis.call("XPENDING",K,G,r[2],r[3],r[1])
local dlq,rty={},{}
for _,e in ipairs(a) do if e[3]>A3 then if e[4]>A5 then dlq[#dlq+1]=e[1] elseif #rty<A6 then rty[#rty+1]=e[1] end end end
for _,e in ipairs(dlq) do redis.call("XADD",K..".dlq","MAXLEN","~",A4,"*","ID",e,unpack(redis.call("XRANGE",K,e,e)[1][2])) end
if #dlq>0 then redis.call("XACK",K,G,unpack(dlq)) redis.call("XDEL",K,unpack(dlq)) end
if #rty==0 then return {} end
return redis.call("XCLAIM",K,G,A2,A3,unpack(rty))
`
)
