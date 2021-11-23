package redmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type ConsumerMessage struct {
	Consumer
	Message
}

type Consumer interface {
	// Name returns the name of consumer.
	Name() string

	// Topic returns the subscribed topic
	Topic() string

	// GroupID returns the subscription group id
	GroupID() string

	// Unsubscribe the consumer
	Unsubscribe() error

	// Receive a single message.
	// This calls blocks until a message is available.
	Receive(context.Context) (Message, error)

	// Chan returns a channel to consume messages from
	Chan() <-chan ConsumerMessage

	// Ack the consumption of a single message
	Ack(Message)

	// AckID the consumption of a single message, identified by its MessageID
	AckID(string)

	// ReconsumeLater mark a message for redelivery after custom delay
	ReconsumeLater(msg Message, delay time.Duration)

	// Acknowledge the failure to process a single message.
	//
	// When a message is "negatively acked" it will be marked for redelivery after
	// some fixed delay. The delay is configurable when constructing the consumer
	// with ConsumerOptions.NAckRedeliveryDelay .
	//
	// This call is not blocking.
	Nack(Message)

	// Acknowledge the failure to process a single message.
	//
	// When a message is "negatively acked" it will be marked for redelivery after
	// some fixed delay. The delay is configurable when constructing the consumer
	// with ConsumerOptions.NackRedeliveryDelay .
	//
	// This call is not blocking.
	NackID(string)

	// Close the consumer and stop the broker to push more messages
	Close()

	// Reset the subscription associated with this consumer to a specific message id.
	// The message id can either be a specific message or represent the first or last messages in the topic.
	//
	// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
	//       seek() on the individual partitions.
	Seek(string) error

	// Reset the subscription associated with this consumer to a specific message publish time.
	//
	// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
	// the individual partitions.
	//
	// @param timestamp
	//            the message publish time where to reposition the subscription
	//
	SeekByTime(time time.Time) error
}

type consumer struct {
	redis.Cmdable
	*ConsumerOptions
}

func newConsumer(rdb redis.Cmdable, opts *ConsumerOptions) (Consumer, error) {
	if len(opts.Topic) == 0 {
		return nil, fmt.Errorf("Topic(s) must be specified")
	}
	if len(opts.Name) == 0 {
		return nil, fmt.Errorf("Consumer name must be specified")
	}
	return &consumer{
		Cmdable:         rdb,
		ConsumerOptions: opts,
	}, nil
}

func (c *consumer) Name() string { return c.ConsumerOptions.Name }

func (c *consumer) Topic() string { return c.ConsumerOptions.Topic }

func (c *consumer) GroupID() string { return c.ConsumerOptions.GroupID }

func (c *consumer) Unsubscribe() error {
	// TODO 明确地取消注册，把pending messages给别人，删除consumer
	return nil
}

func (c *consumer) Receive(ctx context.Context) (_ Message, err error) {
	// Create group
	if err = c.XGroupCreate(
		ctx, c.Topic(), c.GroupID(), "0").Err(); err != nil {
		return
	}

	// Read group
	args := &redis.XReadGroupArgs{
		Group:    c.GroupID(),
		Consumer: c.Name(),
		Streams:  []string{c.Topic(), ">"},
		Count:    1,
	}
	if deadline, ok := ctx.Deadline(); ok {
		args.Block = time.Until(deadline)
	}

	r, err := c.XReadGroup(ctx, args).Result()
	if err != nil {
		return
	}
	fmt.Println(r)

	return nil, nil
}

func (c *consumer) Chan() <-chan ConsumerMessage {
	return nil
}

func (c *consumer) Ack(Message) {
}

func (c *consumer) AckID(string) {
}

func (c *consumer) ReconsumeLater(msg Message, delay time.Duration) {
}

func (c *consumer) Nack(Message) {
}

func (c *consumer) NackID(string) {
}

func (c *consumer) Close() {
}

func (c *consumer) Seek(string) error {
	return nil
}

func (c *consumer) SeekByTime(time time.Time) error {
	return nil
}

type ConsumerOptions struct {
	// Specify the topic this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topic string

	// Specify a list of topics this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	//Topics []string

	// Specify a regular expression to subscribe to multiple topics under the same namespace.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	//TopicsPattern string

	// AutoCreateTopic specify creating topic if not exists
	AutoCreateTopic bool

	*TopicOptions

	// Specify the interval in which to poll for new partitions or new topics if using a TopicsPattern.
	AutoDiscoveryPeriod time.Duration

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	//SubscriptionName string
	GroupID string

	// Attach a set of application defined properties to the consumer
	// This properties will be visible in the topic stats
	Properties map[string]string

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Exclusive`
	Type SubscriptionType

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Configuration for Dead Letter Queue consumer policy.
	// eg. route the message to topic X after N failed attempts at processing it
	// By default is nil and there's no DLQ
	//DLQ *DLQPolicy

	// Configuration for Key Shared consumer policy.
	//KeySharedPolicy *KeySharedPolicy

	// Auto retry send messages to default filled DLQPolicy topics
	// Default is false
	RetryEnable bool

	// Sets a `MessageChannel` for the consumer
	// When a message is received, it will be pushed to the channel for consumption
	//MessageChannel chan ConsumerMessage

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize int

	// The delay after which to redeliver the messages that failed to be
	// processed. Default is 1min. (See `Consumer.Nack()`)
	NackRedeliveryDelay time.Duration

	// Set the consumer name.
	Name string

	// If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
	// of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
	// each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
	// point, the messages will be sent as normal.
	//
	// ReadCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer (i.e.
	//  failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics or on a
	//  shared subscription, will lead to the subscription call throwing a PulsarClientException.
	ReadCompacted bool

	// Mark the subscription as replicated to keep it in sync across clusters
	ReplicateSubscriptionState bool

	// A chain of interceptors, These interceptors will be called at some points defined in ConsumerInterceptor interface.
	//Interceptors ConsumerInterceptors

	//Schema Schema

	// MaxReconnectToBroker set the maximum retry number of reconnectToBroker. (default: ultimate)
	MaxReconnectToBroker *uint

	// Decryption decryption related fields to decrypt the encrypted message
	//Decryption *MessageDecryptionInfo
}

func (o *ConsumerOptions) getTopicOptions() *TopicOptions {
	if o.AutoCreateTopic {
		return o.TopicOptions
	}
	return nil
}
