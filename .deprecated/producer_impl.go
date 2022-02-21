package redmq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

var _ Producer = (*producerImpl)(nil)

// Implementation of Producer interface
type producerImpl struct {
	redis.Cmdable
	*ProducerOptions
}

func newProducer(rdb redis.Cmdable, opts *ProducerOptions) (Producer, error) {
	return &producerImpl{
		Cmdable:         rdb,
		ProducerOptions: opts,
	}, nil
}

func (producer *producerImpl) Topic() string { return producer.ProducerOptions.Topic }

func (producer *producerImpl) Name() string { return producer.ProducerOptions.Name }

func (producer *producerImpl) Send(ctx context.Context, msg *ProducerMessage) (_ string, err error) {
	pipe := producer.Pipeline()
	pipe.HSet(ctx, topicMetaKey(producer.Topic()), "topic-idle-since", time.Now().UnixNano()/1e6)

	m := message{
		XPayload:      msg.Payload,
		XProducerName: producer.Name(),
		XPublishTime:  time.Now(),
		XEventTime:    msg.EventTime,
		XProperties:   msg.Properties,
	}

	args := &redis.XAddArgs{
		Stream: topicKey(producer.Topic()),
		Values: m.toValues(),
	}
	if producer.MaxLen > 0 {
		args.MaxLen = producer.MaxLen
		args.Approx = true
	}

	r := pipe.XAdd(ctx, args)

	if _, err = pipe.Exec(ctx); err != nil {
		return
	}

	return r.Result()
}

// PruducerOptions specify options for creating Producer
type ProducerOptions struct {
	// Topic specify the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic string

	// Name specify a name for the producer
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.ProducerName().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	Name string

	// AutoCreateTopic specify creating topic if not exists.
	// By default, the topic should be created expicited.
	AutoCreateTopic bool

	// TopicOptions specify the options for AutoCreateTopic
	TopicOptions

	// MaxLen specify the max size of topic retentions
	MaxLen int64
}

func (o *ProducerOptions) getTopicOptions() *TopicOptions {
	if o.AutoCreateTopic {
		return &o.TopicOptions
	} else {
		return nil
	}
}
