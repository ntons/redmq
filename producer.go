package redmq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Producer interface {
	// Topic return the topic to which producer is publishing to
	Topic() string

	// Name return the producer name which could have been assigned by the system or specified by the client
	Name() string

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(ctx, &ProducerMessage{ Payload: myPayload })
	Send(context.Context, *ProducerMessage) (string, error)
}

// Implementation of Producer interface
type producer struct {
	redis.Cmdable
	*ProducerOptions
}

func newProducer(rdb redis.Cmdable, opts *ProducerOptions) (p Producer, err error) {
	return &producer{
		Cmdable:         rdb,
		ProducerOptions: opts,
	}, nil
}

func (p *producer) Topic() string { return p.ProducerOptions.Topic }

func (p *producer) Name() string { return p.ProducerOptions.Name }

func (p *producer) Send(ctx context.Context, msg *ProducerMessage) (_ string, err error) {
	m := message{
		XPayload:      msg.Payload,
		XProducerName: p.Name(),
		XPublishTime:  time.Now(),
		XEventTime:    msg.EventTime,
		XProperties:   msg.Properties,
	}

	args := &redis.XAddArgs{
		Stream: topicKey(p.Topic()),
		Values: m.toValues(),
	}
	if p.MaxLen > 0 {
		args.MaxLen = p.MaxLen
		args.Approx = true
	}

	id, err := p.XAdd(ctx, args).Result()
	if err != nil {
		return
	}

	return id, nil
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
	*TopicOptions

	// MaxLen specify the max size of topic retentions
	MaxLen int64
}

func (o *ProducerOptions) getTopicOptions() *TopicOptions {
	if o.AutoCreateTopic {
		return o.TopicOptions
	}
	return nil
}
