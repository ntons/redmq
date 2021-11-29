package redmq

import (
	"context"
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
	CreateProducer(context.Context, ProducerOptions) (Producer, error)

	// Subscribe Creates a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(context.Context, ConsumerOptions) (Consumer, error)

	// CreateReader Creates a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(context.Context, ReaderOptions) (Reader, error)

	// Close Closes the Client and free associated resources
	Close()

	Shrink(context.Context) error
}
