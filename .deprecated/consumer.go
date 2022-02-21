package redmq

import (
	"context"
	"time"
)

type ConsumerMessage struct {
	Consumer
	Message
}

func (m ConsumerMessage) Topic() string {
	return m.Message.Topic()
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
	Receive(context.Context, int, time.Duration) ([]ConsumerMessage, error)

	// Chan returns a channel to consume messages from
	Chan(context.Context, int) <-chan ConsumerMessage

	// Ack the consumption of a single message
	Ack(context.Context, Message) error

	// AckID the consumption of a single message, identified by its MessageID
	AckID(context.Context, string) error

	// Close the consumer and stop the broker to push more messages
	Close()
}
