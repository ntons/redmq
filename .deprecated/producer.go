package redmq

import (
	"context"
	"time"
)

type ProducerMessage struct {
	// Payload for the message
	Payload []byte

	// Properties attach application defined properties on the message
	Properties map[string]string

	// EventTime set the event time for a given message
	// By default, messages don't have an event time associated, while the publish
	// time will be be always present.
	// Set the event time to a non-zero timestamp to explicitly declare the time
	// that the event "happened", as opposed to when the message is being published.
	EventTime time.Time
}

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
