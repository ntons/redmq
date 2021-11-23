package redmq

type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode, multiple consumer will be able to use the same
	// subscription and all messages with the same key will be dispatched to only one consumer
	KeyShared
)

type SubscriptionInitialPosition int

const (
	// Latest position which means the start consuming position will be the last message
	SubscriptionPositionLatest SubscriptionInitialPosition = iota

	// Earliest position which means the start consuming position will be the first message
	SubscriptionPositionEarliest
)
