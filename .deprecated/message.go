package redmq

import (
	"encoding/json"
	"fmt"
	"time"
)

type MessageType int

const (
	// Normal message type
	MessageTypeNormal MessageType = iota

	// Pending message which not been acked last time received
	MessageTypePending

	// Retry message claimed from another consumer
	MessageTypeRetry
)

type Message interface {
	// Topic get the topic from which this message originated from
	Topic() string

	// ProducerName returns the name of the producer that has published the message.
	ProducerName() string

	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties() map[string]string

	// Payload get the payload of the message
	Payload() []byte

	// ID get the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	ID() string

	// PublishTime get the publish time of this message. The publish time is the timestamp that a client
	// publish the message.
	PublishTime() time.Time

	// EventTime get the event time associated with this message. It is typically set by the applications via
	// `ProducerMessage.EventTime`.
	// If EventTime is 0, it means there isn't any event time associated with this message.
	EventTime() time.Time

	Type() MessageType
}

type message struct {
	XTopic   string      `json:"-"`
	XID      string      `json:"-"`
	XPayload []byte      `json:"-"`
	XType    MessageType `json:"-"`
	// Serializable fields
	XProducerName string            `json:"producer-name,omitempty"`
	XPublishTime  time.Time         `json:"publish-time,omitempty"`
	XEventTime    time.Time         `json:"event-time,omitempty"`
	XProperties   map[string]string `json:"properties,omitempty"`
}

func parseMessage(mtype MessageType, topic, id string, values map[string]interface{}) (_ Message, err error) {
	var m = message{XType: mtype}
	// parse metadata
	if v, ok := values["METADATA"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected message metadata type: %T", v)
		}
		if err = json.Unmarshal(s2b(s), &m); err != nil {
			return nil, fmt.Errorf("failed to umarshal message metadata")
		}
	}
	// parse payload
	if v, ok := values["PAYLOAD"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected message payload type")
		}
		m.XPayload = s2b(s)
	}
	m.XTopic = topic
	m.XID = id
	return &m, nil
}

func (m *message) toValues() []interface{} {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Errorf("Failed to marshal message"))
	}
	return []interface{}{"METADATA", b, "PAYLOAD", m.XPayload}
}

// Implement Message interface
var _ Message = (*message)(nil)

func (m *message) Topic() string { return m.XTopic }

func (m *message) ID() string { return m.XID }

func (m *message) ProducerName() string { return m.XProducerName }

func (m *message) Properties() map[string]string { return m.XProperties }

func (m *message) Payload() []byte { return m.XPayload }

func (m *message) PublishTime() time.Time { return m.XPublishTime }

func (m *message) EventTime() time.Time { return m.XEventTime }

func (m *message) Type() MessageType { return m.XType }
