package redmq

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("Failed to init test")
	}

	producer, err := client.CreateProducer(ctx, ProducerOptions{
		Topic:           testTopic,
		Name:            testProducerName,
		AutoCreateTopic: true,
		TopicOptions: TopicOptions{
			Topic: testTopic,
		},
		MaxLen: 10,
	})
	if err != nil {
		t.Fatalf("Failed to auto create topic when creating producer: %v", err)
	}

	if _, err := producer.Send(ctx, &ProducerMessage{
		Payload:    []byte("Hello World!"),
		Properties: map[string]string{"foo": "bar"},
		EventTime:  time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	for i := 0; i < 100; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload:    bytes.Repeat([]byte("Hello World!"), 10),
			Properties: map[string]string{"foo": "bar"},
			EventTime:  time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}
}
