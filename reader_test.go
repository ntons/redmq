package redmq

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("Failed to init test")
	}

	producer, err := client.CreateProducer(ctx, &ProducerOptions{
		Topic:           testTopic,
		Name:            testProducerName,
		AutoCreateTopic: true,
		TopicOptions: &TopicOptions{
			Topic:  testTopic,
			MaxLen: 10,
		},
	})
	if err != nil {
		t.Fatalf("Failed to auto create topic when creating producer: %v", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload:    bytes.Repeat([]byte("Hello World!"), 10),
			Properties: map[string]string{"foo": "bar"},
			EventTime:  time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// test creating a reader
	reader, err := client.CreateReader(ctx, &ReaderOptions{
		Topic: testTopic,
	})
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	// test read all 10 message from begin
	res, err := reader.Receive(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to receive messages: %v", err)
	} else if len(res) != 10 {
		t.Fatalf("Expect 10 Messages, but got: %d", len(res))
	}

	// reseek to the begin, and read all 10 message
	reader.Seek(ReaderCursorBegin)
	if res, err := reader.Receive(ctx, 100); err != nil {
		t.Fatalf("Failed to receive messages: %v", err)
	} else if len(res) != 10 {
		t.Fatalf("Expect 10 Messages, but got: %d", len(res))
	}

	// seek to the middle, and read left 5 message
	reader.Seek(res[4].ID())
	if res, err := reader.Receive(ctx, 100); err != nil {
		t.Fatalf("Failed to receive messages: %v", err)
	} else if len(res) != 5 {
		t.Fatalf("Expect 5 Messages, but got: %d", len(res))
	}

	go func() {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload:    bytes.Repeat([]byte("Hello World!"), 10),
			Properties: map[string]string{"foo": "bar"},
			EventTime:  time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}()

	// test waiting for new message
	if res, err = reader.Receive(ctx, 10); err != nil {
		t.Fatalf("Failed to receive messages: %v", err)
	} else if len(res) != 1 {
		t.Fatalf("Expect 1 Messages, but got: %d", len(res))
	}

	// wait util deadline
	if res, err = reader.Receive(ctx, 10); err == nil {
		t.Fatalf("Expect Deadline, but not")
	}
}
