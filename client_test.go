package redmq

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/log-go/config"
)

const (
	testTopic        = "my-test-topic"
	testGroupID      = "my-test-group"
	testProducerName = "my-test-producer"
)

func newTestClient(ctx context.Context) (_ Client, err error) {
	config.DefaultZapConsoleConfig.Use()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	client := NewClient(rdb)

	topics, err := client.ListTopics(ctx)
	if err != nil {
		return
	}

	if len(topics) > 0 {
		if err = client.DeleteTopics(ctx, topics...); err != nil {
			return
		}
	}

	return client, nil
}

func TestClientTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("Failed to init test")
	}

	if err := client.CreateTopic(ctx, &TopicOptions{
		Topic: testTopic,
	}); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	if topics, err := client.ListTopics(ctx); err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	} else {
		found := false
		for _, topic := range topics {
			if found = topic == testTopic; found {
				break
			}
		}
		if !found {
			t.Fatalf("Topic created not found in list")
		}
	}

	if err := client.DeleteTopics(ctx, testTopic); err != nil {
		t.Fatalf("Failed to delete topics: %v", err)
	}

	if topics, err := client.ListTopics(ctx); err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	} else {
		found := false
		for _, topic := range topics {
			if found = topic == testTopic; found {
				break
			}
		}
		if found {
			t.Fatalf("Topic found after deleting")
		}
	}
}

func TestClientCreateProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := newTestClient(ctx)
	if err != nil {
		return
	}

	if _, err = client.CreateProducer(ctx, &ProducerOptions{
		Topic:           testTopic,
		Name:            testProducerName,
		AutoCreateTopic: false, // notice
	}); err != TopicNotFoundError {
		t.Fatalf("Create producer with auto create topic expect fail but not: %v", err)
	}

	if _, err = client.CreateProducer(ctx, &ProducerOptions{
		Topic:           testTopic,
		Name:            testProducerName,
		AutoCreateTopic: true, // notice
		TopicOptions: &TopicOptions{
			Topic: testTopic,
		},
		MaxLen: 10,
	}); err != nil {
		t.Fatalf("Failed to auto create topic when creating producer: %v", err)
	}

	if topics, err := client.ListTopics(ctx); err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	} else {
		found := false
		for _, topic := range topics {
			if found = topic == testTopic; found {
				break
			}
		}
		if !found {
			t.Fatalf("Auto created topic was not been listed")
		}
	}

	if err := client.DeleteTopics(ctx, testTopic); err != nil {
		t.Fatalf("Failed to delete topics: %v", err)
	}
}
