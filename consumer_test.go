package redmq

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRetryDeadLetterScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("failed to new test client: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	if err := cli.CreateTopic(ctx, &TopicOptions{
		Topic: testTopic,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: topicKey(testTopic),
			Values: []string{"foo", "bar"},
		}).Err(); err != nil {
			log.Fatalf("failed to add message: %v", err)
		}
	}

	if err := rdb.XGroupCreate(ctx, topicKey(testTopic), testGroupID, "0").Err(); err != nil {
		log.Fatalf("failed to create group: %v", err)
	}

	// We consume 5 of the 10 but not ack them
	if r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{topicKey(testTopic), ">"},
		Group:    testGroupID,
		Consumer: "dog",
		Count:    5,
	}).Result(); err != nil {
		log.Fatalf("failed to read group: %v", err)
	} else if len(r) != 1 || len(r[0].Messages) != 5 {
		log.Fatalf("unexpected read group result lenght: %v", len(r))
	}

	time.Sleep(time.Millisecond * 10)

	// We exec the script
	if err := rdb.Eval(
		ctx,
		luaRetryDeadLetter,
		[]string{topicKey(testTopic)},
		testGroupID,
		"cat",
		5, // ms, MinIdleTime
		3, // MaxDeadLetterQueueLen
		1, // MaxRetries
		3, // MaxClaims
	).Err(); err != nil {
		log.Fatalf("failed to eval script: %v", err)
	}

	// Now, dog own 2 pending messages, cat own 3 pending messages

	time.Sleep(time.Millisecond * 10)

	// cat's 3 pending messages over max retries, should be delivery to dlq
	// and cat claim dog's 2 pending messages
	if _, err = rdb.Eval(
		ctx,
		luaRetryDeadLetter,
		[]string{topicKey(testTopic)},
		testGroupID,
		"cat",
		5, // ms, MinIdleTime
		3, // MaxDeadLetterQueueLen
		1, // MaxRetries
		3, // MaxClaims
	).Result(); err != nil {
		log.Fatalf("failed to eval script: %v", err)
	}
}
