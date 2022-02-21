package redmq

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("failed to create client")
	}

	var (
		po = ProducerOptions{
			Topic:           testTopic,
			Name:            testProducerName,
			MaxLen:          1000,
			AutoCreateTopic: true,
			TopicOptions: TopicOptions{
				Topic:                   testTopic,
				MinShrinkInterval:       3 * 1000,
				DeleteIdleTopicAfter:    20 * 1000,
				DeleteIdleGroupAfter:    15 * 1000,
				DeleteIdleConsumerAfter: 10 * 1000,
			},
		}
		co = ConsumerOptions{
			Topic:             testTopic,
			GroupID:           testGroupID,
			InitalialPosition: PositionBegin,
			Name:              "dog",
			DeadLetterPolicy: DeadLetterPolicy{
				DeliveryTimeout:       time.Second,
				MaxDeadLetterQueueLen: 200,
				MaxRetries:            2,
				MinRetryInterval:      time.Second / 2,
			},
		}

		wg       sync.WaitGroup
		produced []string
		consumed []string
	)

	producer, err := cli.CreateProducer(ctx, po)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	co.Name = "dog"
	dog, err := cli.Subscribe(ctx, co)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	co.Name = "cat"
	cat, err := cli.Subscribe(ctx, co)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			id, err := producer.Send(ctx, &ProducerMessage{
				Payload:   []byte("HelloWorld!"),
				EventTime: time.Now(),
			})
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("failed to send: %v", err)
				}
				return
			}
			produced = append(produced, id)

			//time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			a, err := dog.Receive(ctx, 10, time.Second/4)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("failed to receive: %v", err)
				}
				return
			}
			for _, m := range a {
				//log.Printf("dog receive: %v,%v,%s", m.ID(), m.Type(), m.Payload())
				if m.Type() == MessageTypeRetry || rand.Int()%2 == 0 {
					if err := m.Ack(ctx); err != nil {
						t.Fatalf("failed to acknowledge: %v", err)
					}
					consumed = append(consumed, m.ID())
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			a, err := cat.Receive(ctx, 10, time.Second/4)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("failed to receive: %v", err)
				}
				return
			}
			for _, m := range a {
				//log.Printf("cat receive: %v,%v,%s", m.ID(), m.Type(), m.Payload())
				if m.Type() == MessageTypeRetry || rand.Int()%2 == 0 {
					if err := m.Ack(ctx); err != nil {
						t.Fatalf("failed to acknowledge: %v", err)
					}
					consumed = append(consumed, m.ID())
				}
			}
		}
	}()

	wg.Wait()

	if len(produced) != len(consumed) {
		t.Fatalf("expect all produced message consumed, but not: %v,%v", len(produced), len(consumed))
	}
	sort.Strings(produced)
	sort.Strings(consumed)
	for i := 0; i < len(produced); i++ {
		if produced[i] != consumed[i] {
			t.Fatalf("expect all produced message consumed, but not: %v,%v", produced[i], consumed[i])
		}
	}
}
