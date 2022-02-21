package redmq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	logcfg "github.com/ntons/log-go/config"
	"github.com/ntons/redis"
)

func initTest(topics ...string) Client {
	logcfg.DefaultZapConsoleConfig.Use()

	url := "redis://127.0.0.1:6379"
	opt, _ := redis.ParseURL(url)
	db := redis.NewClient(opt)

	db.Del(context.TODO(), topics...)

	return New(db)
}

func TestSendRead(t *testing.T) {
	const topic = "test-topic"
	var cursor = "0-0"

	cli := initTest(topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 12; i++ {
		if err := cli.Send(ctx, &Msg{
			Topic:        topic,
			ProducerName: "test-producer",
			EventTime:    time.Now().UnixNano() / 1e6,
			Properties:   map[string]string{"foo": fmt.Sprintf("%d", i)},
		}, 10); err != nil {
			t.Errorf("failed to send: %v", err)
		}
	}

	for {
		msgs, err := cli.Read(ctx, 5, topic, cursor)
		if err != nil {
			t.Errorf("failed to read: %v", err)
		}
		for _, msg := range msgs {
			fmt.Println(msg)
		}
		if len(msgs) > 0 {
			cursor = msgs[len(msgs)-1].Id
		} else {
			break
		}
	}
}

func TestWatch(t *testing.T) {
	const topic1 = "test-topic1"
	const topic2 = "test-topic2"

	cli := initTest(topic1, topic2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cli.Serve(ctx)
		fmt.Println("cli.Serve exited")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 12; i++ {
			if err := cli.Send(ctx, &Msg{
				Topic:        topic1,
				ProducerName: "test-producer",
				EventTime:    time.Now().UnixNano() / 1e6,
				Properties:   map[string]string{"foo": fmt.Sprintf("%d", i)},
			}, 10); err != nil {
				t.Errorf("failed to send: %v", err)
			}
			<-time.After(time.Duration(rand.Int63n(100)) * time.Millisecond)
		}
		fmt.Println("cli.Send exited")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 12; i++ {
			if err := cli.Send(ctx, &Msg{
				Topic:        topic2,
				ProducerName: "test-producer",
				EventTime:    time.Now().UnixNano() / 1e6,
				Properties:   map[string]string{"foo": fmt.Sprintf("%d", i)},
			}, 10); err != nil {
				t.Errorf("failed to send: %v", err)
			}
			<-time.After(time.Duration(rand.Int63n(100)) * time.Millisecond)
		}
		fmt.Println("cli.Send exited")
	}()

	for i := 0; i < 3; i++ {
		i := i
		<-time.After(time.Duration(rand.Int63n(100)) * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cli.Watch(ctx, topic1, "0-0", func(msg *Msg) {
				fmt.Printf("watch[1-%d]: %v\n", i, msg)
			}); err != nil {
				t.Errorf("failed to watch: %v", err)
			}
			fmt.Println("cli.Watch exited")
		}()
	}
	for i := 0; i < 3; i++ {
		i := i
		<-time.After(time.Duration(rand.Int63n(100)) * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cli.Watch(ctx, topic2, "0-0", func(msg *Msg) {
				fmt.Printf("watch[2-%d]: %v\n", i, msg)
			}); err != nil {
				t.Errorf("failed to watch: %v", err)
			}
			fmt.Println("cli.Watch exited")
		}()
	}
}
