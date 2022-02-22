package redmq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ntons/log-go"
	"github.com/ntons/redis"
	"google.golang.org/protobuf/proto"
)

type Client interface {
	Send(ctx context.Context, msg *Msg, opts ...SendOption) error

	Read(ctx context.Context, count int64, topicCursorPairs ...string) ([]*Msg, error)

	Serve(ctx context.Context)

	Watch(ctx context.Context, topic, cursor string, callback func(*Msg)) error
}

var _ Client = (*client)(nil)

type client struct {
	db redis.Client

	poll *poll

	idGen uint64
}

func New(db redis.Client) Client {
	return &client{
		db:   db,
		poll: newPoll(),
	}
}

func (cli *client) Send(ctx context.Context, _msg *Msg, opts ...SendOption) (err error) {
	o := &sendOptions{}
	for _, opt := range opts {
		opt.apply(o)
	}

	msg := proto.Clone(_msg).(*Msg)
	msg.Id = ""
	msg.PublishTime = time.Now().Unix() / 1e6

	args := &redis.XAddArgs{
		Stream:     msg.Topic,
		Values:     encMsg(msg),
		NoMkStream: !o.autoCreate,
	}
	if o.maxLen > 0 {
		args.MaxLen, args.Approx = o.maxLen, true
	}

	if err = cli.db.XAdd(ctx, args).Err(); err != nil {
		return newUnavailableError("redis failure: %v", err)
	}
	return
}

func (cli *client) Read(ctx context.Context, count int64, topicCursorPairs ...string) (msgs []*Msg, err error) {
	if n := len(topicCursorPairs); n == 0 {
		return
	} else if n%2 != 0 {
		return nil, fmt.Errorf("invalid arguments: bad topic cursor pairs")
	}

	args := &redis.XReadArgs{
		Streams: make([]string, 0, len(topicCursorPairs)),
		Count:   count,
		Block:   -1,
	}
	for i, n := 0, len(topicCursorPairs); i < n; i += 2 {
		args.Streams = append(args.Streams, topicCursorPairs[i])
	}
	for i, n := 1, len(topicCursorPairs); i < n; i += 2 {
		args.Streams = append(args.Streams, topicCursorPairs[i])
	}

	streams, err := cli.db.XRead(ctx, args).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis failure: %v", err)
	} else {
		err = nil
	}

	for _, s := range streams {
		for _, m := range s.Messages {
			msg, err := decMsg(m)
			if err != nil {
				log.Warnf("failed to decode msg: %v", err)
				continue // TODO How to do？
			}
			msg.Topic = s.Stream
			msgs = append(msgs, msg)
		}
	}

	return
}

func (cli *client) Watch(ctx context.Context, topic, cursor string, callback func(*Msg)) (err error) {
	var (
		id = atomic.AddUint64(&cli.idGen, 1)
		ch = make(chan *Msg, 100)
	)

	if err = cli.poll.Add(ctx, id, topic, cursor, ch); err != nil {
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		cli.poll.Del(ctx, id, topic)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			callback(msg)
		}
	}
}

func (cli *client) WatchChan(ctx context.Context, topic, cursor string, ch chan<- *Msg) (err error) {
	return cli.Watch(ctx, topic, cursor, func(msg *Msg) { ch <- msg })
}

func (cli *client) Serve(ctx context.Context) {
	cli.poll.Serve(ctx, cli.db)
}
