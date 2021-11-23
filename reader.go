package redmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/log-go"
)

const (
	ReaderCursorBegin = "0"
	ReaderCursorEnd   = "$"
)

type ReaderOptions struct {
	Topic string
}

type ReaderMessage struct {
	Reader
	Message
}

type Reader interface {
	Topic() string

	Cursor() string

	Seek(string)
	SeekByTime(time.Time)

	Receive(context.Context, int64) ([]Message, error)
	Chan(context.Context, int64) <-chan ReaderMessage
}

var _ Reader = (*reader)(nil)

type reader struct {
	redis.Cmdable
	*ReaderOptions

	cursor string
}

func newReader(rdb redis.Cmdable, opts *ReaderOptions) (_ Reader, err error) {
	if len(opts.Topic) == 0 {
		return nil, fmt.Errorf("Topic must be specified")
	}
	r := &reader{
		Cmdable:       rdb,
		ReaderOptions: opts,
		cursor:        ReaderCursorBegin,
	}
	return r, nil
}

func (r *reader) Topic() string { return r.ReaderOptions.Topic }

func (r *reader) Cursor() string { return r.cursor }

func (r *reader) Seek(id string) {
	r.cursor = id
}

func (r *reader) SeekByTime(t time.Time) {
	r.cursor = fmt.Sprintf("%d%03d-0", t.Second(), t.Nanosecond()/1e6)
}

func (r *reader) Receive(ctx context.Context, batchSize int64) (_ []Message, err error) {
	args := &redis.XReadArgs{
		Streams: []string{topicKey(r.Topic()), r.cursor},
		Count:   batchSize,
	}
	if deadline, ok := ctx.Deadline(); ok {
		args.Block = time.Until(deadline)
	} else {
		args.Block = 0
	}

	res, err := r.XRead(ctx, args).Result()
	if err != nil {
		return
	}

	ret := make([]Message, 0, len(res))
	for _, e1 := range res {
		for _, e2 := range e1.Messages {
			r.cursor = e2.ID
			if e2.Values == nil {
				log.Warnf("redmq.Reader: nil message found, topic=%v, id=%v", r.Topic(), e2.ID)
				continue
			}
			m, err := parseMessage(r.Topic(), e2.ID, e2.Values)
			if err != nil {
				log.Warnf("redmq.Reader: bad message found, topic=%v, id=%v, %v", r.Topic(), e2.ID, err)
				continue
			}
			ret = append(ret, m)
		}
	}
	return ret, nil
}

func (r *reader) Chan(ctx context.Context, batchSize int64) <-chan ReaderMessage {
	ch := make(chan ReaderMessage, 1)
	go func() {
		defer func() { close(ch) }()
		for {
			arr, err := r.Receive(ctx, 1)
			if err != nil {
				return
			}
			for _, m := range arr {
				ch <- ReaderMessage{r, m}
			}
		}
	}()
	return ch
}
