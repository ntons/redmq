package redmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/log-go"
)

const (
	PositionBegin = "0"
	PositionEnd   = "$"
)

var _ Reader = (*readerImpl)(nil)

type readerImpl struct {
	redis.Cmdable
	*ReaderOptions

	cursor string
}

func newReader(rdb redis.Cmdable, opts *ReaderOptions) (_ Reader, err error) {
	if len(opts.Topic) == 0 {
		return nil, fmt.Errorf("Topic must be specified")
	}
	reader := &readerImpl{
		Cmdable:       rdb,
		ReaderOptions: opts,
		cursor:        PositionBegin,
	}
	return reader, nil
}

func (reader *readerImpl) Topic() string { return reader.ReaderOptions.Topic }

func (reader *readerImpl) Cursor() string { return reader.cursor }

func (reader *readerImpl) Seek(id string) { reader.cursor = id }

func (reader *readerImpl) SeekByTime(t time.Time) {
	reader.cursor = fmt.Sprintf("%d%03d-0", t.Second(), t.Nanosecond()/1e6)
}

func (reader *readerImpl) Receive(ctx context.Context, batchSize int) (_ []Message, err error) {
	pipe := reader.Pipeline()
	pipe.HSet(ctx, topicMetaKey(reader.Topic()), "topic-idle-since", time.Now().UnixNano()/1e6)

	args := &redis.XReadArgs{
		Streams: []string{topicKey(reader.Topic()), reader.cursor},
		Count:   int64(batchSize),
	}
	if deadline, ok := ctx.Deadline(); ok {
		args.Block = time.Until(deadline)
	} else {
		args.Block = 0
	}

	res := pipe.XRead(ctx, args)

	if _, err = pipe.Exec(ctx); err != nil {
		return
	}

	ret := make([]Message, 0)
	for _, e1 := range res.Val() {
		for _, e2 := range e1.Messages {
			reader.cursor = e2.ID
			if e2.Values == nil {
				log.Warnf("redmq.Reader: nil message found, topic=%v, id=%v", reader.Topic(), e2.ID)
				continue
			}
			m, err := parseMessage(MessageTypeNormal, reader.Topic(), e2.ID, e2.Values)
			if err != nil {
				log.Warnf("redmq.Reader: bad message found, topic=%v, id=%v, %v", reader.Topic(), e2.ID, err)
				continue
			}
			ret = append(ret, m)
		}
	}
	return ret, nil
}

func (reader *readerImpl) Chan(ctx context.Context, batchSize int) <-chan ReaderMessage {
	ch := make(chan ReaderMessage, batchSize)
	go func() {
		defer func() { close(ch) }()
		for {
			arr, err := reader.Receive(ctx, batchSize)
			if err != nil {
				log.Warnf("redmq.Reader: failed to receive: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			for _, m := range arr {
				ch <- ReaderMessage{reader, m}
			}
		}
	}()
	return ch
}

type ReaderOptions struct {
	Topic string
}
