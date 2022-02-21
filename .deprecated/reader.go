package redmq

import (
	"context"
	"time"
)

type ReaderMessage struct {
	Reader
	Message
}

type Reader interface {
	Topic() string

	Cursor() string

	Seek(string)
	SeekByTime(time.Time)

	Receive(context.Context, int) ([]Message, error)
	Chan(context.Context, int) <-chan ReaderMessage
}
