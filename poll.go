package redmq

import (
	"context"
	"sync"
	"time"

	"github.com/ntons/log-go"
	"github.com/ntons/redis"
)

type pollAgent struct {
	cursor string
	ch     chan<- *Msg
}

type pollEntry struct {
	cursor string
	agents map[uint64]*pollAgent
}

func newPollEntry() *pollEntry {
	return &pollEntry{
		cursor: "9999999999999-0",
		agents: make(map[uint64]*pollAgent),
	}
}

func (e *pollEntry) Empty() bool {
	return len(e.agents) == 0
}

func (e *pollEntry) Add(agentId uint64, cursor string, ch chan<- *Msg) {
	if msgIdLess(cursor, e.cursor) {
		e.cursor = cursor
	}
	e.agents[agentId] = &pollAgent{cursor: cursor, ch: ch}
}

func (e *pollEntry) Del(agentId uint64) {
	delete(e.agents, agentId)
}

type pollAddEvent struct {
	agentId uint64
	topic   string
	cursor  string
	ch      chan<- *Msg
}
type pollDelEvent struct {
	agentId uint64
	topic   string
}

type poll struct {
	mu        sync.Mutex
	addEvents chan pollAddEvent
	delEvents chan pollDelEvent
}

func newPoll() *poll {
	return &poll{
		addEvents: make(chan pollAddEvent, 10),
		delEvents: make(chan pollDelEvent, 10),
	}
}

func (p *poll) Add(ctx context.Context, agentId uint64, topic, cursor string, ch chan<- *Msg) (err error) {
	ev := pollAddEvent{
		agentId: agentId,
		topic:   topic,
		cursor:  cursor,
		ch:      ch,
	}
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case p.addEvents <- ev:
	}
	return
}

func (p *poll) Del(ctx context.Context, agentId uint64, topic string) (err error) {
	ev := pollDelEvent{
		agentId: agentId,
		topic:   topic,
	}
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case p.delEvents <- ev:
	}
	return
}

func (p *poll) Serve(ctx context.Context, db redis.Client) {
	entries := make(map[string]*pollEntry)
	for {
		select {
		case <-ctx.Done():
			return
		case v := <-p.addEvents:
			e, ok := entries[v.topic]
			if !ok {
				e = newPollEntry()
				entries[v.topic] = e
			}
			e.Add(v.agentId, v.cursor, v.ch)
		case v := <-p.delEvents:
			if e, ok := entries[v.topic]; ok {
				if e.Del(v.agentId); e.Empty() {
					delete(entries, v.topic)
				}
			}
		default:
		}

		var topics, cursors []string
		for topic, e := range entries {
			topics = append(topics, topic)
			cursors = append(cursors, e.cursor)
		}

		streams, err := db.XRead(ctx, &redis.XReadArgs{
			Streams: append(topics, cursors...),
			Count:   10,
			Block:   200 * time.Millisecond,
		}).Result()
		if err != nil && err != redis.Nil {
			log.Warnf("redis error: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}

		for _, s := range streams {
			e, ok := entries[s.Stream]
			if !ok {
				continue
			}
			for _, m := range s.Messages {
				msg, err := decMsg(m)
				if err != nil {
					log.Warnf("failed to decode msg: %v", err)
					continue // TODO How to do？
				}
				msg.Topic = s.Stream

				e.cursor = msg.Id
				for _, agent := range e.agents {
					if !msgIdLess(agent.cursor, msg.Id) {
						continue
					}
					select {
					case agent.ch <- msg:
						agent.cursor = msg.Id
					default:
						log.Warnf("failed to send msg to agent")
						// TODO How to do?
					}
				}
			}
		}
	}
}
