package oplog

import (
	"context"
	"sync"
	"time"
)

type Event struct {
	Key       string
	Timestamp time.Time
	Fields    map[string]interface{}
}

type Sink interface {
	Close() error

	// Publish sends data to the server
	Publish(ctx context.Context, event Event) error

	// identifies the sink
	Name() string
}

type Output struct {
	mu    sync.RWMutex
	sinks []Sink
}

func NewOutput() *Output {
	return &Output{}
}

func (out *Output) AddSinks(s ...Sink) {
	out.mu.Lock()
	defer out.mu.Unlock()
	out.sinks = append(out.sinks, s...)
}

func (out *Output) Publish(ctx context.Context, event Event) (err error) {
	out.mu.RLock()
	sinks := out.sinks[:]
	out.mu.RUnlock()
	for _, sink := range sinks {
		if err = sink.Publish(ctx, event); err != nil {
			break
		}
	}
	return
}

func (out *Output) Close() {
	out.mu.Lock()
	sinks := out.sinks[:]
	out.sinks = nil
	out.mu.Unlock()

	for _, sink := range sinks {
		sink.Close()
	}
	return
}
