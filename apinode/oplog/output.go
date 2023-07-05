package oplog

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type Event struct {
	Key       string
	Timestamp time.Time
	Fields    map[string]interface{}
}

type Handler interface {
	ConsumerEvent(ctx context.Context, e Event)
}

type Sink interface {
	Close() error

	// Publish sends data to the server
	Publish(ctx context.Context, event Event) error

	// identifies the sink
	Name() string

	StartConsumer(h Handler)
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
	span := trace.SpanFromContextSafe(ctx)
	start := time.Now()
	defer func() {
		span.AppendTrackLog("oplog", start, err)
	}()
	for _, sink := range sinks {
		if err = sink.Publish(ctx, event); err != nil {
			span.Errorf("publish event %v error: %v", event, err)
			break
		}
	}
	return
}

func (out *Output) StartConsumer(name string, h Handler) {
	out.mu.RLock()
	sinks := out.sinks[:]
	out.mu.RUnlock()
	for _, sink := range sinks {
		if sink.Name() == name {
			sink.StartConsumer(h)
			break
		}
	}
}

func (out *Output) Close() {
	out.mu.Lock()
	sinks := out.sinks[:]
	out.sinks = nil
	out.mu.Unlock()

	for _, sink := range sinks {
		sink.Close()
	}
}
