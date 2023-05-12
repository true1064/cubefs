package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/apinode/oplog"
)

type config struct {
	Addrs string `json:"addrs"`
	Topic string `json:"topic"`
}

type sink struct {
	producer sarama.AsyncProducer
	topic    string
	stopc    chan struct{}
	wg       sync.WaitGroup
}

func NewKafkaSink(filename string) (oplog.Sink, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	cfg := config{}
	if err = json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	conf := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(strings.Split(cfg.Addrs, ","), conf)
	if err != nil {
		return nil, err
	}

	s := &sink{
		producer: producer,
		topic:    cfg.Topic,
		stopc:    make(chan struct{}),
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.producer.Errors():
			case <-s.stopc:
				return
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.producer.Successes():
			case <-s.stopc:
				return
			}
		}
	}()
	return s, nil
}

func (s *sink) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return nil
}

func (s *sink) Publish(ctx context.Context, event oplog.Event) error {
	data, err := json.Marshal(event.Fields)
	if err != nil {
		return err
	}
	select {
	case s.producer.Input() <- &sarama.ProducerMessage{Topic: s.topic, Key: sarama.StringEncoder(event.Key), Value: sarama.ByteEncoder(data), Timestamp: event.Timestamp}:
	case <-s.stopc:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *sink) Name() string {
	return "kafka"
}
