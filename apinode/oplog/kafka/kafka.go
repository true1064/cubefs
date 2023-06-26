package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/apinode/oplog"
)

type config struct {
	Addrs string `json:"addrs"`
	Topic string `json:"topic"`
}

type sink struct {
	producer sarama.SyncProducer
	topic    string
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
	conf.Version = sarama.V0_10_0_0
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Addrs, ","), conf)
	if err != nil {
		return nil, err
	}

	s := &sink{
		producer: producer,
		topic:    cfg.Topic,
	}
	return s, nil
}

func (s *sink) Close() error {
	s.producer.Close()
	return nil
}

func (s *sink) Publish(ctx context.Context, event oplog.Event) error {
	data, err := json.Marshal(event.Fields)
	if err != nil {
		return err
	}
	m := &sarama.ProducerMessage{Topic: s.topic, Key: sarama.StringEncoder(event.Key), Value: sarama.ByteEncoder(data), Timestamp: event.Timestamp}
	if _, _, err := s.producer.SendMessage(m); err != nil {
		return err
	}
	return nil
}

func (s *sink) Name() string {
	return "kafka"
}
