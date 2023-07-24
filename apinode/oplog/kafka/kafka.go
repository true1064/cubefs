package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/apinode/oplog"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var consumerGroup = "cfa-oplog"

type sink struct {
	topic         string
	producer      sarama.SyncProducer
	consumerGroup sarama.ConsumerGroup
	handler       oplog.Handler
	wg            sync.WaitGroup
	once          sync.Once
	stopCtx       context.Context
	stopCancel    context.CancelFunc
}

func NewKafkaSink(addrs string, topic string) (oplog.Sink, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_1_0_0
	conf.Metadata.RefreshFrequency = 120 * time.Second
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true

	kafkaAddrs := strings.Split(addrs, ",")
	producer, err := sarama.NewSyncProducer(kafkaAddrs, conf)
	if err != nil {
		return nil, err
	}

	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	conf.Consumer.Offsets.CommitInterval = time.Second
	consumerGroup, err := sarama.NewConsumerGroup(kafkaAddrs, consumerGroup, conf)
	if err != nil {
		producer.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{
		topic:         topic,
		producer:      producer,
		consumerGroup: consumerGroup,
		stopCtx:       ctx,
		stopCancel:    cancel,
	}
	return s, nil
}

func (s *sink) Close() error {
	s.stopCancel()
	s.producer.Close()
	s.consumerGroup.Close()
	s.wg.Wait()
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

func (s *sink) StartConsumer(h oplog.Handler) {
	s.once.Do(func() {
		s.wg.Add(1)
		s.handler = h
		go func() {
			defer s.wg.Done()
			for {
				if err := s.consumerGroup.Consume(s.stopCtx, []string{s.topic}, s); err != nil {
					log.Panicf("consume error: %v", err)
				}

				log.Infof("Rebalance partition for topic %s", s.topic)
				if s.stopCtx.Err() != nil {
					return
				}
			}
		}()
	})
}

func (s *sink) Name() string {
	return "kafka"
}

func (s *sink) Setup(sess sarama.ConsumerGroupSession) error {
	partitions, ok := sess.Claims()[s.topic]
	if !ok {
		return fmt.Errorf("not found topic %s", s.topic)
	}
	log.Infof("kafka consumer group setup, partitions: %v", partitions)
	return nil
}

func (s *sink) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (s *sink) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	type task struct {
		e    oplog.Event
		done chan struct{}
	}

	ctx := sess.Context()
	ch := make(chan task)
	stopc := make(chan struct{})

	go func() {
		for {
			select {
			case t := <-ch:
				s.handler.ConsumerEvent(ctx, t.e)
				close(t.done)
			case <-stopc:
				return
			}
		}
	}()
	defer close(stopc)
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}
			e := oplog.Event{
				Key:       string(msg.Key),
				Timestamp: msg.Timestamp,
			}
			log.Debugf("consume message from partition %d offset %d", msg.Partition, msg.Offset)
			if err := json.Unmarshal(msg.Value, &e.Fields); err != nil {
				log.Errorf("unmarshal kafka msg error: %v", err)
			} else {
				t := task{
					e:    e,
					done: make(chan struct{}),
				}
				select {
				case ch <- t:
				case <-ctx.Done():
					return nil
				}

				select {
				case <-t.done:
				case <-ctx.Done():
					return nil
				}
			}
			sess.MarkMessage(msg, "")
		case <-ctx.Done():
			return nil
		}
	}
}
