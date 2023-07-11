package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/cubefs/cubefs/apinode/oplog"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaSink(t *testing.T) {
	config := "{\"addrs\": \"127.0.0.1:9050\", \"topic\": \"cfa-unit-test\"}"
	os.WriteFile("/tmp/kafka.conf", []byte(config), 0o600)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:9050")
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("cfa-unit-test", 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("cfa-unit-test", 0, sarama.OffsetOldest, 0).
			SetOffset("cfa-unit-test", 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	sink, err := NewKafkaSink("/tmp/kafka.conf")
	require.Nil(t, err)
	sink.Close()
	broker.Close()
}

func TestPublish(t *testing.T) {
	config := "{\"addrs\": \"127.0.0.1:9050\", \"topic\": \"cfa-unit-test\"}"
	os.WriteFile("/tmp/kafka.conf", []byte(config), 0o600)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	broker := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:9050")
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("cfa-unit-test", 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("cfa-unit-test", 0, sarama.OffsetOldest, 0).
			SetOffset("cfa-unit-test", 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	s, err := NewKafkaSink("/tmp/kafka.conf")
	require.Nil(t, err)
	producer := s.(*sink).producer
	producer.Close()
	broker.Close()

	mockProducer := mocks.NewSyncProducer(t, nil)
	s.(*sink).producer = mockProducer
	event := oplog.Event{
		Key:       "1234",
		Timestamp: time.Now(),
		Fields:    map[string]interface{}{"name": "zhangsan", "age": 30},
	}
	mockProducer.ExpectSendMessageAndSucceed()
	err = s.Publish(context.TODO(), event)
	require.Nil(t, err)
	s.Close()
}

type handler struct {
	recv  int
	tests int
	done  chan struct{}
}

func (h *handler) ConsumerEvent(ctx context.Context, e oplog.Event) {
	h.recv++
	if h.recv == h.tests {
		close(h.done)
	}
}

func TestConsumer(t *testing.T) {
	config := "{\"addrs\": \"127.0.0.1:9050\", \"topic\": \"cfa-unit-test\"}"
	os.WriteFile("/tmp/kafka.conf", []byte(config), 0o600)

	broker := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:9050")
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("cfa-unit-test", 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("cfa-unit-test", 0, sarama.OffsetOldest, 0).
			SetOffset("cfa-unit-test", 0, sarama.OffsetNewest, 10),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, consumerGroup, broker),
		"HeartbeatRequest": sarama.NewMockHeartbeatResponse(t),
		"JoinGroupRequest": sarama.NewMockSequence(
			sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.RangeBalanceStrategyName),
		),
		"SyncGroupRequest": sarama.NewMockSequence(
			sarama.NewMockSyncGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(
				&sarama.ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						"cfa-unit-test": {0},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(
			consumerGroup, "cfa-unit-test", 0, 0, "", sarama.ErrNoError,
		).SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("cfa-unit-test", 0, 0, sarama.ByteEncoder("{\"name\": \"zhangsan\", \"age\": 20}")),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("cfa-unit-test", 0, 1, sarama.ByteEncoder("{\"name\": \"lisi\", \"age\": 25}")),
		),
	})

	sink, err := NewKafkaSink("/tmp/kafka.conf")
	require.Nil(t, err)

	h := &handler{
		recv:  0,
		tests: 2,
		done:  make(chan struct{}),
	}

	sink.StartConsumer(h)
	<-h.done
	sink.Close()
	broker.Close()
}
