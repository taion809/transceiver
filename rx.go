package transceiver

import (
	"context"
	"fmt"
	"strings"

	"time"

	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type Rx interface {
	Receive(context.Context) error
}

type RxHandler interface {
	Handle(context.Context, *RxMessage) error
}

type RxMessage struct {
	Topic string
	Body  []byte
}

type DefaultRx struct {
	Consumer     *kafka.Consumer
	Handler      RxHandler
	Logger       *zap.SugaredLogger
	ReceiverFunc func(context.Context) error
}

type Muxer struct {
	Receivers []Rx
}

const (
	defaultGroupID = "g1"

	// Default timeout in milliseconds
	defaultSessionTimeout = 6000
)

var (
	defaultClientID       = fmt.Sprintf("transciever-client-%d", time.Now().Nanosecond())
	defaultConsumerConfig = &kafka.ConfigMap{
		"client.id":                       defaultClientID,
		"group.id":                        defaultGroupID,
		"session.timeout.ms":              defaultSessionTimeout,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	}
)

func NewConsumer(servers []string, topics []string, additionalConfig *kafka.ConfigMap) (*kafka.Consumer, error) {
	if additionalConfig == nil {
		additionalConfig = defaultConsumerConfig
	}

	additionalConfig.SetKey("bootstrap.servers", strings.Join(servers, ","))

	c, err := kafka.NewConsumer(additionalConfig)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (r *DefaultRx) Receive(ctx context.Context) error {
	if r.ReceiverFunc == nil {
		return DefaultRxReceiverFunc(r)(ctx)
	}

	return r.ReceiverFunc(ctx)
}

func (m *Muxer) Register(receiver Rx) {
	m.Receivers = append(m.Receivers, receiver)
}

func (m *Muxer) Run(ctx context.Context) error {
	var wg = &sync.WaitGroup{}

	for _, receiver := range m.Receivers {
		wg.Add(1)

		go func(ctx context.Context, rx Rx) {
			rx.Receive(ctx)
			wg.Done()
		}(ctx, receiver)
	}

	wg.Wait()
	return nil
}

func DefaultRxReceiverFunc(receiver *DefaultRx) func(context.Context) error {
	return func(ctx context.Context) error {
		for {
			select {
			case ev := <-receiver.Consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					receiver.Logger.Debug(zap.Stringer("event", e))
					receiver.Consumer.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					receiver.Logger.Debug(zap.Stringer("event", e))
					receiver.Consumer.Unassign()
				case *kafka.Message:
					receiver.Logger.Debug(zap.String("msg", "received message"), zap.String("topic", *e.TopicPartition.Topic), zap.Stringer("partition", e.TopicPartition))
					if err := receiver.Handler.Handle(ctx, &RxMessage{Topic: *e.TopicPartition.Topic, Body: e.Value}); err != nil {
						return err
					}
				case kafka.PartitionEOF:
					receiver.Logger.Debug(zap.String("msg", "kafka partition reached EOF"), zap.Stringer("event", e))
				case kafka.Error:
					return e
				}
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					return err
				}

				return nil
			}

		}
	}
}
