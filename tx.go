package transceiver

import (
	"context"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type Tx interface {
	Send(context.Context, []byte) error
}

type DefaultTx struct {
	Producer *kafka.Producer
	Topic    string
	Logger   *zap.SugaredLogger
	Confirm  func(context.Context) error
}

type TxMessage struct {
	Body           []byte
	TopicPartition *kafka.TopicPartition
}

var DefaultTopicPartition = kafka.TopicPartition{Partition: kafka.PartitionAny}

func NewDefaultTx(servers []string, topic string, logger *zap.SugaredLogger, additionalConfig *kafka.ConfigMap) (*DefaultTx, error) {
	if additionalConfig == nil {
		additionalConfig = &kafka.ConfigMap{}
	}

	additionalConfig.SetKey("bootstrap.servers", strings.Join(servers, ","))

	producer, err := kafka.NewProducer(additionalConfig)
	if err != nil {
		return nil, err
	}

	return &DefaultTx{Producer: producer, Topic: topic, Confirm: DefaultTxConfirm(producer, logger)}, nil
}

func (t *DefaultTx) Close() {
	if t == nil || t.Producer == nil {
		return
	}

	t.Producer.Close()
}

func (t *DefaultTx) Send(ctx context.Context, msg *TxMessage) error {
	tp := msg.TopicPartition
	if tp == nil {
		tp = &DefaultTopicPartition
		tp.Topic = &t.Topic
	}

	message := &kafka.Message{
		Value:          msg.Body,
		TopicPartition: *tp,
	}

	select {
	case t.Producer.ProduceChannel() <- message:
		return t.Confirm(ctx)
	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return err
		}

		return nil
	}
}

func DefaultTxConfirm(producer *kafka.Producer, logger *zap.SugaredLogger) func(context.Context) error {
	return func(ctx context.Context) error {
		for {
			select {
			case ev := <-producer.Events():
				switch event := ev.(type) {
				case *kafka.Error:
					return event
				case *kafka.Message:
					if event.TopicPartition.Error != nil {
						return event.TopicPartition.Error
					}

					return nil
				default:
					logger.Info(zap.Stringer("event", ev))
				}
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					return err
				}
			}
		}
	}
}
