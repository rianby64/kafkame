package kafkame

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Publisher struct {
	writer        Writer
	writerBuilder func() Writer
	log           Logger
}

func (queue *Publisher) Publish(ctx context.Context, payloads ...interface{}) error {
	queue.writer = queue.writerBuilder()
	messages := make([]kafka.Message, len(payloads))

	for i, payload := range payloads {
		bytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		messages[i] = kafka.Message{
			Value: bytes,
		}
	}

	defer func() {
		if err := queue.writer.Close(); err != nil {
			queue.log.Println(err, "at publish")
		}
	}()

	return queue.writer.WriteMessages(ctx, messages...)
}

func NewPublisher(writerBuilder func() Writer, log Logger) *Publisher {
	return &Publisher{
		writerBuilder: writerBuilder,
		log:           log,
	}
}
