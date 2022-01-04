package kafkame

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

type Listener struct {
	reader        Reader
	readerBuilder func() Reader

	lastMsg chan []byte
	log     Logger
}

func (queue *Listener) GetMsg() <-chan []byte {
	return queue.lastMsg
}

func (queue *Listener) Close() error {
	return queue.reader.Close()
}

type Reader interface {
	Close() error
	ReadMessage(ctx context.Context) (kafka.Message, error)
}

func (queue *Listener) Listen(ctx context.Context) error {
	for {
		msg, err := queue.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}

			if err := queue.reader.Close(); err != nil {
				queue.log.Println(err)
			}

			time.Sleep(retryToConnect)
			queue.reader = queue.readerBuilder() // rebuild the connection

			continue
		}

		select {
		case queue.lastMsg <- msg.Value:
			queue.log.Println("message received")
		case <-time.After(listenTimeout):
			queue.log.Println("cannot receive")
		}

	}

	if err := queue.reader.Close(); err != nil {
		return err
	}

	return nil
}

func NewListener(readerBuilder func() Reader, log Logger) *Listener {
	l := &Listener{
		readerBuilder(),
		readerBuilder,
		make(chan []byte, 1),
		log,
	}

	return l
}
