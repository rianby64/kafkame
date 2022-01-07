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

	RetryToConnect time.Duration
	ListenTimeout  time.Duration
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
	defer func() {
		if err := queue.reader.Close(); err != nil {
			queue.log.Println(err)
		}
	}()

	for {
		msg, err := queue.reader.ReadMessage(ctx)
		if err != nil {
			queue.log.Println(err)

			if errors.Is(err, context.Canceled) {
				return err
			}

			if err := queue.reader.Close(); err != nil {
				queue.log.Println(err)
			}

			time.Sleep(queue.RetryToConnect)
			queue.reader = queue.readerBuilder() // rebuild the connection

			continue
		}

		select {
		case queue.lastMsg <- msg.Value:
			queue.log.Println("message received")
		case <-time.After(queue.ListenTimeout):
			// possibly here I've a big bug... why? queue.lastMsg is not being cleared
			queue.log.Println("cannot receive")
		}
	}
}

func NewListener(readerBuilder func() Reader, log Logger) *Listener {
	l := &Listener{
		readerBuilder(),
		readerBuilder,
		make(chan []byte, 1),
		log,
		RetryToConnect,
		ListenTimeout,
	}

	return l
}
