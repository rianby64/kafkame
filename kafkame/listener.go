package kafkame

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	ErrorListenerTimeout = errors.New("listener timeout")
)

type Listener struct {
	reader        Reader
	readerBuilder func() Reader

	lastMsg chan []byte
	log     Logger

	RetryToConnect time.Duration
	ListenTimeout  time.Duration

	processDroppedMsg func(msg *kafka.Message, log Logger) error
	close             chan struct{}
	closed            bool
}

func (queue *Listener) GetMsg() <-chan []byte {
	return queue.lastMsg
}

func (queue *Listener) Close() error {
	defer func() {
		if !queue.closed {
			queue.close <- struct{}{}
		}

		queue.closed = true
	}()

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
		case <-queue.close:
			queue.log.Println("listener closed")

			return nil
		case <-time.After(queue.ListenTimeout):
			// the strategy here to apply is:
			// - log this issue,
			// - exit the loop in order to stop receiving new messages,
			// - perhaps I've to send back the message to the queue, but
			//   this looks more like an indirect (unexpected) behaviour.
			err := queue.processDroppedMsg(&msg, queue.log)

			if err != nil {
				return err
			}
		}
	}
}

func defaultProcessDroppedMsg(_ *kafka.Message, log Logger) error {
	log.Println("message dropped")

	return ErrorListenerTimeout
}

func NewListener(readerBuilder func() Reader, processDroppedMsg func(msg *kafka.Message, log Logger) error, log Logger) *Listener {
	pdm := defaultProcessDroppedMsg
	if processDroppedMsg != nil {
		pdm = processDroppedMsg
	}

	l := &Listener{
		readerBuilder(),
		readerBuilder,
		make(chan []byte), // I need this unbuffered because I want to lost only one message
		log,
		RetryToConnect,
		ListenTimeout,
		pdm,
		make(chan struct{}, 1),
		false,
	}

	return l
}
