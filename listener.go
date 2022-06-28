package kafkame

import (
	"context"
	"errors"
	"sync"
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
	lockClosed        *sync.Mutex
	closed            bool
	cancel            context.CancelFunc
}

func (queue *Listener) Msg() <-chan []byte {
	return queue.lastMsg
}

func (queue *Listener) Shutdown() error {
	queue.lockClosed.Lock()
	defer queue.lockClosed.Unlock()

	if queue.cancel != nil {
		queue.cancel()
	}

	if !queue.closed {
		queue.closed = true
		close(queue.lastMsg)
	}

	return queue.reader.Close()
}

type Reader interface {
	Close() error
	ReadMessage(ctx context.Context) (kafka.Message, error)
}

func (queue *Listener) Listen(ctx context.Context) error {
	readCtx, cancel := context.WithCancel(ctx)
	queue.lockClosed.Lock()
	queue.cancel = cancel
	queue.lockClosed.Unlock()

	defer func() {
		if err := queue.Shutdown(); err != nil {
			queue.log.Println(err)
		}
	}()

	for {
		if queue.closed {
			queue.log.Println("listener closed")

			return nil
		}

		msg, err := queue.reader.ReadMessage(readCtx)
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
		case <-readCtx.Done():
			queue.log.Println("listener closed")

			return nil

		case queue.lastMsg <- msg.Value:
			queue.log.Println("message received")

		case <-time.After(queue.ListenTimeout):
			if err := queue.processDroppedMsg(&msg, queue.log); err != nil {
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

	return &Listener{
		reader:            readerBuilder(),
		readerBuilder:     readerBuilder,
		lastMsg:           make(chan []byte),
		log:               log,
		RetryToConnect:    RetryToConnect,
		ListenTimeout:     ListenTimeout,
		processDroppedMsg: pdm,
		lockClosed:        &sync.Mutex{},
	}
}

// TODO: Would be nice to have the constructor with the options pattern.
// https://www.sohamkamani.com/golang/options-pattern/
