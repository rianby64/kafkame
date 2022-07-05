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

type Options struct {
	retryToConnect    time.Duration
	listenTimeout     time.Duration
	processDroppedMsg func(msg *kafka.Message, log Logger) error
}

func (opts *Options) WithRetryToConnect(retryToConnect time.Duration) *Options {
	opts.retryToConnect = retryToConnect

	return opts
}

func (opts *Options) WithListenTimeout(listenTimeout time.Duration) *Options {
	opts.listenTimeout = listenTimeout

	return opts
}

func (opts *Options) WithProcessDroppedMsg(processDroppedMsg func(msg *kafka.Message, log Logger) error) *Options {
	pdm := defaultProcessDroppedMsg
	if processDroppedMsg != nil {
		pdm = processDroppedMsg
	}

	opts.processDroppedMsg = pdm

	return opts
}

type Listener struct {
	readerBuilder func() Reader
	log           Logger

	retryToConnect    time.Duration
	listenTimeout     time.Duration
	processDroppedMsg func(msg *kafka.Message, log Logger) error

	reader  Reader
	lastMsg chan []byte
	cancel  context.CancelFunc

	lockClosed *sync.Mutex
	closed     bool
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

			time.Sleep(queue.retryToConnect)
			queue.reader = queue.readerBuilder() // rebuild the connection

			continue
		}

		select {
		case <-readCtx.Done():
			queue.log.Println("listener closed")

			return nil

		case queue.lastMsg <- msg.Value:
			queue.log.Println("message received")

		case <-time.After(queue.listenTimeout):
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

func NewListener(readerBuilder func() Reader, log Logger, opts ...*Options) *Listener {
	finalOpts := &Options{
		processDroppedMsg: defaultProcessDroppedMsg,
		listenTimeout:     ListenTimeout,
		retryToConnect:    RetryToConnect,
	}

	for _, opt := range opts {
		if opt.listenTimeout != 0 {
			finalOpts.listenTimeout = opt.listenTimeout
		}

		if opt.retryToConnect != 0 {
			finalOpts.retryToConnect = opt.retryToConnect
		}

		if opt.processDroppedMsg != nil {
			finalOpts.processDroppedMsg = opt.processDroppedMsg
		}
	}

	return &Listener{
		reader:            readerBuilder(),
		readerBuilder:     readerBuilder,
		lastMsg:           make(chan []byte),
		log:               log,
		retryToConnect:    finalOpts.retryToConnect,
		listenTimeout:     finalOpts.listenTimeout,
		processDroppedMsg: finalOpts.processDroppedMsg,
		lockClosed:        &sync.Mutex{},
	}
}
