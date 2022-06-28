package kafkame_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rianby64/kafkame"
)

func Test_case_one_message_no_errors_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := []string{"message received"}
	log := &LoggerMock{}
	opts := kafkame.NewOptions()
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock1{
			msg: expectedMsg,
		}
	}, log, opts)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	lastMsg := listener.Msg()

	go func() {
		if err := listener.Listen(ctx); err != nil {
			t.Log(err)
		}
	}()

	msg := <-lastMsg

	assert.Equal(t, string(msg), expectedMsg)
	assert.Equal(t, log.Msg, expectedLogs)
}

func Test_case_one_message_context_cancel_no_errors_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := []string{"message received", "context canceled"}
	log := &LoggerMock{}
	cancelChan := make(chan struct{}, 1)
	opts := kafkame.NewOptions()
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock2{
			msg:    expectedMsg,
			Cancel: cancelChan,
		}
	}, log, opts)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	lastMsg := listener.Msg()

	go func() {
		if err := listener.Listen(ctx); err != nil {
			t.Log(err)
		}
	}()
	go func() {
		time.Sleep(time.Millisecond * 10)
		done()
	}()

	msg := <-lastMsg
	<-cancelChan

	assert.Equal(t, expectedMsg, string(msg))
	assert.Equal(t, expectedLogs, log.Msg)
}

func Test_case_one_message_one_error_reconnect_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := []string{"random error", "message received"}
	log := &LoggerMock{}
	reconnectChan := make(chan struct{}, 1)
	alreadyFail := false

	opts := kafkame.NewOptions().WithRetryToConnect(0)
	listener := kafkame.NewListener(func() kafkame.Reader {
		if alreadyFail {
			reconnectChan <- struct{}{}
		}

		defer func() {
			alreadyFail = true
		}()

		return &ReaderMock3{
			msg:         expectedMsg,
			alreadyFail: alreadyFail,
		}
	}, log, opts)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	lastMsg := listener.Msg()

	go func() {
		if err := listener.Listen(ctx); err != nil {
			t.Log(err)
		}
	}()

	msg := <-lastMsg
	<-reconnectChan

	assert.Equal(t, expectedMsg, string(msg))
	assert.Equal(t, expectedLogs, log.Msg)
}

func Test_case_one_message_dropped_OK(t *testing.T) {
	expectedLogs := []string{"message dropped"}
	log := &LoggerMock{}
	errListener := make(chan error, 1)
	opts := kafkame.NewOptions().WithListenTimeout(time.Millisecond * time.Duration(100))
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock4{}
	}, log, opts)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	go func() {
		errListener <- listener.Listen(ctx)
	}()

	assert.Equal(t, <-errListener, kafkame.ErrorListenerTimeout)
	assert.Equal(t, log.Msg, expectedLogs)
}

func Test_case_close_OK(t *testing.T) {
	expectedLogs := []string{"listener closed"}
	log := &LoggerMock{}
	errListener := make(chan error, 1)
	opts := kafkame.NewOptions().WithListenTimeout(time.Second * time.Duration(5))
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock5{}
	}, log, opts)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	go func() {
		errListener <- listener.Listen(ctx)
	}()

	// time.Sleep(time.Second)
	if err := listener.Shutdown(); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, <-errListener, nil)
	assert.Equal(t, log.Msg, expectedLogs)
}
