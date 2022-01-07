package kafkame_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"kafkame/kafkame"
)

func Test_case_one_message_no_errors_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := []string{"message received"}
	log := &LoggerMock{}
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock1{
			msg: expectedMsg,
		}
	}, log)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	lastMsg := listener.GetMsg()

	go listener.Listen(ctx)

	msg := <-lastMsg

	assert.Equal(t, string(msg), expectedMsg)
	assert.Equal(t, log.Msg, expectedLogs)
}

func Test_case_one_message_context_cancel_no_errors_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := []string{"message received", "context canceled"}
	log := &LoggerMock{}
	cancelChan := make(chan struct{}, 1)
	listener := kafkame.NewListener(func() kafkame.Reader {
		return &ReaderMock2{
			msg:    expectedMsg,
			Cancel: cancelChan,
		}
	}, log)

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	lastMsg := listener.GetMsg()

	go listener.Listen(ctx)
	go done()

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
	}, log)
	listener.RetryToConnect = 0

	assert.NotNil(t, listener)

	ctx, done := context.WithCancel(context.TODO())
	defer done()

	lastMsg := listener.GetMsg()

	go listener.Listen(ctx)

	msg := <-lastMsg
	<-reconnectChan

	assert.Equal(t, expectedMsg, string(msg))
	assert.Equal(t, expectedLogs, log.Msg)
}
