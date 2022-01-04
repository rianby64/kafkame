package kafkame_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"kafkame/kafkame"
)

func Test_case_one_message_no_errors_OK(t *testing.T) {
	expectedMsg := "expected message"
	expectedLogs := "message received"
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
