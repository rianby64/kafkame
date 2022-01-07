package kafkame_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type LoggerMock struct {
	Msg []string
}

func (logger *LoggerMock) Println(msgs ...interface{}) {
	for _, v := range msgs {
		logger.Msg = append(logger.Msg, fmt.Sprintf("%v", v))
	}
}

// ReaderMock1 is being used at Test_case_one_message_no_errors_OK
type ReaderMock1 struct {
	msg string

	alreadySent bool
}

func (reader *ReaderMock1) Close() error {
	return nil
}

func (reader *ReaderMock1) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg := kafka.Message{
		Value: []byte(reader.msg),
	}

	if reader.alreadySent {
		time.Sleep(time.Hour) // (2) block any futher sending
	}

	reader.alreadySent = true

	return msg, nil // (1) - send the message
}

// ReaderMock2 is being used at Test_case_one_message_context_cancel_no_errors_OK
type ReaderMock2 struct {
	msg string

	Cancel      chan struct{}
	alreadySent bool
}

func (reader *ReaderMock2) Close() error {
	return nil
}

func (reader *ReaderMock2) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg := kafka.Message{
		Value: []byte(reader.msg),
	}

	if reader.alreadySent {
		<-ctx.Done() // (2) wait for canceling the context
		go func() {
			reader.Cancel <- struct{}{}
		}()

		return msg, context.Canceled // (3) ...
	}

	reader.alreadySent = true

	return msg, nil // (1) - send the message
}

// ReaderMock3 is being used at Test_case_one_message_one_error_reconnect_OK
type ReaderMock3 struct {
	msg string

	alreadyFail bool
	alreadySent bool
}

func (reader *ReaderMock3) Close() error {
	return nil
}

func (reader *ReaderMock3) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg := kafka.Message{
		Value: []byte(reader.msg),
	}

	if !reader.alreadyFail {
		// (1) - fail as we want to trigger the reconnect function
		return msg, errors.New("random error")
	}

	if reader.alreadySent {
		time.Sleep(time.Hour) // (3) - block any futher sending
	}

	reader.alreadySent = true

	return msg, nil // (2) - send the message
}

// ReaderMock4 is being used at Test_case_one_message_one_error_reconnect_OK
type ReaderMock4 struct {
	msg string
}

func (reader *ReaderMock4) Close() error {
	return nil
}

func (reader *ReaderMock4) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg := kafka.Message{
		Value: []byte(reader.msg),
	}

	return msg, nil // (1) - send the message
}
