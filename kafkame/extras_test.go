package kafkame_test

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type LoggerMock struct {
	Msg string
}

func (logger *LoggerMock) Println(msgs ...interface{}) {
	for _, v := range msgs {
		logger.Msg += fmt.Sprintf("%v", v)
	}
}

// ReaderMock1 is being used at Test_case_one_message_no_errors_OK
type ReaderMock1 struct {
	msg string
}

func (reader *ReaderMock1) Close() error {
	return nil
}

func (reader *ReaderMock1) ReadMessage(ctx context.Context) (kafka.Message, error) {
	time.Sleep(time.Duration(1) * time.Second)

	return kafka.Message{
		Value: []byte(reader.msg),
	}, nil
}
