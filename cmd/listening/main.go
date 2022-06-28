package main

import (
	"context"
	"log"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/rianby64/kafkame"
)

type Config struct {
	NAME          string   `env:"NAME" envDefault:"name"`
	KAFKA_USER    string   `env:"KAFKA_USER"`
	KAFKA_PASS    string   `env:"KAFKA_PASS"`
	KAFKA_TOPIC   string   `env:"KAFKA_TOPIC,required"`
	KAFKA_BROKERS []string `env:"KAFKA_BROKERS,required"`
}

func main() {
	cfg := loadConfig()

	consumer := kafkame.NewListener(
		func() kafkame.Reader {
			return kafkame.NewReader(cfg.KAFKA_USER,
				cfg.KAFKA_PASS,
				cfg.NAME,
				cfg.KAFKA_TOPIC,
				cfg.KAFKA_BROKERS)
		},
		nil,
		log.Default(),
	)

	go func() {
		if err := consumer.Listen(context.Background()); err != nil {
			log.Fatalln(err)
		}
	}()

	for {
		msg := <-consumer.Msg()
		log.Println(string(msg))
	}
}

func loadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("%+v\n", err)
	}

	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("%+v\n", err)
	}

	return cfg
}
