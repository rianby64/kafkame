package main

import (
	"context"
	"fmt"
	"log"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"

	"kafkame/kafkame"
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

	signaler := kafkame.NewListener(
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
		if err := signaler.Listen(context.Background()); err != nil {
			log.Fatalln(err)
		}
	}()

	for {
		lastMsg := <-signaler.LastMsg()
		fmt.Println(string(lastMsg))
	}
}

func loadConfig() Config {
	godotenv.Load()
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("%+v\n", err)
	}

	return cfg
}
