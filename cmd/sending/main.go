package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"

	"kafkame/kafkame"
)

type Config struct {
	NAME          string   `env:"NAME"`
	KAFKA_USER    string   `env:"KAFKA_USER"`
	KAFKA_PASS    string   `env:"KAFKA_PASS"`
	KAFKA_TOPIC   string   `env:"KAFKA_TOPIC,required"`
	KAFKA_BROKERS []string `env:"KAFKA_BROKERS,required"`
}

func main() {
	cfg := loadConfig()

	publisher := kafkame.NewPublisher(
		func() kafkame.Writer {
			return kafkame.NewWriter(cfg.KAFKA_USER,
				cfg.KAFKA_PASS,
				cfg.KAFKA_TOPIC,
				cfg.KAFKA_BROKERS)
		},
		log.Default(),
	)

	i := 0
	for {
		time.Sleep(time.Second * time.Duration(5))
		payload := map[string]string{
			"msg": "this is what I want to publish",
			"i":   fmt.Sprint(i),
		}

		if err := publisher.Publish(context.Background(), payload); err != nil {
			log.Println(err)

			continue
		}

		log.Println("sent...")
		i++
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
