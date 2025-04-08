// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/vgrigalashvili/IoT-ingestor/internal/db"
	"github.com/vgrigalashvili/IoT-ingestor/internal/logger"
	"github.com/vgrigalashvili/IoT-ingestor/internal/mqtt"
	"github.com/vgrigalashvili/IoT-ingestor/internal/processor"
	"github.com/vgrigalashvili/IoT-ingestor/internal/rabbitmq"
	redisutil "github.com/vgrigalashvili/IoT-ingestor/internal/redis"
)

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Missing required environment variable: %s", key)
	}
	return v
}

func main() {
	_ = godotenv.Load(".env")
	log := logger.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize PostgreSQL
	pgUrl := "postgres://" + mustGetEnv("POSTGRES_USER") + ":" +
		mustGetEnv("POSTGRES_PASSWORD") + "@" +
		mustGetEnv("POSTGRES_HOST") + ":" +
		mustGetEnv("POSTGRES_PORT") + "/" +
		mustGetEnv("POSTGRES_DB")

	dbPool, err := pgxpool.New(ctx, pgUrl)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to PostgreSQL")
	}
	defer dbPool.Close()

	// Create database queries instance
	queries := db.New(dbPool)

	// Initialize Redis
	rdb := redisutil.Init(mustGetEnv("REDIS_ADDR"))
	defer rdb.Close()

	// Initialize RabbitMQ with persistent connection
	rabbitClient, err := rabbitmq.NewClient(
		"amqp://"+mustGetEnv("RABBITMQ_USER")+":"+mustGetEnv("RABBITMQ_PASS")+"@"+
			mustGetEnv("RABBITMQ_HOST")+":"+mustGetEnv("RABBITMQ_PORT"),
		"sensor_exchange",
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RabbitMQ client")
	}
	defer rabbitClient.Close()

	// Initialize MQTT with message processing
	mqttClient := mqtt.NewClient(
		mustGetEnv("MQTT_BROKER"),
		func(topic string, payload []byte) {
			start := time.Now()
			err := processor.Handle(ctx, queries, rdb, rabbitClient, payload)

			log.Debug().
				Str("topic", topic).
				Dur("duration", time.Since(start)).
				Err(err).
				Msg("Message processed")
		},
	)

	// Start MQTT with backoff
	go func() {
		retryDelay := time.Second
		for {
			log.Info().Str("broker", mqttClient.Broker).Msg("Attempting MQTT connection")

			if err := mqttClient.Connect(); err != nil {
				log.Error().
					Err(err).
					Str("broker", mqttClient.Broker).
					Dur("retry_delay", retryDelay).
					Msg("MQTT connection failed")

				time.Sleep(retryDelay)
				retryDelay = time.Duration(math.Min(float64(retryDelay*2), float64(time.Minute)))
				continue
			}

			if err := mqttClient.Subscribe(); err != nil {
				log.Error().Err(err).Msg("MQTT subscription failed")
				continue
			}

			log.Info().Msg("MQTT connection established successfully")
			break
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Info().Msg("IoT ingestor started successfully")

	select {
	case <-sigChan:
		log.Info().Msg("Shutting down gracefully...")
		cancel()
		mqttClient.Disconnect(250)
	case <-ctx.Done():
	}
}
