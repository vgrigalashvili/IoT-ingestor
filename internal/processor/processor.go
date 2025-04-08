// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package processor handles end-to-end processing: parse, dedup, insert, and publish.
package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/vgrigalashvili/IoT-ingestor/internal/db"
	"github.com/vgrigalashvili/IoT-ingestor/internal/rabbitmq"
	redisutil "github.com/vgrigalashvili/IoT-ingestor/internal/redis"
)

// TelemetryPayload represents normalized sensor data from MQTT
type TelemetryPayload struct {
	DeviceID    string    `json:"device_id"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
}

// Handle processes a single MQTT message payload through the full pipeline
func Handle(
	ctx context.Context,
	q *db.Queries,
	rdb *redis.Client,
	rmq *rabbitmq.RabbitMQClient,
	raw []byte,
) error {
	startTime := time.Now()
	logger := log.With().Str("component", "processor").Logger()

	defer func() {
		logger.Debug().
			Dur("duration_ms", time.Since(startTime)).
			Msg("Processing completed")
	}()

	var data TelemetryPayload
	if err := json.Unmarshal(raw, &data); err != nil {
		logger.Error().Err(err).Msg("Failed to parse telemetry payload")
		return err
	}

	// Validate device ID
	deviceUUID, err := uuid.Parse(data.DeviceID)
	if err != nil {
		logger.Error().Err(err).Str("device_id", data.DeviceID).Msg("Invalid device UUID")
		return err
	}

	// Deduplication check
	dup, err := redisutil.IsDuplicate(ctx, rdb, data.DeviceID, data.Timestamp.Unix())
	if err != nil {
		logger.Error().Err(err).Msg("Redis deduplication check failed")
		return err
	}
	if dup {
		logger.Info().Str("device_id", data.DeviceID).Msg("Duplicate payload skipped")
		return nil
	}

	// Database insertion
	_, err = q.InsertSensorData(ctx, db.InsertSensorDataParams{
		DeviceID:    toPgUUID(deviceUUID),
		Timestamp:   toPgTimestamp(data.Timestamp),
		Temperature: toPgFloat64(data.Temperature),
		Humidity:    toPgFloat64(data.Humidity),
	})
	if err != nil {
		logger.Error().Err(err).Msg("Database insertion failed")
		return err
	}

	// Publish to RabbitMQ
	if err := rmq.Publish(data); err != nil {
		logger.Error().Err(err).Msg("Failed to publish to RabbitMQ")
		return err
	}

	logger.Info().
		Str("device_id", data.DeviceID).
		Float64("temperature", data.Temperature).
		Float64("humidity", data.Humidity).
		Msg("Telemetry processed successfully")

	return nil
}

// Conversion helpers for PostgreSQL types
func toPgUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

func toPgTimestamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func toPgFloat64(f float64) pgtype.Float8 {
	return pgtype.Float8{Float64: f, Valid: true}
}
