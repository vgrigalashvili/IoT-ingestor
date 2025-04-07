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
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/vgrigalashvili/IoT-ingestor/internal/db"
	"github.com/vgrigalashvili/IoT-ingestor/internal/rabbitmq"
	redisutil "github.com/vgrigalashvili/IoT-ingestor/internal/redis"
)

// TelemetryPayload represents normalized sensor data from MQTT.
type TelemetryPayload struct {
	DeviceID    string    `json:"device_id"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
}

// Handle processes a single MQTT message payload: decode, deduplicate, insert, publish.
func Handle(ctx context.Context, q *db.Queries, rdb *redis.Client, ch *amqp091.Channel, raw []byte) {
	var data TelemetryPayload
	if err := json.Unmarshal(raw, &data); err != nil {
		log.Error().Err(err).Msg("Failed to parse telemetry")
		return
	}

	deviceUUID, err := uuid.Parse(data.DeviceID)
	if err != nil {
		log.Error().Err(err).Msg("Invalid UUID")
		return
	}

	dup, err := redisutil.IsDuplicate(ctx, rdb, data.DeviceID, data.Timestamp.Unix())
	if err != nil {
		log.Error().Err(err).Msg("Redis error")
		return
	}
	if dup {
		log.Info().Str("device", data.DeviceID).Msg("Duplicate payload skipped")
		return
	}

	_, err = q.InsertSensorData(ctx, db.InsertSensorDataParams{
		DeviceID:    toPgUUID(deviceUUID),
		Timestamp:   toPgTimestamp(data.Timestamp),
		Temperature: toPgFloat64(data.Temperature),
		Humidity:    toPgFloat64(data.Humidity),
	})
	if err != nil {
		log.Error().Err(err).Msg("DB insert failed")
		return
	}

	if err := rabbitmq.Publish(ch, "sensor_exchange", data); err != nil {
		log.Error().Err(err).Msg("Publish to RabbitMQ failed")
		return
	}

	log.Info().Str("device", data.DeviceID).Msg("Telemetry processed successfully")
}

// toPgUUID converts uuid.UUID to pgtype.UUID
func toPgUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{
		Bytes: u,
		Valid: true,
	}
}

// toPgTimestamp converts time.Time to pgtype.Timestamptz
func toPgTimestamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{
		Time:  t,
		Valid: true,
	}
}

// toPgFloat64 converts float64 to pgtype.Float8
func toPgFloat64(f float64) pgtype.Float8 {
	return pgtype.Float8{
		Float64: f,
		Valid:   true,
	}
}
