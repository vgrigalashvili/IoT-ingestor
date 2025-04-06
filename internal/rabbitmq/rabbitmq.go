// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package rabbitmq provides publishing capabilities for fanout analytics.
package rabbitmq

import (
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

// Init connects to RabbitMQ and declares the sensor exchange.
func Init(url string) (*amqp091.Connection, *amqp091.Channel, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	err = ch.ExchangeDeclare("sensor_exchange", "fanout", true, false, false, false, nil)
	return conn, ch, err
}

// Publish sends telemetry data to the fanout exchange.
func Publish(ch *amqp091.Channel, exchange string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return ch.Publish(exchange, "", false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}
