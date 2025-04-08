// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package mqtt connects to the broker and listens for sensor telemetry.
package mqtt

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Client struct {
	client  mqtt.Client
	Broker  string // <- Changed to exported field
	handler func(string, []byte)
	opts    *mqtt.ClientOptions
}

func NewClient(broker string, messageHandler func(string, []byte)) *Client {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("iot-ingestor-" + uuid.New().String()[:8]).
		SetAutoReconnect(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetCleanSession(true).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Error().Err(err).Msg("MQTT connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			log.Info().Msg("MQTT connection established")
		})

	return &Client{
		client:  mqtt.NewClient(opts),
		Broker:  broker, // <- Capitalized field name
		handler: messageHandler,
		opts:    opts,
	}
}

// Connect establishes the MQTT connection with retry logic
func (c *Client) Connect() error {
	if token := c.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// Subscribe starts listening to sensor topics
func (c *Client) Subscribe() error {
	if token := c.client.Subscribe("sensors/+", 1, func(_ mqtt.Client, msg mqtt.Message) {
		c.handler(msg.Topic(), msg.Payload())
	}); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// Disconnect closes the MQTT connection gracefully
func (c *Client) Disconnect(quiesce uint) {
	c.client.Disconnect(quiesce)
	log.Info().Msg("MQTT client disconnected")
}
