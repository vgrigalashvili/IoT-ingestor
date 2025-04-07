// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package mqtt connects to the broker and listens for sensor telemetry.
package mqtt

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

// Start connects to an MQTT broker and subscribes to all sensor topics.
func Start(broker string, onMessage func(topic string, payload []byte)) {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("iot-ingestor")

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal().Err(token.Error()).Msg("MQTT connection failed")
	}

	if token := client.Subscribe("sensors/+", 1, func(_ mqtt.Client, msg mqtt.Message) {
		onMessage(msg.Topic(), msg.Payload())
	}); token.Wait() && token.Error() != nil {
		log.Fatal().Err(token.Error()).Msg("MQTT subscription failed")
	}

	log.Info().Msg("MQTT connected and subscribed")
}
