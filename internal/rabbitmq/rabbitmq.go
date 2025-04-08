// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package rabbitmq provides publishing capabilities for fanout analytics.
package rabbitmq

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn         *amqp091.Connection
	channel      *amqp091.Channel
	exchangeName string
	url          string
}

// NewClient creates a new persistent RabbitMQ client
func NewClient(url, exchange string) (*RabbitMQClient, error) {
	client := &RabbitMQClient{
		exchangeName: exchange,
		url:          url,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	go client.handleReconnect()
	return client, nil
}

func (c *RabbitMQClient) connect() error {
	var err error
	c.conn, err = amqp091.Dial(c.url)
	if err != nil {
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}

	return c.channel.ExchangeDeclare(
		c.exchangeName, // name
		"fanout",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
}

func (c *RabbitMQClient) handleReconnect() {
	for {
		reason := <-c.conn.NotifyClose(make(chan *amqp091.Error))
		log.Printf("Connection closed: %v", reason)

		// Exponential backoff
		retryWait := 1 * time.Second
		for {
			time.Sleep(retryWait)

			err := c.connect()
			if err == nil {
				log.Println("Successfully reconnected")
				return
			}

			retryWait *= 2
			if retryWait > 30*time.Second {
				retryWait = 30 * time.Second
			}
			log.Printf("Failed to reconnect: %v. Retrying in %v", err, retryWait)
		}
	}
}

// Publish safely handles message publishing with retries
func (c *RabbitMQClient) Publish(payload any) error {
	const maxRetries = 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if c.channel.IsClosed() {
			if err := c.connect(); err != nil {
				lastErr = err
				continue
			}
		}

		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		err = c.channel.Publish(
			c.exchangeName, // exchange
			"",             // routing key
			false,          // mandatory
			false,          // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        body,
				Timestamp:   time.Now(),
			},
		)

		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}

	return errors.New("failed to publish after retries: " + lastErr.Error())
}

// Close cleans up resources properly
func (c *RabbitMQClient) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
