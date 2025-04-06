// Copyright (c) 2025 Vladimer Grigalashvili
// SPDX-License-Identifier: MIT

// Package redis provides deduplication using Redis cache.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Init sets up a Redis client from a given address (e.g., "localhost:6379").
func Init(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

// IsDuplicate returns true if the given reading was already seen (within TTL).
func IsDuplicate(ctx context.Context, client *redis.Client, deviceID string, ts int64) (bool, error) {
	key := fmt.Sprintf("dedup:%s:%d", deviceID, ts)
	ok, err := client.SetNX(ctx, key, 1, 60*time.Second).Result()
	return !ok, err // not OK → already exists → duplicate
}
