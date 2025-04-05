-- name: InsertSensorData :one
INSERT INTO sensor_data (device_id, timestamp, temperature, humidity)
VALUES ($1, $2, $3, $4)
RETURNING id;
