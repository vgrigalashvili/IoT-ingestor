CREATE TABLE sensor_data (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);
