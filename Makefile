# Load .env before running these:
# export $(cat .env | xargs)

run:
	go run ./cmd/ingestor/main.go

generate:
	sqlc generate

dev-up:
	docker compose up -d

dev-down:
	docker-compose down

build:
	go build -o bin/ingestor ./cmd/ingestor

migrate-up:
	migrate -path db/migrations -database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable" up

migrate-down:
	migrate -path db/migrations -database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable" down

migrate-force:
	migrate -path db/migrations -database "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable" force
