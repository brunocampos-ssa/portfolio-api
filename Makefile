.PHONY: run test build db-up db-down db-reset clean watcher snapshot

# Run the API server
run:
	go run ./cmd/api

# Run the event watcher
watcher:
	go run ./cmd/event-watcher

# Run the snapshot runner
snapshot:
	go run ./cmd/snapshot-runner

# Run all tests
test:
	go test ./... -v -count=1

# Build all binaries
build:
	go build -o bin/portfolio-api ./cmd/api
	go build -o bin/event-watcher ./cmd/event-watcher
	go build -o bin/snapshot-runner ./cmd/snapshot-runner

# Start PostgreSQL via Docker Compose
db-up:
	docker compose up -d postgres

# Stop PostgreSQL
db-down:
	docker compose down

# Reset database (destroy volume and recreate)
db-reset:
	docker compose down -v
	docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@echo "Database reset complete. Migrations ran automatically."

# Remove build artifacts
clean:
	rm -rf bin/
