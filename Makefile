.PHONY: build run test clean lint mod deps help dev-up dev-down dev-logs db-init gen-jwt-keys

GOCACHE ?= $(CURDIR)/.gocache
GO := env GOCACHE=$(GOCACHE) go

BINARY_NAME=bin/zhiguang-server
CONFIG ?= config/config-local.yaml

# Build the application binary
build:
	$(GO) build -o $(BINARY_NAME) ./cmd/server

# Run the application
run:
	$(GO) run ./cmd/server -config $(CONFIG)

# Run tests with coverage
test:
	$(GO) test ./... -v -cover -coverprofile=coverage.out -timeout 120s

# Run unit tests only (packages without external dependencies)
test-unit:
	$(GO) test ./pkg/... ./internal/server/... ./internal/auth/... ./internal/counter/... ./internal/fanout/... ./internal/knowpost/... ./internal/outbox/... ./internal/profile/... ./internal/relation/... ./internal/search/... ./internal/storage/... ./internal/cache/... ./internal/canal/... -count=1 -timeout 60s

# Run short tests (fast, no external dependencies)
test-short:
	$(GO) test ./... -short -count=1 -timeout 60s

# Show test coverage in browser
test-cover:
	$(GO) tool cover -html=coverage.out


# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out
	rm -rf .gocache/

# Lint with golangci-lint
lint:
	golangci-lint run ./...

# Tidy Go module dependencies
mod:
	$(GO) mod tidy

# Download dependencies
deps:
	$(GO) mod download

# Start the full local stack
dev-up:
	docker compose up -d

# Stop the full local stack
dev-down:
	docker compose down

# Tail local stack logs
dev-logs:
	docker compose logs -f

# Initialize the MySQL schema inside the running mysql container
db-init:
	docker compose exec -T mysql mysql -uroot -proot123 zhiguang < db/schema.sql

# Generate local RS256 JWT keys used by config/config-local.yaml
gen-jwt-keys:
	mkdir -p config/keys
	$(GO) run ./cmd/gen-jwt-keys -private config/keys/private.pem -public config/keys/public.pem

# Show help
help:
	@echo "Zhiguang Go Server — Available targets:"
	@echo "  build   - Build the application binary"
	@echo "  run     - Run the application with CONFIG=$(CONFIG)"
	@echo "  test    - Run all tests with coverage"
	@echo "  clean   - Remove build artifacts"
	@echo "  lint    - Run go vet"
	@echo "  mod     - Tidy Go modules"
	@echo "  deps    - Download dependencies"
	@echo "  dev-up  - Start the full Docker Compose stack"
	@echo "  dev-down - Stop the full Docker Compose stack"
	@echo "  dev-logs - Tail full Docker Compose logs"
	@echo "  db-init - Initialize the MySQL schema inside Docker"
	@echo "  gen-jwt-keys - Generate local RSA keys for JWT signing"
