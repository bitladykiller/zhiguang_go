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
	$(GO) test ./... -v -cover -coverprofile=coverage.out


# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out
	rm -rf .gocache/

# Lint with go vet
lint:
	$(GO) vet ./...

# Tidy Go module dependencies
mod:
	$(GO) mod tidy

# Download dependencies
deps:
	$(GO) mod download

# Start local dependency containers
dev-up:
	docker compose up -d

# Stop local dependency containers
dev-down:
	docker compose down

# Tail local dependency container logs
dev-logs:
	docker compose logs -f

# Initialize the MySQL schema inside the running mysql container
db-init:
	docker compose exec -T mysql mysql -uroot -proot123 zhiguang < db/schema.sql

# Generate local RS256 JWT keys used by config/config-local.yaml
gen-jwt-keys:
	mkdir -p config/keys
	openssl genrsa -out config/keys/private.pem 2048
	openssl rsa -in config/keys/private.pem -pubout -out config/keys/public.pem

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
	@echo "  dev-up  - Start MySQL/Redis/Kafka/Elasticsearch via Docker Compose"
	@echo "  dev-down - Stop local dependency containers"
	@echo "  dev-logs - Tail Docker Compose logs"
	@echo "  db-init - Initialize the MySQL schema inside Docker"
	@echo "  gen-jwt-keys - Generate local RSA keys for JWT signing"
