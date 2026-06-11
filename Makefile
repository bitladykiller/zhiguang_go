.PHONY: build run fmt vet lint test check clean mod deps help dev-up dev-down dev-logs db-init gen-jwt-keys

GOCACHE ?= $(CURDIR)/.gocache
GO := env GOCACHE=$(GOCACHE) go
GOLANGCI_LINT_CACHE ?= $(CURDIR)/.golangci-cache
GOLANGCI_LINT := env GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) golangci-lint

BINARY_NAME=bin/zhiguang-server
CONFIG ?= config/config-local.yaml

# Build the application binary
build:
	$(GO) build -o $(BINARY_NAME) ./cmd/server

# Run the application
run:
	$(GO) run ./cmd/server -config $(CONFIG)

# Format all Go source files in-place
fmt:
	find . -name '*.go' -not -path './vendor/*' -print0 | xargs -0 gofmt -w

# Run go vet checks
vet:
	$(GO) vet ./...

# Run golangci-lint when available, otherwise fall back to go vet
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		$(GOLANGCI_LINT) run ./...; \
	else \
		echo "golangci-lint not found, falling back to go vet"; \
		$(GO) vet ./...; \
	fi

# Run tests with coverage
test:
	$(GO) test ./... -v -cover -coverprofile=coverage.out

# Run the standard local quality gate
check: vet lint test


# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out
	rm -rf .gocache/
	rm -rf .golangci-cache/

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
	openssl genrsa -out config/keys/private.pem 2048
	openssl rsa -in config/keys/private.pem -pubout -out config/keys/public.pem

# Show help
help:
	@echo "Zhiguang Go Server — Available targets:"
	@echo "  build   - Build the application binary"
	@echo "  run     - Run the application with CONFIG=$(CONFIG)"
	@echo "  fmt     - Format all Go source files with gofmt"
	@echo "  vet     - Run go vet"
	@echo "  lint    - Run golangci-lint (or go vet fallback)"
	@echo "  test    - Run all tests with coverage"
	@echo "  check   - Run vet, lint, and tests"
	@echo "  clean   - Remove build artifacts"
	@echo "  mod     - Tidy Go modules"
	@echo "  deps    - Download dependencies"
	@echo "  dev-up  - Start the full Docker Compose stack"
	@echo "  dev-down - Stop the full Docker Compose stack"
	@echo "  dev-logs - Tail full Docker Compose logs"
	@echo "  db-init - Initialize the MySQL schema inside Docker"
	@echo "  gen-jwt-keys - Generate local RSA keys for JWT signing"
