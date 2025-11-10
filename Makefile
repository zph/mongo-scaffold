.PHONY: help clean start setup stop seed build build-mlaunch build-m build-mongo-cluster test test-integration

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: build-m build-mlaunch build-mongo-cluster ## Build all binaries (m, mlaunch, mongo-cluster)

build-m: ## Build m binary (MongoDB version manager)
	@go build -o bin/m ./cmd/m

build-mlaunch: ## Build mlaunch binary (MongoDB cluster launcher)
	@go build -o bin/mlaunch ./cmd/mlaunch

build-mongo-cluster: ## Build mongo-cluster binary
	@go build -o bin/mongo-cluster ./cmd/mongo-cluster

clean: ## Clean data directory and stop running clusters
	@bin/mlaunch stop --dir ./data 2>/dev/null || true
	@rm -rf ./data

start: ## Start MongoDB cluster
	@bin/mlaunch start

stop: ## Stop MongoDB cluster
	@bin/mlaunch stop --dir ./data

setup: ## Run setup script
	@./bin/setup

seed: ## Run seed script
	@./bin/seed

test: ## Run unit tests (skips integration tests)
	@go test -short -v ./...

test-integration: ## Run all tests including integration tests
	@go test -tags=integration -v ./...

connect: ## Connect to MongoDB cluster
	@mongo keyhole
