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

clean: build
	@bin/mlaunch reset --yes

start: build
	@bin/mongo-cluster start

stop: ## Stop MongoDB cluster
	@bin/mongo-cluster stop --file cluster.json

setup: ## Run setup script
	@./bin/mongo-cluster start

seed: ## Run seed script
	@./bin/seed

test: ## Run unit tests (skips integration tests)
	@go test -short -v ./...

test-integration: ## Run all tests including integration tests
	@go test -tags=integration -v ./...

connect: ## Connect to MongoDB cluster
	@mongo keyhole

release: ## Commit VERSION file, create git tag, confirm, and push
	@if [ -z "$$(git status --porcelain VERSION)" ]; then \
		echo "No changes to VERSION file"; \
		exit 1; \
	fi
	@VERSION=$$(cat VERSION); \
	BRANCH=$$(git branch --show-current); \
	echo "Current VERSION: $$VERSION"; \
	echo "Current branch: $$BRANCH"; \
	echo ""; \
	echo "This will:"; \
	echo "  1. Commit changes to VERSION file"; \
	echo "  2. Create git tag v$$VERSION"; \
	echo "  3. Push commit to origin $$BRANCH"; \
	echo "  4. Push tag v$$VERSION to origin"; \
	echo ""; \
	printf "Continue? [y/N] "; \
	read REPLY; \
	if [ "$$REPLY" != "y" ] && [ "$$REPLY" != "Y" ]; then \
		echo "Aborted."; \
		exit 1; \
	fi; \
	git add VERSION; \
	git commit -m "Bump version to $$VERSION"; \
	git tag -a "v$$VERSION" -m "Release v$$VERSION"; \
	echo ""; \
	echo "Created tag v$$VERSION"; \
	echo "Pushing to remote..."; \
	git push origin $$BRANCH; \
	git push origin "v$$VERSION"; \
	echo ""; \
	echo "Release v$$VERSION pushed successfully!"

test-release: ## Test GoReleaser locally (snapshot build)
	@goreleaser release --snapshot --clean
	@./scripts/generate-casks-readme.sh

install-local: ## Install Homebrew formula from local dist directory (creates local tap)
	@if [ ! -f dist/homebrew/mongo-scaffold.rb ]; then \
		echo "Formula not found. Run 'make test-release' first to generate it."; \
		exit 1; \
	fi
	@echo "Creating local tap and installing formula..."
	@TAP_NAME="zph/local"; \
	TAP_DIR=$$(brew --repository)/$$TAP_NAME; \
	if [ ! -d $$TAP_DIR ]; then \
		echo "Creating local tap: $$TAP_NAME"; \
		mkdir -p $$TAP_DIR/Formula; \
		brew tap $$TAP_NAME 2>/dev/null || true; \
	fi; \
	cp dist/homebrew/mongo-scaffold.rb $$TAP_DIR/Formula/; \
	echo "Installing mongo-scaffold from local tap..."; \
	brew install mongo-scaffold
