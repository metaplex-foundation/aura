.PHONY: ci build start build-integrity-verification start-integrity-verification dev stop clippy test start-backfiller ensure-git-cliff changelog release-changelog release
SHELL := /bin/bash

ci:
	@./docker/build-all.sh

build:
	@docker compose -f docker-compose.yaml build ingester das-api synchronizer slot-persister backfill

start:
	@docker compose -f docker-compose.yaml up -d ingester

start-slot-persister:
	@docker compose -f docker-compose.yaml up -d slot-persister

start-synchronizer:
	@docker compose -f docker-compose.yaml up -d synchronizer

start-api:
	@docker compose -f docker-compose.yaml up -d das-api

start-rocksdb-backup:
	@docker compose -f docker-compose.yaml up -d rocksdb-backup

stop-api:
	@docker stop --time 20 das-api

build-integrity-verification:
	@docker compose -f docker-compose.yaml build integrity-verification

start-integrity-verification:
	@docker compose -f docker-compose.yaml up -d integrity-verification

start-backfiller:
	@docker compose -f docker-compose.yaml up -d backfill

dev:
	@docker compose -f docker-compose.yaml up -d db

stop:
	@docker stop --time 1000 ingester

clippy:
	@cargo clean -p postgre-client -p rocks-db -p interface
	@cargo clippy

test:
	@cargo clean -p postgre-client -p rocks-db -p interface
	@cargo test --features integration_tests

# Ensure git-cliff is installed
ensure-git-cliff:
	@which git-cliff > /dev/null || cargo install git-cliff

# Generate a full changelog using git-cliff
changelog:
	@git-cliff --output CHANGELOG.md

# Generate an incremental changelog for a specific release
# Usage: make release-changelog VERSION=0.5.0 [PREV_VERSION=v0.4.0]
release-changelog:
	@scripts/update_changelog.sh "v$(VERSION)" $(PREV_VERSION)

# Trigger release preparation workflow
# Usage: make release VERSION=0.5.0 [BASE_COMMIT=abc123]
release:
	@echo "Preparing release v$(VERSION)..."
	@[ -n "$(VERSION)" ] || (echo "Error: VERSION is required. Usage: make release VERSION=0.5.0"; exit 1)
	@gh workflow run release-prepare.yml -f version=$(VERSION) $(if $(BASE_COMMIT),-f base_commit=$(BASE_COMMIT),)
	@echo "Release preparation workflow triggered. Check GitHub Actions for progress."
