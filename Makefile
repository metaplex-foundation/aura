.PHONY: ci build start build-integrity-verification start-integrity-verification dev stop clippy test start-backfiller
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

# Generate a changelog using git-cliff
changelog:
	@git-cliff --output CHANGELOG.md
