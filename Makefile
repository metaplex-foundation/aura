.PHONY: build start build-integrity-verification start-integrity-verification start-raw-backfiller start-core-indexing dev stop clippy test

SHELL := /bin/bash

build:
	@docker compose -f docker-compose.yaml build ingester raw-backfiller das-api synchronizer core-indexing slot-persister

start:
	@docker compose -f docker-compose.yaml up -d ingester

start-slot-persister:
	@docker compose -f docker-compose.yaml up -d slot-persister

start-synchronizer:
	@docker compose -f docker-compose.yaml up -d synchronizer

start-api:
	@docker compose -f docker-compose.yaml up -d das-api

stop-api:
	@docker stop --time 20 das-api

build-integrity-verification:
	@docker compose -f docker-compose.yaml build integrity-verification

start-integrity-verification:
	@docker compose -f docker-compose.yaml up -d integrity-verification

start-raw-backfiller:
	@docker compose -f docker-compose.yaml up -d raw-backfiller

start-core-indexing:
	@docker compose -f docker-compose.yaml up -d core-indexing

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