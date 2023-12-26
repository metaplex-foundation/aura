.PHONY: build start dev stop clippy test

SHELL := /bin/bash

build:
	@docker compose -f docker-compose.yaml build ingester-first-consumer

start:
	@docker compose -f docker-compose.yaml up -d ingester-first-consumer

dev:
	@docker compose -f docker-compose.yaml up -d db

stop:
	@docker compose -f docker-compose.yaml stop ingester-first-consumer

clippy:
	@cargo clean -p postgre-client
	@cargo clean -p rocks-db
	@cargo clippy

test:
	@cargo clean -p postgre-client
	@cargo clean -p rocks-db
	@cargo test --features integration_tests