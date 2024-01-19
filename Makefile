.PHONY: build start dev stop clippy test

SHELL := /bin/bash

build:
	@docker compose -f docker-compose.yaml build ingester-first-consumer

start:
	@docker compose -f docker-compose.yaml up -d ingester-first-consumer

start-raw-backriller:
	@docker compose -f docker-compose.yaml up -d raw-backriller

dev:
	@docker compose -f docker-compose.yaml up -d db

stop:
	@docker stop --time 1000 ingester-first-consumer

clippy:
	@cargo clean -p postgre-client -p rocks-db -p interface
	@cargo clippy

test:
	@cargo clean -p postgre-client -p rocks-db -p interface
	@cargo test --features integration_tests