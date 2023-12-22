.PHONY: build start dev stop

SHELL := /bin/bash

build:
	@docker compose -f docker-compose.yaml build ingester-first-consumer

start:
	@docker compose -f docker-compose.yaml up -d ingester-first-consumer

dev:
	@docker compose -f docker-compose.yaml up -d db

stop:
	@docker compose -f docker-compose.yaml stop ingester-first-consumer
