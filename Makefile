.PHONY: build start build-json-downloader start-json-downloader build-backfiller start-backfiller dev stop

SHELL := /bin/bash

build:
	@docker compose -f docker-compose.yaml build ingester-first-consumer ingester-second-consumer

start:
	@docker compose -f docker-compose.yaml up -d ingester-first-consumer ingester-second-consumer

build-json-downloader:
	@docker compose -f docker-compose.yaml build

start-json-downloader:
	@docker compose -f docker-compose.yaml up -d

build-backfiller:
	@docker compose -f docker-compose.yaml build backfiller-consumer backfiller

start-backfiller:
	@docker compose -f docker-compose.yaml up -d backfiller-consumer backfiller

dev:
	@docker compose -f docker-compose.yaml up -d db

stop:
	@docker compose -f docker-compose.yaml stop ingester-first-consumer ingester-second-consumer backfiller
