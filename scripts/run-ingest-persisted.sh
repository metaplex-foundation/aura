#!/usr/bin/env bash

cargo b --release --package nft_ingester --bin raw_backfiller

# This group of parameters has to be changed depending on the server where we are running it
# or range of slots we are going to persist.
export INGESTER_ROCKS_DB_PATH_CONTAINER="/rocksdb-data"
export INGESTER_BIG_TABLE_CONFIG='{creds="/aura/creds.json", timeout=1000}'
export INGESTER_MIGRATION_STORAGE_PATH="/migration_storage"
export INGESTER_METRICS_PORT=9091

# This group of parameters almost never changes.
# These are like constants for ingesting persisted raw Solana blocks.
export INGESTER_BACKFILLER_MODE=IngestPersisted
export INGESTER_SLOT_UNTIL=0
export INGESTER_SLOT_START_FROM=0
export INGESTER_LOG_LEVEL=warn
export INGESTER_RUN_PROFILING=false
export INGESTER_PROFILING_FILE_PATH_CONTAINER="./"
export INGESTER_HEAP_PATH="/usr/src/app/heaps"
export INGESTER_WORKERS_COUNT=1
export INGESTER_CHUNK_SIZE=20
export INGESTER_PERMITTED_TASKS=5000
export INGESTER_WAIT_PERIOD_SEC=60
export INGESTER_SHOULD_REINGEST=false

./target/release/raw_backfiller
