export RUST_LOG=info
export JSON_MIGRATOR_DATABASE_CONFIG='{max_postgres_connections=500, url="postgres://solana:solana@localhost:5432/solana"}'
export JSON_MIGRATOR_JSON_SOURCE_DB="./rocksdb/rocksdb-data"
export JSON_MIGRATOR_JSON_TARGET_DB="./dest-rocksdb"
export JSON_MIGRATOR_METRICS_PORT=9091
export JSON_MIGRATOR_WORK_MODE="Full"
cargo run --package nft_ingester --bin migrator
