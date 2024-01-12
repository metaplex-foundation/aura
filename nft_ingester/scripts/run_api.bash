export RUST_LOG=info
export RUST_BACKTRACE=1
export API_DATABASE_CONFIG='{max_postgres_connections=250, url="postgres://solana:solana@localhost:5432/v3"}'
export API_ROCKS_DB_PATH_CONTAINER="/rocksdb/rocksdb-data"
export API_ROCKS_DB_SECONDARY_PATH_CONTAINER="/rocksdb/secondary-api-rocksdb-data"
export API_ROCKS_SYNC_INTERVAL_SECONDS=5
export API_METRICS_PORT=8985
export API_SERVER_PORT=8990
export API_RUST_LOG=info
export API_SQL_LOG_LEVEL="error"
export API_PEER_GRPC_PORT=8991
export API_PEER_GRPC_MAX_GAP_SLOTS=1000000
cargo run --package nft_ingester --bin api
