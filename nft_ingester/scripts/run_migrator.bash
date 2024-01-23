export RUST_LOG=info
export INGESTER_DATABASE_CONFIG='{max_postgres_connections=500, url="postgres://solana:solana@localhost:5432/solana"}'
export INGESTER_TCP_CONFIG='{receiver_addr=["eq15.everstake.one:2000", "127.0.0.1:3052"], receiver_connect_timeout=10, receiver_reconnect_interval=5, backfiller_receiver_addr="127.0.0.1:3334", backfiller_receiver_connect_timeout=10, backfiller_receiver_reconnect_interval=5, backfiller_sender_port=3334, backfiller_sender_batch_max_bytes=1, backfiller_sender_buffer_size=1}'
export INGESTER_TX_BACKGROUND_SAVERS=5
export INGESTER_BACKFILL_BACKGROUND_SAVERS=1
export INGESTER_MPLX_BUFFER_SIZE=500
export INGESTER_MPLX_WORKERS=10
export INGESTER_SPL_BUFFER_SIZE=500
export INGESTER_SPL_WORKERS=50
export INGESTER_CONSUMER_NUMBER=0
export RUST_BACKTRACE=1
export INGESTER_AEROSPIKE_CONFIG='{use_alternate=false, addr="localhost:3000"}'
export INGESTER_BACKGROUND_TASK_RUNNER_CONFIG='{delete_interval=30, retry_interval=1, purge_time=3600, batch_size=100, lock_duration=600, max_attempts=3, timeout=3}'
export INGESTER_METRICS_PORT_FIRST_CONSUMER=9091
export INGESTER_RUN_BUBBLEGUM_BACKFILLER=false
export INGESTER_SYNCHRONIZER_BATCH_SIZE=1
export INGESTER_GAPFILLER_PEER_ADDR="0.0.0.0"
export INGESTER_ROCKS_BACKUP_URL="127.0.0.1:3051/snapshot"
export INGESTER_PEER_GRPC_PORT=9090
export INGESTER_PEER_GRPC_MAX_GAP_SLOTS=1000000
export INGESTER_BACKFILL_RPC_ADDRESS="https://solana-mainnet.rpc.extrnode.com/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXX"

export INGESTER_ROCKS_BACKUP_ARCHIVES_DIR="./dest-rocksdb/_rocks_backup_archives"
export INGESTER_ROCKS_BACKUP_DIR="./dest-rocksdb/_rocksdb_backup"

export INGESTER_JSON_SOURCE_DB="./rocksdb/rocksdb-data"
export INGESTER_ROCKS_DB_PATH_CONTAINER="./dest-rocksdb"
cargo run --package nft_ingester --bin migrator
