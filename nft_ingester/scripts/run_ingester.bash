export RUST_LOG=error
export INGESTER_DATABASE_CONFIG='{max_postgres_connections=500, url="postgres://solana:solana@localhost:5432/v3"}'
export INGESTER_TCP_CONFIG='{receiver_addr=["eq15.everstake.one:2000", "127.0.0.1:3052"], receiver_connect_timeout=10, receiver_reconnect_interval=5, backfiller_receiver_addr="127.0.0.1:3334", backfiller_receiver_connect_timeout=10, backfiller_receiver_reconnect_interval=5, backfiller_sender_port=3334, backfiller_sender_batch_max_bytes=1, backfiller_sender_buffer_size=1,snapshot_receiver_addr="127.0.0.1:3052"}'
export INGESTER_TX_BACKGROUND_SAVERS=5
export INGESTER_BACKFILL_BACKGROUND_SAVERS=1
export INGESTER_MPLX_BUFFER_SIZE=500
export INGESTER_MPLX_WORKERS=10
export INGESTER_SPL_BUFFER_SIZE=500
export INGESTER_SPL_WORKERS=50
export INGESTER_CONSUMER_NUMBER=0
export RUST_BACKTRACE=1
export INGESTER_METRICS_PORT_FIRST_CONSUMER=9094
export INGESTER_METRICS_PORT_SECOND_CONSUMER=9095
export INGESTER_BACKGROUND_TASK_RUNNER_CONFIG='{delete_interval=30, retry_interval=1, purge_time=3600, batch_size=100, lock_duration=600, max_attempts=3, timeout=3}'

export INGESTER_ROCKS_DB_PATH_CONTAINER="/rocksdb/rocksdb-data"
export INGESTER_ROCKS_BACKUP_URL="127.0.0.1:3051/snapshot"
export INGESTER_ROCKS_BACKUP_ARCHIVES_DIR="/rocksdb/_rocks_backup_archives"
export INGESTER_ROCKS_BACKUP_DIR="/rocksdb/_rocksdb_backup"
export INGESTER_ROCKS_FLUSH_BEFORE_BACKUP=true
export INGESTER_ROCKS_INTERVAL_IN_SECONDS=7200

export APP_DATABASE_URL="postgres://solana:solana@localhost:5432/v3"
export APP_SERVER_PORT=9090
export APP_METRICS_PORT=8125
export APP_METRICS_HOST="localhost"
export APP_ARCHIVES_DIR="/rocksdb/_rocks_backup_archives"

export INGESTER_BG_TASK_RUNNER_METRICS_PORT=9093

export INGESTER_RUN_BUBBLEGUM_BACKFILLER=true
export INGESTER_SLOT_UNTIL=0
export INGESTER_SLOT_START_FROM=236032212
export INGESTER_BIG_TABLE_CONFIG='{creds="./creds.json", timeout=1000}'

export INGESTER_SYNCHRONIZER_BATCH_SIZE=500

cargo run --package nft_ingester --bin ingester
# start with restore rocks DB
#cargo run --package nft_ingester --bin ingester -- --restore-rocks-db
