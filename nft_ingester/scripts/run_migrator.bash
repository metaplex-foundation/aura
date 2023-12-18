export RUST_LOG=error
export INGESTER_DATABASE_CONFIG='{max_postgres_connections=500, url="postgres://solana:solana@localhost:5432/v2"}'
export INGESTER_OLD_DATABASE_CONFIG='{max_postgres_connections=500, url="postgres://solana:solana@localhost:5432/solana"}'
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
cargo run --package nft_ingester --bin migrator
