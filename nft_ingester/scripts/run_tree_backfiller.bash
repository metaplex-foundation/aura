export RUST_LOG=info
export TREE_BACKFILLER_SOURCE_ROCKS="./backfilled-tree"
export TREE_BACKFILLER_TARGET_ROCKS="./rocks_target"

export INGESTER_LOOKUP_KEY="BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"

export TREE_BACKFILLER_METRICS_PORT=8080

export INGESTER_BIG_TABLE_CONFIG='{creds="./creds.json", timeout=1000}'

export INGESTER_BACKFILLER_MODE=Persist
export INGESTER_SLOT_UNTIL=0
export INGESTER_SLOT_START_FROM=246464001
cargo run --package nft_ingester --bin tree_backfiller
