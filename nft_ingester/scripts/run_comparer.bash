export COMPARER_RUST_LOG=info
export COMPARER_SOURCE_ROCKS="./backfilled-tree"
export COMPARER_TARGET_ROCKS="./rocks_target"
export COMPARER_TARGET_TREE_KEY=""
export COMPARER_METRICS_PORT=8080

cargo run --package nft_ingester --bin cl_comparer
