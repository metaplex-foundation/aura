use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use interface::signature_persistence::BlockProducer;
use nft_ingester::backfiller::connect_new_bigtable_from_config;
use nft_ingester::config::{
    init_logger, setup_config, BackfillerConfig, IngesterConfig, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::error::IngesterError;
use rocks_db::Storage;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};

const DEFAULT_SECONDARY_ROCKSDB_PATH: &str = "./secondary_db";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());
    info!("Starting Comparing Blocks");

    let primary_storage_path = config.rocks_db_path_container.clone().unwrap();
    let secondary_storage_path = config
        .rocks_db_secondary_path_container
        .clone()
        .unwrap_or(DEFAULT_SECONDARY_ROCKSDB_PATH.to_string());

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    let storage = Storage::open_secondary(
        &primary_storage_path,
        &secondary_storage_path,
        mutexed_tasks.clone(),
    )
    .unwrap();
    let rocks_storage = Arc::new(storage);
    let backfiller_config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
    let big_table_client = Arc::new(
        connect_new_bigtable_from_config(backfiller_config.clone())
            .await
            .unwrap(),
    );

    for block in get_blocks_to_compare() {
        let bigtable_block = big_table_client.get_block(block).await.unwrap();
        let storage_block = rocks_storage
            .raw_blocks_cbor
            .get_cbor_encoded(block)
            .await
            .unwrap()
            .unwrap()
            .block;

        if let Some(diff) = assert_json_matches_no_panic(
            &json!(bigtable_block),
            &json!(storage_block),
            Config::new(CompareMode::Strict),
        )
        .err()
        {
            error!("block: {}, diff: {}", block, diff)
        }
    }

    Ok(())
}

fn get_blocks_to_compare() -> Vec<u64> {
    // TODO
    vec![]
}
