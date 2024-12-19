use std::sync::Arc;

use clap::{arg, Parser};
use entities::models::RawBlock;
use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::config::init_logger;
use rocks_db::column::TypedColumn;
use rocks_db::columns::offchain_data::OffChainData;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::info;

use nft_ingester::error::IngesterError;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, required = true)]
    pub source_db: String,
    #[arg(short, long, required = true)]
    pub target_db: String,
    #[arg(short, long, default_value_t = String::from("info"))]
    log_level: String,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config = Args::parse();
    init_logger(config.log_level.as_str());

    info!("Started...");

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let secondary_rocks_dir = TempDir::new().unwrap();
    let source_storage = Storage::open_secondary(
        &config.source_db,
        &secondary_rocks_dir.path().to_str().unwrap().to_string(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let target_storage = Storage::open(
        &config.target_db,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let cf = &target_storage.db.cf_handle(RawBlock::NAME).unwrap();
    info!("Copying raw blocks...");
    source_storage
        .raw_blocks_cbor
        .iter_start()
        .filter_map(|k| k.ok())
        .for_each(|(k, v)| target_storage.db.put_cf(cf, k, v).unwrap());
    info!("Done copying raw blocks");

    let cf = &target_storage.db.cf_handle(OffChainData::NAME).unwrap();
    info!("Copying offchain data...");
    source_storage
        .asset_offchain_data
        .iter_start()
        .filter_map(|k| k.ok())
        .for_each(|(k, v)| target_storage.db.put_cf(cf, k, v).unwrap());
    info!("Done copying offchain data");
    Ok(())
}
