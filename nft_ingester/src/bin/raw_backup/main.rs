#![deprecated = "Deprecated in favor of `ingester` binary"]
use std::sync::Arc;

use clap::{arg, Parser};
use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::{config::init_logger, error::IngesterError};
use rocks_db::{
    column::TypedColumn, columns::offchain_data::OffChainData, migrator::MigrationState, Storage,
};
use tempfile::TempDir;
use tracing::info;

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

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let secondary_rocks_dir = TempDir::new().unwrap();
    let source_storage = Storage::open_secondary(
        config.source_db.as_str(),
        secondary_rocks_dir.path().to_str().unwrap(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let target_storage =
        Storage::open(&config.target_db, red_metrics.clone(), MigrationState::Last).unwrap();

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
