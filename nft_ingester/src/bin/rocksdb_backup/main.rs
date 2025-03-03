use std::sync::Arc;

use clap::Parser;
use nft_ingester::config::{init_logger, RocksDbBackupServiceClapArgs};
use prometheus_client::registry::Registry;
use rocks_db::{
    backup_service::{RocksDbBackupService, RocksDbBackupServiceConfig},
    errors::RocksDbBackupServiceError,
    migrator::MigrationState,
    Storage,
};
use tracing::{debug, info};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), RocksDbBackupServiceError> {
    let args = RocksDbBackupServiceClapArgs::parse();
    init_logger(&args.log_level);

    info!("Starting RocksDb backup service...");

    let mut registry = Registry::default();
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);

    let storage = Storage::open_secondary(
        &args.rocks_db_path,
        &args.rocks_db_secondary_path,
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    debug!(
        rocks_db_path_container = ?args.rocks_db_path,
        rocks_db_secondary_path = ?args.rocks_db_secondary_path,
        "Opened RocksDb in secondary mode"
    );

    let rocks_storage = Arc::new(storage);

    info!("Starting store DB backup...");
    let mut backup_service = RocksDbBackupService::new(
        rocks_storage.db.clone(),
        &RocksDbBackupServiceConfig {
            rocks_backup_dir: args.backup_dir,
            rocks_backup_archives_dir: args.backup_archives_dir,
            rocks_flush_before_backup: args.flush_before_backup,
        },
    )?;

    backup_service.perform_backup().await?;

    Ok(())
}
