use std::sync::Arc;

use clap::Parser;
use metrics_utils::{utils::start_metrics, RocksDbMetricsConfig};
use nft_ingester::{
    config::{init_logger, RocksDbBackupServiceClapArgs},
    init::graceful_stop,
};
use prometheus_client::registry::Registry;
use rocks_db::{
    backup_service::{RocksDbBackupService, RocksDbBackupServiceConfig},
    errors::RocksDbBackupServiceError,
    migrator::MigrationState,
    Storage,
};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), RocksDbBackupServiceError> {
    let args = RocksDbBackupServiceClapArgs::parse();
    init_logger(&args.log_level);

    info!("Starting RocksDb backup service...");

    let guard = if args.is_run_profiling {
        Some(pprof::ProfilerGuardBuilder::default().frequency(100).build().unwrap())
    } else {
        None
    };

    let mut registry = Registry::default();
    let metrics = Arc::new(RocksDbMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let storage = Storage::open_secondary(
        &args.rocks_db_path_container,
        &args.rocks_db_secondary_path,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    debug!(
        rocks_db_path_container = %args.rocks_db_path_container,
        rocks_db_secondary_path = %args.rocks_db_secondary_path,
        "Opened RocksDb in secondary mode"
    );

    let rocks_storage = Arc::new(storage);
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let shutdown_token = CancellationToken::new();

    info!("Starting store DB backup...");
    let mut backup_service = RocksDbBackupService::new(
        rocks_storage.db.clone(),
        &RocksDbBackupServiceConfig {
            rocks_backup_dir: args.backup_dir,
            rocks_backup_archives_dir: args.backup_archives_dir,
            rocks_flush_before_backup: args.flush_before_backup,
            rocks_interval_in_seconds: args.interval_in_seconds,
        },
    )?;
    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks
        .lock()
        .await
        .spawn(async move { backup_service.perform_backup(metrics.clone(), cloned_rx).await });

    start_metrics(registry, args.metrics_port).await;
    // --stop
    graceful_stop(
        mutexed_tasks.clone(),
        shutdown_tx,
        Some(shutdown_token),
        guard,
        args.profiling_file_path_container,
        &args.heap_path,
    )
    .await;

    Ok(())
}
