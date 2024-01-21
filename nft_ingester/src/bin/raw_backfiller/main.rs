use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use log::{error, info};
use nft_ingester::backfiller::{Backfiller, BigTableClient};
use nft_ingester::config::{init_logger, setup_config, BackfillerConfig, RawBackfillConfig};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use prometheus_client::registry::Registry;

use metrics_utils::utils::setup_metrics;
use metrics_utils::BackfillerMetricsConfig;
use rocks_db::Storage;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinSet;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting raw backfill server...");

    let config: RawBackfillConfig = setup_config();
    init_logger(&config.log_level);

    let mut guard = None;
    if config.run_profiling {
        guard = Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        );
    }

    let mut registry = Registry::default();
    let metrics = Arc::new(BackfillerMetricsConfig::new());
    metrics.register(&mut registry);

    tokio::spawn(async move {
        match setup_metrics(registry, config.metrics_port).await {
            Ok(_) => {
                info!("Setup metrics successfully")
            }
            Err(e) => {
                error!("Setup metrics failed: {:?}", e)
            }
        }
    });

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let keep_running = Arc::new(AtomicBool::new(true));

    let primary_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());

    let storage = Storage::open(&primary_storage_path, mutexed_tasks.clone()).unwrap();

    let rocks_storage = Arc::new(storage);

    let consumer = rocks_storage.clone();
    let backfiller_config: BackfillerConfig = setup_config();

    let big_table_client = Arc::new(
        BigTableClient::connect_new_from_config(backfiller_config.clone())
            .await
            .unwrap(),
    );
    let backfiller = Backfiller::new(
        rocks_storage.clone(),
        big_table_client.clone(),
        backfiller_config.clone(),
    );

    backfiller
        .start_backfill(
            mutexed_tasks.clone(),
            keep_running.clone(),
            metrics.clone(),
            consumer,
            big_table_client.clone(),
        )
        .await
        .unwrap();
    info!("running backfiller to persist raw data");

    let (shutdown_tx, _shutdown_rx) = oneshot::channel::<()>();

    // --stop
    graceful_stop(
        mutexed_tasks,
        keep_running.clone(),
        shutdown_tx,
        guard,
        config.profiling_file_path_container,
    )
    .await;

    Ok(())
}
