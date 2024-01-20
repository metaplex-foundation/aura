use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log::{error, info};
use nft_ingester::config::{init_logger, setup_config, MigrateRocksConfig};
use nft_ingester::error::IngesterError;
use prometheus_client::registry::Registry;

use metrics_utils::utils::setup_metrics;
use metrics_utils::{MetricStatus, RocksMigrateMetricsConfig};
use rocks_db::Storage;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: MigrateRocksConfig = setup_config();
    init_logger(&config.log_level);

    info!("Starting Rocks DB`s migrate...");

    let mut registry = Registry::default();
    let metrics = Arc::new(RocksMigrateMetricsConfig::new());
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
    let targeted_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());
    let targeted_storage = Storage::open(&targeted_storage_path, mutexed_tasks.clone()).unwrap();
    let rocks_targeted_storage = Arc::new(targeted_storage);

    let migrated_storage_path = config.migrated_rocks_path.clone().unwrap();
    let migrated_storage = Storage::open(&migrated_storage_path, mutexed_tasks.clone()).unwrap();
    let rocks_migrated_storage = Arc::new(migrated_storage);

    info!(
        "Start migration from {} into {}",
        &migrated_storage_path, &targeted_storage_path
    );

    let keep_running = Arc::new(AtomicBool::new(true));
    migrate_rocks(
        keep_running.clone(),
        mutexed_tasks.clone(),
        rocks_targeted_storage,
        rocks_migrated_storage,
        metrics,
    )
    .await;

    while let Some(task) = mutexed_tasks.lock().await.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the tasks was finished")
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Task error: {}", err);
            }
        }
    }

    Ok(())
}

async fn migrate_rocks(
    keep_running: Arc<AtomicBool>,
    mutex_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    targeted_storage: Arc<Storage>,
    migrated_storage: Arc<Storage>,
    metrics: Arc<RocksMigrateMetricsConfig>,
) {
    let slots_iterator = migrated_storage.raw_blocks_cbor.iter_start();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(1000)); // max parallel tasks
    let mut tasks = Vec::new();
    for slot in slots_iterator {
        if !keep_running.load(Ordering::SeqCst) {
            break;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let targeted_storage_clone = targeted_storage.clone();
        let metrics_clone = metrics.clone();
        tasks.push(tokio::task::spawn(async move {
            let _permit = permit;
            match slot {
                Ok((key, value)) => {
                    let block =
                        match serde_cbor::from_slice::<rocks_db::raw_block::RawBlock>(&value) {
                            Ok(block) => block,
                            Err(e) => {
                                metrics_clone
                                    .inc_slots_merged("deserialized_block", MetricStatus::FAILURE);
                                error!("serde_cbor::from_slice: {}", e);
                                return;
                            }
                        };
                    let slot = match rocks_db::key_encoders::decode_u64(key.to_vec()) {
                        Ok(slot) => slot,
                        Err(e) => {
                            metrics_clone
                                .inc_slots_merged("deserialized_slot", MetricStatus::FAILURE);
                            error!("decode_u64: {}", e);
                            return;
                        }
                    };

                    match targeted_storage_clone
                        .raw_blocks_cbor
                        .put_cbor_encoded(slot, block)
                        .await
                    {
                        Ok(_) => {
                            metrics_clone
                                .inc_slots_merged("put_cbor_encoded", MetricStatus::SUCCESS);
                        }
                        Err(e) => {
                            error!("put_cbor_encoded: {}", e);
                            metrics_clone
                                .inc_slots_merged("put_cbor_encoded", MetricStatus::FAILURE);
                        }
                    }
                    metrics_clone.set_last_merged_slot(slot as i64);
                }
                Err(e) => {
                    error!("slot: {}", e);
                    metrics_clone.inc_slots_merged("slots_iterator", MetricStatus::FAILURE);
                }
            }
        }));
    }

    mutex_tasks.lock().await.spawn(tokio::spawn(async move {
        for task in tasks {
            task.await.unwrap_or_else(|e| error!("migrate task {}", e));
        }
    }));
}
