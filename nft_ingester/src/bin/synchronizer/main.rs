use entities::enums::ASSET_TYPES;
use nft_ingester::config::{
    init_logger, setup_config, SynchronizerConfig, SYNCHRONIZER_CONFIG_PREFIX,
};
use nft_ingester::error::IngesterError;
use nft_ingester::index_syncronizer::{SyncStatus, Synchronizer};
use nft_ingester::init::{graceful_stop, init_index_storage_with_migration};
use postgre_client::PG_MIGRATIONS_PATH;
use prometheus_client::registry::Registry;
use std::path::PathBuf;
use std::sync::Arc;

use metrics_utils::SynchronizerMetricsConfig;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const DEFAULT_SECONDARY_ROCKSDB_PATH: &str = "./my_rocksdb_secondary";
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: SynchronizerConfig = setup_config(SYNCHRONIZER_CONFIG_PREFIX);
    init_logger(config.log_level.as_str());
    tracing::info!("Starting Synchronizer server...");

    let guard = if config.run_profiling {
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    let max_postgre_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(DEFAULT_MAX_POSTGRES_CONNECTIONS);

    let mut registry = Registry::default();
    let metrics = Arc::new(SynchronizerMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);
    metrics_utils::utils::start_metrics(registry, config.metrics_port).await;

    let index_storage = Arc::new(
        init_index_storage_with_migration(
            &config.database_config.get_database_url().unwrap(),
            max_postgre_connections,
            red_metrics.clone(),
            DEFAULT_MAX_POSTGRES_CONNECTIONS,
            PG_MIGRATIONS_PATH,
            Some(PathBuf::from(config.dump_path.clone())),
        )
        .await?,
    );

    let primary_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());
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
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);
    let cloned_tasks = mutexed_tasks.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    mutexed_tasks.lock().await.spawn(async move {
        // --stop
        graceful_stop(
            cloned_tasks,
            shutdown_tx,
            None,
            guard,
            config.profiling_file_path_container,
            &config.heap_path,
        )
        .await;

        Ok(())
    });

    let synchronizer = Arc::new(Synchronizer::new(
        rocks_storage.clone(),
        index_storage.clone(),
        config.dump_synchronizer_batch_size,
        config.dump_path.to_string(),
        metrics.clone(),
        config.parallel_tasks,
    ));

    if let Err(e) = rocks_storage.db.try_catch_up_with_primary() {
        tracing::error!("Sync rocksdb error: {}", e);
    }

    for asset_type in ASSET_TYPES {
        let synchronizer = synchronizer.clone();
        let shutdown_rx = shutdown_rx.resubscribe();

        mutexed_tasks.lock().await.spawn(async move {
            if let Ok(SyncStatus::FullSyncRequired(_)) = synchronizer
                .get_sync_state(config.dump_sync_threshold, asset_type)
                .await
            {
                let res = synchronizer.full_syncronize(&shutdown_rx, asset_type).await;
                match res {
                    Ok(_) => {
                        tracing::info!("Full synchronization finished successfully");
                    }
                    Err(e) => {
                        tracing::error!("Full synchronization failed: {:?}", e);
                    }
                }
            }

            Ok(())
        });
    }
    while (mutexed_tasks.lock().await.join_next().await).is_some() {}

    let shutdown_rx_clone = shutdown_rx.resubscribe();
    let synchronizer_clone = synchronizer.clone();
    mutexed_tasks.lock().await.spawn(async move {
        let shutdown_rx = shutdown_rx_clone.resubscribe();
        let synchronizer = synchronizer_clone.clone();
        while shutdown_rx.is_empty() {
            let result = synchronizer
                .synchronize_nft_asset_indexes(&shutdown_rx, config.dump_sync_threshold)
                .await;

            match result {
                Ok(_) => {
                    tracing::info!("Non Fungible Synchronization finished successfully",)
                }
                Err(e) => tracing::error!("Non Fungible Synchronization failed: {:?}", e),
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(
                config.timeout_between_syncs_sec,
            ))
            .await;
        }

        Ok(())
    });

    mutexed_tasks.lock().await.spawn(async move {
        let shutdown_rx = shutdown_rx.resubscribe();
        let synchronizer = synchronizer.clone();
        while shutdown_rx.is_empty() {
            let result = synchronizer
                .synchronize_fungible_asset_indexes(&shutdown_rx, config.dump_sync_threshold)
                .await;

            match result {
                Ok(_) => {
                    tracing::info!("Fungible Synchronization finished successfully",)
                }
                Err(e) => tracing::error!("Fungible Synchronization failed: {:?}", e),
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(
                config.timeout_between_syncs_sec,
            ))
            .await;
        }

        Ok(())
    });

    while (mutexed_tasks.lock().await.join_next().await).is_some() {}

    Ok(())
}
