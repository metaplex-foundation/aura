use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use entities::enums::ASSET_TYPES;
use metrics_utils::SynchronizerMetricsConfig;
use nft_ingester::{
    config::{init_logger, SynchronizerClapArgs},
    error::IngesterError,
    index_syncronizer::{SyncStatus, Synchronizer},
    init::{graceful_stop, init_index_storage_with_migration},
};
use postgre_client::PG_MIGRATIONS_PATH;
use prometheus_client::registry::Registry;
use rocks_db::{migrator::MigrationState, Storage};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 2;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let args = SynchronizerClapArgs::parse();
    init_logger(&args.log_level);

    tracing::info!("Starting Synchronizer server...");

    let guard = if args.run_profiling {
        Some(pprof::ProfilerGuardBuilder::default().frequency(100).build().unwrap())
    } else {
        None
    };

    let mut registry = Registry::default();
    let metrics = Arc::new(SynchronizerMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);
    metrics_utils::utils::start_metrics(registry, args.metrics_port).await;

    let pg_index_storage = Arc::new(
        init_index_storage_with_migration(
            &args.pg_database_url,
            args.pg_max_db_connections,
            red_metrics.clone(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            PG_MIGRATIONS_PATH,
            Some(PathBuf::from(args.rocks_dump_path.clone())),
            Some(args.pg_max_query_statement_timeout_secs),
        )
        .await?,
    );

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

    let rocks_storage = Arc::new(storage);
    let cloned_tasks = mutexed_tasks.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let shutdown_token = CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();

    mutexed_tasks.lock().await.spawn(async move {
        // --stop
        graceful_stop(
            cloned_tasks,
            shutdown_tx,
            Some(shutdown_token_clone),
            guard,
            args.profiling_file_path_container,
            &args.heap_path,
        )
        .await;

        Ok(())
    });

    let synchronizer = Arc::new(Synchronizer::new(
        rocks_storage.clone(),
        pg_index_storage.clone(),
        args.dump_synchronizer_batch_size,
        args.rocks_dump_path.clone(),
        metrics.clone(),
        args.synchronizer_parallel_tasks,
    ));

    if let Err(e) = rocks_storage.db.try_catch_up_with_primary() {
        tracing::error!("Sync rocksdb error: {}", e);
    }

    let mut sync_tasks = JoinSet::new();
    for asset_type in ASSET_TYPES {
        let synchronizer = synchronizer.clone();
        let shutdown_rx = shutdown_rx.resubscribe();
        let shutdown_token = shutdown_token.clone();
        sync_tasks.spawn(async move {
            if let Ok(SyncStatus::FullSyncRequired(_)) = synchronizer
                .get_sync_state(args.dump_sync_threshold, asset_type)
                .await
            {
                tracing::info!("Starting full sync for {:?}", asset_type);
                let res = synchronizer.full_syncronize(&shutdown_rx, asset_type).await;
                match res {
                    Ok(_) => {
                        tracing::info!("Full {:?} synchronization finished successfully", asset_type);
                    }
                    Err(e) => {
                        tracing::error!("Full {:?} synchronization failed: {:?}", asset_type, e);
                    }
                }
            }
            while shutdown_rx.is_empty() {
                let result = synchronizer
                    .synchronize_asset_indexes(asset_type, &shutdown_rx, args.dump_sync_threshold)
                    .await;

                match result {
                    Ok(_) => {
                        tracing::info!("{:?} Synchronization finished successfully", asset_type)
                    }
                    Err(e) => tracing::error!("{:?} Synchronization failed: {:?}", asset_type, e),
                }
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(
                        args.timeout_between_syncs_sec,
                    )) => {}
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutdown signal received, stopping {:?} synchronizer", asset_type);
                        break;
                    }
                }
            }

        });
    }
    while let Some(task) = sync_tasks.join_next().await {
        task.map_err(|e| {
            IngesterError::UnrecoverableTaskError(format!("joining task failed: {}", e))
        })?;
    }

    while (mutexed_tasks.lock().await.join_next().await).is_some() {}

    Ok(())
}
