use std::sync::Arc;

use nft_ingester::backfiller::{BackfillSource, Backfiller, DirectBlockParser, TransactionsParser};
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{
    self, init_logger, setup_config, BackfillerConfig, RawBackfillConfig, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use nft_ingester::processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::transaction_ingester;
use prometheus_client::registry::Registry;
use tempfile::TempDir;
use tracing::{error, info};

use metrics_utils::red::RequestErrorDurationMetrics;
use metrics_utils::utils::setup_metrics;
use metrics_utils::{BackfillerMetricsConfig, IngesterMetricsConfig};
use rocks_db::bubblegum_slots::BubblegumSlotGetter;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting raw backfill server...");

    let config: RawBackfillConfig = setup_config(INGESTER_CONFIG_PREFIX);
    init_logger(&config.log_level);

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

    let mut registry = Registry::default();
    let metrics = Arc::new(BackfillerMetricsConfig::new());
    metrics.register(&mut registry);
    let ingester_metrics = Arc::new(IngesterMetricsConfig::new());
    ingester_metrics.register(&mut registry);

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

    let primary_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    {
        // storage in secondary mod cannot create new column families, that
        // could be required for migration_version_manager, so firstly open
        // storage with MigrationState::CreateColumnFamilies in order to create
        // all column families
        Storage::open(
            &config
                .rocks_db_path_container
                .clone()
                .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
            mutexed_tasks.clone(),
            red_metrics.clone(),
            MigrationState::CreateColumnFamilies,
        )
        .unwrap();
    }
    let migration_version_manager_dir = TempDir::new().unwrap();
    let migration_version_manager = Storage::open_secondary(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        migration_version_manager_dir.path().to_str().unwrap(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();
    Storage::apply_all_migrations(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        &config.migration_storage_path,
        Arc::new(migration_version_manager),
    )
    .await
    .unwrap();
    let storage = Storage::open(
        &primary_storage_path,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let consumer = rocks_storage.clone();
    let backfiller_config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);

    let backfiller_source = Arc::new(
        BackfillSource::new(
            &backfiller_config.backfiller_source_mode,
            backfiller_config.rpc_host.clone(),
            &backfiller_config.big_table_config,
        )
        .await,
    );

    let backfiller = Backfiller::new(
        rocks_storage.clone(),
        backfiller_source.clone(),
        backfiller_config.clone(),
    );
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    match backfiller_config.backfiller_mode {
        config::BackfillerMode::IngestDirectly => {
            todo!();
        }
        config::BackfillerMode::Persist | config::BackfillerMode::PersistAndIngest => {
            backfiller
                .start_backfill(
                    mutexed_tasks.clone(),
                    shutdown_rx.resubscribe(),
                    metrics.clone(),
                    consumer,
                    backfiller_source.clone(),
                )
                .await
                .unwrap();
            info!("running backfiller to persist raw data");
        }
        config::BackfillerMode::IngestPersisted => {
            let buffer = Arc::new(Buffer::new());
            // run dev->null buffer consumer
            let cloned_rx = shutdown_rx.resubscribe();
            let clonned_json_deque = buffer.json_tasks.clone();
            mutexed_tasks.lock().await.spawn(async move {
                info!("Running empty buffer consumer...");
                while cloned_rx.is_empty() {
                    clonned_json_deque.lock().await.clear();
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }

                Ok(())
            });
            let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
                rocks_storage.clone(),
                ingester_metrics.clone(),
                buffer.json_tasks.clone(),
            ));

            let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
                bubblegum_updates_processor.clone(),
            ));

            let consumer = Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                rocks_storage.clone(),
                metrics.clone(),
            ));
            let producer = rocks_storage.clone();

            let transactions_parser = Arc::new(TransactionsParser::new(
                rocks_storage.clone(),
                Arc::new(BubblegumSlotGetter::new(rocks_storage.clone())),
                consumer,
                producer,
                metrics.clone(),
                backfiller_config.workers_count,
                backfiller_config.chunk_size,
            ));

            mutexed_tasks.lock().await.spawn(async move {
                info!("Running transactions parser...");

                transactions_parser
                    .parse_raw_transactions(
                        shutdown_rx.resubscribe(),
                        backfiller_config.permitted_tasks,
                        backfiller_config.slot_until,
                    )
                    .await;

                Ok(())
            });

            info!("running backfiller on persisted raw data");
        }
        config::BackfillerMode::None => {
            info!("not running backfiller");
        }
    };

    // --stop
    graceful_stop(
        mutexed_tasks,
        shutdown_tx,
        guard,
        config.profiling_file_path_container,
        &config.heap_path,
    )
    .await;

    Ok(())
}
