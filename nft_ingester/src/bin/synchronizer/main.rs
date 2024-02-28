use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use nft_ingester::config::{
    init_logger, setup_config, SynchronizerConfig, SYNCHRONIZER_CONFIG_PREFIX,
};
use nft_ingester::error::IngesterError;
use nft_ingester::index_syncronizer::Synchronizer;
use nft_ingester::init::graceful_stop;
use postgre_client::PgClient;
use prometheus_client::registry::Registry;

use metrics_utils::utils::setup_metrics;
use metrics_utils::SynchronizerMetricsConfig;
use rocks_db::column::TypedColumn;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::{AssetsUpdateIdx, Storage};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const DEFAULT_SECONDARY_ROCKSDB_PATH: &str = "./my_rocksdb_secondary";
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    tracing::info!("Starting Synchronizer server...");

    let config: SynchronizerConfig = setup_config(SYNCHRONIZER_CONFIG_PREFIX);
    init_logger(config.log_level.as_str());
    let mut guard = None;
    if config.run_profiling {
        guard = Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        );
    }

    let max_postgre_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(DEFAULT_MAX_POSTGRES_CONNECTIONS);

    let mut registry = Registry::default();
    let metrics = Arc::new(SynchronizerMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);
    tokio::spawn(async move {
        match setup_metrics(registry, config.metrics_port).await {
            Ok(_) => {
                tracing::info!("Setup metrics successfully")
            }
            Err(e) => {
                tracing::error!("Setup metrics failed: {:?}", e)
            }
        }
    });

    let index_storage = Arc::new(
        PgClient::new(
            &config.database_config.get_database_url().unwrap(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            max_postgre_connections,
            red_metrics.clone(),
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
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let cloned_tasks = mutexed_tasks.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        let keep_running = Arc::new(AtomicBool::new(true));
        // --stop
        graceful_stop(
            cloned_tasks,
            keep_running.clone(),
            shutdown_tx,
            guard,
            config.profiling_file_path_container,
        )
        .await;
    }));

    let synchronizer = Synchronizer::new(
        rocks_storage.clone(),
        index_storage.clone(),
        config.dump_synchronizer_batch_size,
        config.dump_path.to_string(),
        metrics.clone(),
    );

    for item in rocks_storage.assets_update_idx.iter_end() {
        let (idx_key, _) = item.unwrap();
        let key = AssetsUpdateIdx::decode_key(idx_key.to_vec())?;
        let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
        let (seq, slot, pubkey) = (decoded_key.0, decoded_key.1, decoded_key.2);
        if slot >= 250868000 {
            continue;
        }
        tracing::info!(
            "Assuming the last key has seq {} slot {} pubkey {}",
            seq,
            slot,
            pubkey
        );
        tracing::info!("Running write dump on start");

        synchronizer
            .write_dump_to_index_storage(decoded_key)
            .await
            .unwrap();
        tracing::info!("Dump synchronizer finished");
        return Ok(());
    }
    tracing::error!("No keys found in the database");
    return Ok(());

    tracing::info!("Running dump synchronizer on start");
    synchronizer
        .full_syncronize(shutdown_rx.resubscribe())
        .await
        .unwrap();
    tracing::info!("Dump synchronizer finished");

    Ok(())
}
