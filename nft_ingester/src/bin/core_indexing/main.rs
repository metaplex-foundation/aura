use std::sync::Arc;

use log::{error, info};
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{init_logger, setup_config, IngesterConfig, INGESTER_CONFIG_PREFIX};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;

use tempfile::TempDir;

use metrics_utils::{MetricState, MetricsTrait};
use nft_ingester::message_handler::MessageHandlerCoreIndexing;
use nft_ingester::mpl_core_fee_indexing_processor::MplCoreFeeProcessor;
use nft_ingester::tcp_receiver::TcpReceiver;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting raw backfill server...");

    let config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);
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

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    let primary_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());

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
            metrics_state.red_metrics.clone(),
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
        metrics_state.red_metrics.clone(),
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
        metrics_state.red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();
    let rocks_storage = Arc::new(storage);
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let buffer = Arc::new(Buffer::new());

    let message_handler = Arc::new(MessageHandlerCoreIndexing::new(buffer.clone()));

    let geyser_tcp_receiver = TcpReceiver::new(
        message_handler.clone(),
        config.tcp_config.get_tcp_receiver_reconnect_interval()?,
    );
    let snapshot_tcp_receiver = TcpReceiver::new(
        message_handler.clone(),
        config.tcp_config.get_tcp_receiver_reconnect_interval()? * 2,
    );

    let snapshot_addr = config.tcp_config.get_snapshot_addr_ingester()?;
    let geyser_addr = config
        .tcp_config
        .get_tcp_receiver_addr_ingester(config.consumer_number)?;

    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        let geyser_tcp_receiver = Arc::new(geyser_tcp_receiver);
        while cloned_rx.is_empty() {
            let geyser_tcp_receiver_clone = geyser_tcp_receiver.clone();
            let cl_rx = cloned_rx.resubscribe();
            if let Err(e) = tokio::spawn(async move {
                geyser_tcp_receiver_clone
                    .connect(geyser_addr, cl_rx)
                    .await
                    .unwrap()
            })
            .await
            {
                error!("geyser_tcp_receiver panic: {:?}", e);
            }
        }
        Ok(())
    });
    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        let snapshot_tcp_receiver = Arc::new(snapshot_tcp_receiver);
        while cloned_rx.is_empty() {
            let snapshot_tcp_receiver_clone = snapshot_tcp_receiver.clone();
            let cl_rx = cloned_rx.resubscribe();
            if let Err(e) = tokio::spawn(async move {
                snapshot_tcp_receiver_clone
                    .connect(snapshot_addr, cl_rx)
                    .await
                    .unwrap()
            })
            .await
            {
                error!("snapshot_tcp_receiver panic: {:?}", e);
            }
        }
        Ok(())
    });

    let mpl_core_fee_parser = MplCoreFeeProcessor::new(
        rocks_storage.clone(),
        buffer.clone(),
        metrics_state.ingester_metrics.clone(),
        config.mpl_core_buffer_size,
    );

    for _ in 0..config.mplx_workers {
        let mut cloned_core_parser = mpl_core_fee_parser.clone();
        let cloned_rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(async move {
            cloned_core_parser.process(cloned_rx).await;
            Ok(())
        });
    }

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
