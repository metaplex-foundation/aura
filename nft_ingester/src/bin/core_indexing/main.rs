use std::sync::Arc;

use log::{error, info};
use metrics_utils::{MetricState, MetricsTrait};
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{init_logger, setup_config, IngesterConfig, INGESTER_CONFIG_PREFIX};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use nft_ingester::message_handler::MessageHandlerCoreIndexing;
use nft_ingester::mpl_core_fee_indexing_processor::MplCoreFeeProcessor;
use nft_ingester::tcp_receiver::TcpReceiver;
use postgre_client::PgClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

pub const PG_MIGRATIONS_PATH: &str = "./migrations";
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting raw backfill server...");

    let config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());

    let guard = if config.run_profiling.unwrap_or_default() {
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
    let index_storage = Arc::new(
        PgClient::new(
            &config.database_config.get_database_url().unwrap(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            DEFAULT_MAX_POSTGRES_CONNECTIONS,
            metrics_state.red_metrics.clone(),
        )
        .await?,
    );

    index_storage
        .run_migration(PG_MIGRATIONS_PATH)
        .await
        .map_err(IngesterError::SqlxError)?;

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

    let mpl_core_fee_parser = MplCoreFeeProcessor::build(
        index_storage.clone(),
        buffer.clone(),
        metrics_state.ingester_metrics.clone(),
        Arc::new(RpcClient::new(config.backfill_rpc_address)),
        mutexed_tasks.clone(),
        config.mpl_core_buffer_size,
    )
    .await?;

    for _ in 0..config.mplx_workers {
        let mut cloned_core_parser = mpl_core_fee_parser.clone();
        let cloned_rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(async move {
            cloned_core_parser.start_processing(cloned_rx).await;
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
