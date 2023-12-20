use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use log::{error, info};
use tokio::task::JoinSet;

use metrics_utils::utils::setup_metrics;
use metrics_utils::{IngesterMetricsConfig, MetricState, MetricsTrait};
use nft_ingester::api::service::start_api;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{setup_config, BackfillerConfig, IngesterConfig, INGESTER_BACKUP_NAME};
use nft_ingester::db_v2::DBClient as DBClientV2;
use nft_ingester::index_syncronizer::Synchronizer;
use nft_ingester::init::graceful_stop;
use nft_ingester::json_downloader::JsonDownloader;
use nft_ingester::message_handler::MessageHandler;
use nft_ingester::mplx_updates_processor::MplxAccsProcessor;
use nft_ingester::tcp_receiver::TcpReceiver;
use nft_ingester::token_updates_processor::TokenAccsProcessor;
use nft_ingester::{config::init_logger, error::IngesterError};
use postgre_client::PgClient;
use rocks_db::backup_service::BackupService;
use rocks_db::errors::BackupServiceError;
use rocks_db::{backup_service, Storage};

mod backfiller;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting Ingester");

    init_logger();
    let config: IngesterConfig = setup_config();

    let mut metrics_state = MetricState::new(IngesterMetricsConfig::new());
    metrics_state.register_metrics();
    let metrics = Arc::new(metrics_state.metrics);

    if !config.get_is_snapshot() {
        let metrics_port = config.get_metrics_port(config.consumer_number)?;
        tokio::spawn(async move {
            match setup_metrics(metrics_state.registry, metrics_port).await {
                Ok(_) => {
                    info!("Setup metrics successfully")
                }
                Err(e) => {
                    error!("Setup metrics failed: {:?}", e)
                }
            }
        });
    }

    let mut tasks = JoinSet::new();

    // setup buffer
    let buffer = Arc::new(Buffer::new());

    let keep_running = Arc::new(AtomicBool::new(true));

    let cloned_buffer = buffer.clone();
    let cloned_keep_running = keep_running.clone();
    let cloned_metrics = metrics.clone();
    tasks.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            cloned_buffer.debug().await;
            cloned_buffer.capture_metrics(&cloned_metrics).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }));

    // start parsers
    let storage = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let bubblegum_updates_processor =
        BubblegumTxProcessor::new(rocks_storage.clone(), buffer.clone(), metrics.clone());

    let cloned_keep_running = keep_running.clone();
    tasks.spawn(tokio::spawn(async move {
        bubblegum_updates_processor.run(cloned_keep_running).await;
    }));

    let config: BackfillerConfig = setup_config();

    let backfiller = backfiller::Backfiller::new(rocks_storage.clone(), buffer.clone(), config)
        .await
        .unwrap();

    backfiller
        .start_backfill(&mut tasks, keep_running.clone())
        .await
        .unwrap();

    // --stop
    graceful_stop(tasks, true, keep_running.clone()).await;

    Ok(())
}
