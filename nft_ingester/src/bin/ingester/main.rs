use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use log::{error, info};
use nft_ingester::{backfiller, config, transaction_ingester};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use backfill_rpc::rpc::BackfillRPC;
use interface::signature_persistence::ProcessingDataGetter;
use metrics_utils::utils::start_metrics;
use metrics_utils::{
    ApiMetricsConfig, BackfillerMetricsConfig, IngesterMetricsConfig, JsonDownloaderMetricsConfig,
    JsonMigratorMetricsConfig, MetricState, MetricStatus, MetricsTrait, RpcBackfillerMetricsConfig,
    SynchronizerMetricsConfig,
};
use nft_ingester::api::service::start_api;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{
    setup_config, BackfillerConfig, IngesterConfig, INGESTER_BACKUP_NAME, INGESTER_CONFIG_PREFIX,
};
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
use rocks_db::storage_traits::AssetSlotStorage;
use rocks_db::{backup_service, Storage};
use tonic::transport::Server;

use nft_ingester::backfiller::{DirectBlockParser, TransactionsParser};

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting Ingester");
    let args = Args::parse();

    let ing_config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);
    init_logger(&ing_config.get_log_level());

    let mut guard = None;
    if ing_config.get_is_run_profiling() {
        guard = Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        );
    }

    let mut metrics_state = MetricState::new(
        IngesterMetricsConfig::new(),
        ApiMetricsConfig::new(),
        JsonDownloaderMetricsConfig::new(),
        BackfillerMetricsConfig::new(),
        RpcBackfillerMetricsConfig::new(),
        SynchronizerMetricsConfig::new(),
        JsonMigratorMetricsConfig::new(),
    );
    metrics_state.register_metrics();
    start_metrics(
        metrics_state.registry,
        ing_config.get_metrics_port(ing_config.consumer_number)?,
    )
    .await;

    let mut tasks = JoinSet::new();

    // setup buffer
    let buffer = Arc::new(Buffer::new());

    let keep_running = Arc::new(AtomicBool::new(true));

    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    // start parsers
    let storage = Storage::open(
        &ing_config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        mutexed_tasks.clone(),
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));
    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        backfill_bubblegum_updates_processor.clone(),
    ));

    if ing_config.run_bubblegum_backfiller {
        let config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);

        let big_table_client = Arc::new(
            backfiller::BigTableClient::connect_new_from_config(config.clone())
                .await
                .unwrap(),
        );
        let backfiller = backfiller::Backfiller::new(
            rocks_storage.clone(),
            big_table_client.clone(),
            config.clone(),
        );

        match config.backfiller_mode {
            config::BackfillerMode::IngestDirectly => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        keep_running.clone(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        big_table_client.clone(),
                    )
                    .await
                    .unwrap();
                info!("running backfiller directly from bigtable to ingester");
            }
            config::BackfillerMode::Persist | config::BackfillerMode::PersistAndIngest => {
                let consumer = rocks_storage.clone();
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        keep_running.clone(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        big_table_client.clone(),
                    )
                    .await
                    .unwrap();
                info!("running backfiller to persist raw data");
            }
            config::BackfillerMode::IngestPersisted => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                let producer = rocks_storage.clone();

                let transactions_parser = Arc::new(TransactionsParser::new(
                    rocks_storage.clone(),
                    consumer,
                    producer,
                    metrics_state.backfiller_metrics.clone(),
                    config.workers_count,
                    config.chunk_size,
                ));

                let cloned_keep_running = keep_running.clone();
                mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                    info!("Running transactions parser...");

                    transactions_parser
                        .parse_raw_transactions(
                            cloned_keep_running,
                            config.permitted_tasks,
                            config.slot_until,
                        )
                        .await;
                }));

                info!("running backfiller on persisted raw data");
            }
            config::BackfillerMode::PartiallyIngest => {
                info!("BackfillerMode is PartiallyIngest");

                let target_rocks = rocks_storage.clone();

                let source_rocks = Storage::open_as_secondary(
                    &ing_config.rocks_source_path.clone().unwrap(),
                    "./secondary",
                    mutexed_tasks.clone(),
                )
                .unwrap();
                let producer = Arc::new(source_rocks);

                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    target_rocks.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));

                if ing_config.read_blocks_from_file.unwrap() {
                    let transactions_parser = Arc::new(TransactionsParser::new(
                        target_rocks.clone(),
                        consumer,
                        producer,
                        metrics_state.backfiller_metrics.clone(),
                        config.workers_count,
                        config.chunk_size,
                    ));
    
                    let cloned_keep_running = keep_running.clone();
                    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                        info!("Running transactions parser...");
    
                        transactions_parser
                            .parse_concrete_blocks(
                                cloned_keep_running,
                                config.permitted_tasks,
                                config.slot_until,
                                &ing_config.blocks_file_path.clone().unwrap(),
                            )
                            .await;
                    }));
                } else {
                    backfiller
                        .start_backfill(
                            mutexed_tasks.clone(),
                            keep_running.clone(),
                            metrics_state.backfiller_metrics.clone(),
                            consumer,
                            producer.clone(),
                        )
                        .await
                        .unwrap();
                }
            }
            config::BackfillerMode::None => {
                info!("not running backfiller");
            }
        };
    }

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // --stop
    graceful_stop(
        mutexed_tasks,
        keep_running.clone(),
        shutdown_tx,
        guard,
        ing_config.profiling_file_path_container,
    )
    .await;

    Ok(())
}

async fn restore_rocksdb(config: &IngesterConfig) -> Result<(), BackupServiceError> {
    std::fs::create_dir_all(config.rocks_backup_archives_dir.as_str())?;
    let backup_path = format!(
        "{}/{}",
        config.rocks_backup_archives_dir, INGESTER_BACKUP_NAME
    );

    backup_service::download_backup_archive(config.rocks_backup_url.as_str(), backup_path.as_str())
        .await?;
    backup_service::unpack_backup_archive(
        backup_path.as_str(),
        config.rocks_backup_archives_dir.as_str(),
    )?;

    let unpacked_archive = format!(
        "{}/{}",
        config.rocks_backup_archives_dir,
        backup_service::get_backup_dir_name(config.rocks_backup_dir.as_str())
    );
    backup_service::restore_external_backup(
        unpacked_archive.as_str(),
        config
            .rocks_db_path_container
            .clone()
            .unwrap_or("./my_rocksdb".to_string())
            .as_str(),
    )?;

    // remove unpacked files
    std::fs::remove_dir_all(unpacked_archive)?;

    info!("restore_rocksdb fin");
    Ok(())
}
