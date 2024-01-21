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
    MetricState, MetricStatus, MetricsTrait, RpcBackfillerMetricsConfig, SynchronizerMetricsConfig,
};
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
use rocks_db::storage_traits::AssetSlotStorage;
use rocks_db::{backup_service, Storage};
use tonic::transport::Server;

use nft_ingester::backfiller::DirectBlockParser;

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

    let config: IngesterConfig = setup_config();
    init_logger(&config.get_log_level());

    let mut guard = None;
    if config.get_is_run_profiling() {
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
    );
    metrics_state.register_metrics();
    start_metrics(
        metrics_state.registry,
        config.get_metrics_port(config.consumer_number)?,
    )
    .await;

    // try to restore rocksDB first
    if args.restore_rocks_db {
        restore_rocksdb(&config).await?;
    }

    let db_client_v2 = Arc::new(DBClientV2::new(&config.database_config).await?);

    let mut tasks = JoinSet::new();

    // setup buffer
    let buffer = Arc::new(Buffer::new());

    // setup receiver
    let message_handler = Arc::new(MessageHandler::new(buffer.clone()));

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
    let keep_running = Arc::new(AtomicBool::new(true));
    let cloned_keep_running = keep_running.clone();

    tasks.spawn(tokio::spawn(async move {
        geyser_tcp_receiver
            .connect(geyser_addr, cloned_keep_running)
            .await
            .unwrap()
    }));
    let cloned_keep_running = keep_running.clone();
    tasks.spawn(tokio::spawn(async move {
        snapshot_tcp_receiver
            .connect(snapshot_addr, cloned_keep_running)
            .await
            .unwrap()
    }));

    let cloned_buffer = buffer.clone();
    let cloned_keep_running = keep_running.clone();
    let cloned_metrics = metrics_state.ingester_metrics.clone();
    tasks.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            cloned_buffer.debug().await;
            cloned_buffer.capture_metrics(&cloned_metrics).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }));

    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    // start parsers
    let storage = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        mutexed_tasks.clone(),
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);
    let newest_restored_slot = rocks_storage.last_saved_slot()?.unwrap_or(0);

    // start backup service
    let backup_cfg = backup_service::load_config()?;
    let mut backup_service = BackupService::new(rocks_storage.db.clone(), &backup_cfg)?;
    let cloned_metrics = metrics_state.ingester_metrics.clone();

    let cloned_keep_running = keep_running.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        backup_service.perform_backup(cloned_metrics, cloned_keep_running)
    }));

    let mplx_accs_parser = MplxAccsProcessor::new(
        config.mplx_buffer_size,
        buffer.clone(),
        db_client_v2.clone(),
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
    );

    let token_accs_parser = TokenAccsProcessor::new(
        rocks_storage.clone(),
        db_client_v2.clone(),
        buffer.clone(),
        metrics_state.ingester_metrics.clone(),
        config.spl_buffer_size,
    );

    for _ in 0..config.mplx_workers {
        let mut cloned_mplx_parser = mplx_accs_parser.clone();

        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_mplx_parser
                .process_metadata_accs(cloned_keep_running)
                .await;
        }));

        let mut cloned_token_parser = token_accs_parser.clone();

        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_token_parser
                .process_token_accs(cloned_keep_running)
                .await;
        }));

        let mut cloned_token_parser = token_accs_parser.clone();

        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_token_parser
                .process_mint_accs(cloned_keep_running)
                .await;
        }));
    }

    let first_processed_slot = Arc::new(AtomicU64::new(0));
    let first_processed_slot_clone = first_processed_slot.clone();
    let cloned_rocks_storage = rocks_storage.clone();
    let cloned_keep_running = keep_running.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            let slot = cloned_rocks_storage.last_saved_slot();

            match slot {
                Ok(slot) => {
                    if let Some(slot) = slot {
                        if slot != newest_restored_slot {
                            first_processed_slot_clone.store(slot, Ordering::SeqCst);
                            break;
                        }
                    }
                }
                Err(e) => {
                    // If error returned from DB - stop all services
                    error!("Error while getting last saved slot: {}", e);
                    cloned_keep_running.store(false, Ordering::SeqCst);
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }));

    let cloned_keep_running = keep_running.clone();
    let cloned_rocks_storage = rocks_storage.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        match start_api(
            cloned_rocks_storage.clone(),
            cloned_keep_running,
            metrics_state.api_metrics.clone(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("Start API: {}", e);
            }
        };
    }));

    let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));

    let cloned_keep_running = keep_running.clone();
    let buffer_clone = buffer.clone();
    let bubblegum_updates_processor_clone = bubblegum_updates_processor.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            if let Some(tx) = buffer_clone.get_processing_transaction().await {
                if let Err(e) = bubblegum_updates_processor_clone
                    .process_transaction(tx)
                    .await
                {
                    if e != IngesterError::NotImplemented {
                        error!("Background saver could not process received data: {}", e);
                    }
                }
            }
        }
    }));

    let json_downloader = JsonDownloader::new(
        rocks_storage.clone(),
        metrics_state.json_downloader_metrics.clone(),
    )
    .await;

    let cloned_keep_running = keep_running.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        json_downloader.run(cloned_keep_running).await;
    }));

    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        bubblegum_updates_processor.clone(),
    ));

    if config.run_bubblegum_backfiller {
        let config: BackfillerConfig = setup_config();

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
                    metrics_state.backfiller_metrics.clone(),
                ));
                let producer = rocks_storage.clone();
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        keep_running.clone(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        producer,
                    )
                    .await
                    .unwrap();
                info!("running backfiller on persisted raw data");
            }
            config::BackfillerMode::None => {
                info!("not running backfiller");
            }
        };
    }

    let max_postgre_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(100);

    let index_storage = Arc::new(
        PgClient::new(
            &config.database_config.get_database_url().unwrap(),
            &config.get_sql_log_level(),
            100,
            max_postgre_connections,
        )
        .await,
    );

    let synchronizer = Synchronizer::new(
        rocks_storage.clone(),
        index_storage.clone(),
        config.synchronizer_batch_size,
        metrics_state.synchronizer_metrics.clone(),
    );

    let cloned_keep_running = keep_running.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            let res = synchronizer
                .synchronize_asset_indexes(cloned_keep_running.clone())
                .await;
            match res {
                Ok(_) => {
                    info!("Synchronization finished successfully");
                }
                Err(e) => {
                    error!("Synchronization failed: {:?}", e);
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // setup dependencies for grpc server
    let uc = usecase::asset_streamer::AssetStreamer::new(
        config.peer_grpc_max_gap_slots,
        rocks_storage.clone(),
    );
    let serv = grpc::service::PeerGapFillerServiceImpl::new(Arc::new(uc));
    let addr = format!("0.0.0.0:{}", config.peer_grpc_port).parse()?;
    // Spawn the gRPC server task and add to JoinSet
    mutexed_tasks.lock().await.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(GapFillerServiceServer::new(serv))
            .serve_with_shutdown(addr, async {
                shutdown_rx.await.ok();
            })
            .await
        {
            eprintln!("Server error: {}", e);
        }
        Ok(())
    });

    let transactions_getter = Arc::new(BackfillRPC::connect(config.backfill_rpc_address));
    let signature_fetcher = usecase::signature_fetcher::SignatureFetcher::new(
        rocks_storage,
        transactions_getter,
        tx_ingester,
        metrics_state.rpc_backfiller_metrics.clone(),
    );
    let cloned_keep_running = keep_running.clone();

    let metrics_clone = metrics_state.rpc_backfiller_metrics.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        let program_id = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
        while cloned_keep_running.load(Ordering::SeqCst) {
            let res = signature_fetcher.fetch_signatures(program_id).await;
            match res {
                Ok(_) => {
                    metrics_clone
                        .inc_run_fetch_signatures("fetch_signatures", MetricStatus::SUCCESS);
                    info!(
                        "signatures sync finished successfully for program_id: {}",
                        program_id
                    );
                }
                Err(e) => {
                    metrics_clone
                        .inc_run_fetch_signatures("fetch_signatures", MetricStatus::FAILURE);
                    error!(
                        "signatures sync failed: {:?} for program_id: {}",
                        e, program_id
                    );
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    }));

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
