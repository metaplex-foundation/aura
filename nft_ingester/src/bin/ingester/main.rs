use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::FutureExt;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use log::{error, info, warn};
use nft_ingester::{backfiller, config, json_worker, transaction_ingester};
use rocks_db::bubblegum_slots::{BubblegumSlotGetter, IngestableSlotGetter};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_transaction_status::UiConfirmedBlock;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;
use tokio::time::Instant;

use backfill_rpc::rpc::BackfillRPC;
use grpc::client::Client;
use interface::error::{StorageError, UsecaseError};
use interface::signature_persistence::{BlockProducer, ProcessingDataGetter};
use metrics_utils::utils::start_metrics;
use metrics_utils::{BackfillerMetricsConfig, MetricState, MetricStatus, MetricsTrait};
use nft_ingester::api::service::start_api;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{
    setup_config, ApiConfig, BackfillerConfig, BackfillerSourceMode, IngesterConfig,
    INGESTER_BACKUP_NAME, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::index_syncronizer::Synchronizer;
use nft_ingester::init::graceful_stop;
use nft_ingester::json_worker::JsonWorker;
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

use nft_ingester::backfiller::{
    connect_new_bigtable_from_config, DirectBlockParser, ForceReingestableSlotGetter,
    TransactionsParser,
};
use nft_ingester::fork_cleaner::ForkCleaner;
use nft_ingester::gapfiller::process_asset_details_stream;
use nft_ingester::mpl_core_processor::MplCoreProcessor;
use nft_ingester::sequence_consistent::SequenceConsistentGapfiller;
use usecase::bigtable::BigTableClient;
use usecase::proofs::MaybeProofChecker;
use usecase::slots_collector::{SlotsCollector, SlotsGetter};

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const PG_MIGRATIONS_PATH: &str = "./migrations";
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

enum BackfillSource {
    Bigtable(Arc<BigTableClient>),
    Rpc(Arc<BackfillRPC>),
}

impl BackfillSource {
    async fn new(ingester_config: &IngesterConfig, backfiller_config: &BackfillerConfig) -> Self {
        match ingester_config.backfiller_source_mode {
            BackfillerSourceMode::Bigtable => Self::Bigtable(Arc::new(
                connect_new_bigtable_from_config(backfiller_config.clone())
                    .await
                    .unwrap(),
            )),
            BackfillerSourceMode::RPC => Self::Rpc(Arc::new(BackfillRPC::connect(
                ingester_config.backfill_rpc_address.clone(),
            ))),
        }
    }
}

#[async_trait]
impl SlotsGetter for BackfillSource {
    async fn get_slots(
        &self,
        collected_key: &Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        match self {
            BackfillSource::Bigtable(bigtable) => {
                bigtable
                    .big_table_inner_client
                    .get_slots(collected_key, start_at, rows_limit)
                    .await
            }
            BackfillSource::Rpc(rpc) => rpc.get_slots(collected_key, start_at, rows_limit).await,
        }
    }
}

#[async_trait]
impl BlockProducer for BackfillSource {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<UiConfirmedBlock, StorageError> {
        match self {
            BackfillSource::Bigtable(bigtable) => bigtable.get_block(slot, backup_provider).await,
            BackfillSource::Rpc(rpc) => rpc.get_block(slot, backup_provider).await,
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting Ingester");
    let args = Args::parse();

    let config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());

    let guard = if config.get_is_run_profiling() {
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    // try to restore rocksDB first
    if args.restore_rocks_db {
        restore_rocksdb(&config).await?;
    }

    let max_postgre_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(DEFAULT_MAX_POSTGRES_CONNECTIONS);

    let index_storage = Arc::new(
        PgClient::new(
            &config.database_config.get_database_url().unwrap(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            max_postgre_connections,
            metrics_state.red_metrics.clone(),
        )
        .await?,
    );

    index_storage
        .run_migration(PG_MIGRATIONS_PATH)
        .await
        .map_err(IngesterError::SqlxError)?;

    let tasks = JoinSet::new();

    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    // start parsers
    let storage = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        mutexed_tasks.clone(),
        metrics_state.red_metrics.clone(),
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);
    let last_saved_slot = rocks_storage.last_saved_slot()?.unwrap_or_default();
    let synchronizer = Synchronizer::new(
        rocks_storage.clone(),
        index_storage.clone(),
        index_storage.clone(),
        config.dump_synchronizer_batch_size,
        config.dump_path.to_string(),
        metrics_state.synchronizer_metrics.clone(),
        config.synchronizer_parallel_tasks,
        config.run_temp_sync_during_dump,
    );
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    if config.run_dump_synchronize_on_start {
        tracing::info!("Running dump synchronizer on start");
        synchronizer
            .full_syncronize(&shutdown_rx.resubscribe())
            .await
            .unwrap();
    }
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

    mutexed_tasks.lock().await.spawn(async move {
        let geyser_tcp_receiver = Arc::new(geyser_tcp_receiver);
        while cloned_keep_running.load(Ordering::SeqCst) {
            let geyser_tcp_receiver_clone = geyser_tcp_receiver.clone();
            let cloned_keep_running = cloned_keep_running.clone();
            if let Err(e) = tokio::spawn(async move {
                geyser_tcp_receiver_clone
                    .connect(geyser_addr, cloned_keep_running)
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
    let cloned_keep_running = keep_running.clone();
    mutexed_tasks.lock().await.spawn(async move {
        let snapshot_tcp_receiver = Arc::new(snapshot_tcp_receiver);
        while cloned_keep_running.load(Ordering::SeqCst) {
            let snapshot_tcp_receiver_clone = snapshot_tcp_receiver.clone();
            let cloned_keep_running = cloned_keep_running.clone();
            if let Err(e) = tokio::spawn(async move {
                snapshot_tcp_receiver_clone
                    .connect(snapshot_addr, cloned_keep_running)
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

    let cloned_buffer = buffer.clone();
    let cloned_keep_running = keep_running.clone();
    let cloned_metrics = metrics_state.ingester_metrics.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            cloned_buffer.debug().await;
            cloned_buffer.capture_metrics(&cloned_metrics).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }));

    // start backup service
    let backup_cfg = backup_service::load_config()?;
    let mut backup_service = BackupService::new(rocks_storage.db.clone(), &backup_cfg)?;
    let cloned_metrics = metrics_state.ingester_metrics.clone();

    if config.store_db_backups() {
        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            backup_service.perform_backup(cloned_metrics, cloned_keep_running)
        }));
    }

    let mplx_accs_parser = MplxAccsProcessor::new(
        config.mplx_buffer_size,
        buffer.clone(),
        index_storage.clone(),
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
    );

    let token_accs_parser = TokenAccsProcessor::new(
        rocks_storage.clone(),
        buffer.clone(),
        metrics_state.ingester_metrics.clone(),
        config.spl_buffer_size,
    );
    let mpl_core_parser = MplCoreProcessor::new(
        rocks_storage.clone(),
        index_storage.clone(),
        buffer.clone(),
        metrics_state.ingester_metrics.clone(),
        config.mpl_core_buffer_size,
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
        let mut cloned_mplx_parser = mplx_accs_parser.clone();
        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_mplx_parser
                .process_burnt_accs(cloned_keep_running)
                .await;
        }));

        let mut cloned_token_parser = token_accs_parser.clone();

        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_token_parser
                .process_mint_accs(cloned_keep_running)
                .await;
        }));

        let mut cloned_mplx_parser = mplx_accs_parser.clone();
        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_mplx_parser
                .process_edition_accs(cloned_keep_running)
                .await;
        }));

        let mut cloned_core_parser = mpl_core_parser.clone();
        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_core_parser
                .process_mpl_assets(cloned_keep_running)
                .await;
        }));

        let mut cloned_core_parser = mpl_core_parser.clone();
        let cloned_keep_running = keep_running.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            cloned_core_parser
                .process_mpl_asset_burn(cloned_keep_running)
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
                        if slot != last_saved_slot {
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

    let json_processor = Arc::new(
        JsonWorker::new(
            index_storage.clone(),
            rocks_storage.clone(),
            metrics_state.json_downloader_metrics.clone(),
        )
        .await,
    );

    match Client::connect(config.clone()).await {
        Ok(gaped_data_client) => {
            while first_processed_slot.load(Ordering::SeqCst) == 0
                && keep_running.load(Ordering::SeqCst)
            {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            if keep_running.load(Ordering::SeqCst) {
                let cloned_keep_running = keep_running.clone();
                let cloned_rocks_storage = rocks_storage.clone();
                mutexed_tasks.lock().await.spawn(async move {
                    info!(
                        "Processed {} gaped assets",
                        process_asset_details_stream(
                            cloned_keep_running,
                            cloned_rocks_storage,
                            last_saved_slot,
                            first_processed_slot.load(Ordering::SeqCst),
                            gaped_data_client,
                        )
                        .await
                    );
                    Ok(())
                });
            }
        }
        Err(e) => error!("GRPC Client new: {}", e),
    };

    let cloned_keep_running = keep_running.clone();
    let cloned_rocks_storage = rocks_storage.clone();
    let cloned_api_metrics = metrics_state.api_metrics.clone();
    let proof_checker = config.rpc_host.clone().map(|host| {
        Arc::new(MaybeProofChecker::new(
            Arc::new(RpcClient::new(host)),
            config.check_proofs_probability,
            config.check_proofs_commitment,
        ))
    });
    let tasks_clone = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();

    let middleware_json_downloader = config
        .json_middleware_config
        .as_ref()
        .filter(|conf| conf.is_enabled)
        .map(|_| json_processor.clone());

    let api_config: ApiConfig = setup_config(INGESTER_CONFIG_PREFIX);

    let cloned_index_storage = index_storage.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        match start_api(
            cloned_index_storage,
            cloned_rocks_storage.clone(),
            cloned_rx,
            cloned_api_metrics,
            api_config.server_port,
            proof_checker,
            api_config.max_page_limit,
            middleware_json_downloader.clone(),
            middleware_json_downloader,
            api_config.json_middleware_config,
            tasks_clone,
            &api_config.archives_dir,
            api_config.consistence_synchronization_api_threshold,
            api_config.consistence_backfilling_slots_threshold,
            api_config.batch_mint_service_port,
            api_config.file_storage_path_container.as_str(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("Start API: {}", e);
            }
        };
    }));

    let geyser_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));

    let cloned_keep_running = keep_running.clone();
    let buffer_clone = buffer.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            if let Some(tx) = buffer_clone.get_processing_transaction().await {
                if let Err(e) = geyser_bubblegum_updates_processor
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

    let cloned_keep_running = keep_running.clone();
    let cloned_js = json_processor.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        json_worker::run(cloned_js, cloned_keep_running).await;
    }));

    let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));
    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        backfill_bubblegum_updates_processor.clone(),
    ));
    let backfiller_config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
    let backfiller_source = Arc::new(BackfillSource::new(&config, &backfiller_config).await);
    let backfiller = Arc::new(backfiller::Backfiller::new(
        rocks_storage.clone(),
        backfiller_source.clone(),
        backfiller_config.clone(),
    ));

    let rpc_backfiller = Arc::new(BackfillRPC::connect(config.backfill_rpc_address.clone()));
    if config.run_bubblegum_backfiller {
        if backfiller_config.should_reingest {
            warn!("reingest flag is set, deleting last fetched slot");
            rocks_storage
                .delete_parameter::<u64>(rocks_db::parameters::Parameter::LastFetchedSlot)
                .await?;
        }

        match backfiller_config.backfiller_mode {
            config::BackfillerMode::IngestDirectly => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        shutdown_rx.resubscribe(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        backfiller_source.clone(),
                    )
                    .await
                    .unwrap();
                info!("running backfiller directly from bigtable to ingester");
            }
            config::BackfillerMode::Persist => {
                let consumer = rocks_storage.clone();
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        shutdown_rx.resubscribe(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        backfiller_source.clone(),
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
                    Arc::new(BubblegumSlotGetter::new(rocks_storage.clone())),
                    consumer,
                    producer,
                    metrics_state.backfiller_metrics.clone(),
                    backfiller_config.workers_count,
                    backfiller_config.chunk_size,
                ));

                let cloned_rx = shutdown_rx.resubscribe();
                mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                    info!("Running transactions parser...");

                    transactions_parser
                        .parse_raw_transactions(
                            cloned_rx,
                            backfiller_config.permitted_tasks,
                            backfiller_config.slot_until,
                        )
                        .await;
                }));

                info!("running backfiller on persisted raw data");
            }
            config::BackfillerMode::PersistAndIngest => {
                let rx = shutdown_rx.resubscribe();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_fetcher_");
                let backfiller_clone = backfiller.clone();
                let rpc_backfiller_clone = rpc_backfiller.clone();
                mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                    info!("Running slot fetcher...");
                    if let Err(e) = backfiller_clone
                        .run_perpetual_slot_collection(
                            metrics,
                            Duration::from_secs(backfiller_config.wait_period_sec),
                            rpc_backfiller_clone,
                            rx,
                        )
                        .await
                    {
                        error!("Error while running perpetual slot fetcher: {}", e);
                    }
                    info!("Slot fetcher finished working");
                }));

                // run perpetual slot persister
                let rx = shutdown_rx.resubscribe();
                let consumer = rocks_storage.clone();
                let producer = backfiller_source.clone();
                let metrics: Arc<BackfillerMetricsConfig> =
                    Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_persister_");
                let slot_getter = Arc::new(BubblegumSlotGetter::new(rocks_storage.clone()));
                let backfiller_clone = backfiller.clone();
                mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                    info!("Running slot persister...");
                    let none: Option<Arc<Storage>> = None;
                    if let Err(e) = backfiller_clone
                        .run_perpetual_slot_processing(
                            metrics,
                            slot_getter,
                            consumer,
                            producer,
                            Duration::from_secs(backfiller_config.wait_period_sec),
                            rx,
                            none,
                        )
                        .await
                    {
                        error!("Error while running perpetual slot persister: {}", e);
                    }
                    info!("Slot persister finished working");
                }));
                // run perpetual ingester
                let rx = shutdown_rx.resubscribe();
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                let producer = rocks_storage.clone();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_ingester_");
                let slot_getter = Arc::new(IngestableSlotGetter::new(rocks_storage.clone()));
                let backfiller_clone = backfiller.clone();
                let backup = backfiller_source.clone();
                mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
                    info!("Running slot ingester...");
                    if let Err(e) = backfiller_clone
                        .run_perpetual_slot_processing(
                            metrics,
                            slot_getter,
                            consumer,
                            producer,
                            Duration::from_secs(backfiller_config.wait_period_sec),
                            rx,
                            Some(backup),
                        )
                        .await
                    {
                        error!("Error while running perpetual slot ingester: {}", e);
                    }
                    info!("Slot ingester finished working");
                }));
            }
            config::BackfillerMode::None => {
                info!("not running backfiller");
            }
        };
    }

    if !config.disable_synchronizer {
        let rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            synchronizer
                .run(
                    &rx,
                    config.dump_sync_threshold,
                    tokio::time::Duration::from_secs(5),
                )
                .await;
        }));
    }
    // setup dependencies for grpc server
    let uc = usecase::asset_streamer::AssetStreamer::new(
        config.peer_grpc_max_gap_slots,
        rocks_storage.clone(),
    );
    let serv = grpc::service::PeerGapFillerServiceImpl::new(Arc::new(uc));
    let addr = format!("0.0.0.0:{}", config.peer_grpc_port).parse()?;
    // Spawn the gRPC server task and add to JoinSet
    let mut rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(GapFillerServiceServer::new(serv))
            .serve_with_shutdown(addr, rx.recv().map(|_| ()))
            .await
        {
            eprintln!("Server error: {}", e);
        }
        Ok(())
    });

    let rocks_clone = rocks_storage.clone();
    let signature_fetcher = usecase::signature_fetcher::SignatureFetcher::new(
        rocks_clone,
        rpc_backfiller.clone(),
        tx_ingester.clone(),
        metrics_state.rpc_backfiller_metrics.clone(),
    );
    let cloned_keep_running = keep_running.clone();

    let metrics_clone = metrics_state.rpc_backfiller_metrics.clone();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        let program_id = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
        while cloned_keep_running.load(Ordering::SeqCst) {
            let res = signature_fetcher
                .fetch_signatures(program_id, config.rpc_retry_interval_millis)
                .await;
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
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }));

    if config.run_sequence_consistent_checker {
        let force_reingestable_slot_processor = Arc::new(ForceReingestableSlotGetter::new(
            rocks_storage.clone(),
            Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                rocks_storage.clone(),
                metrics_state.backfiller_metrics.clone(),
            )),
        ));

        let slots_collector = SlotsCollector::new(
            force_reingestable_slot_processor.clone(),
            backfiller_source.clone(),
            metrics_state.backfiller_metrics.clone(),
        );
        let sequence_consistent_gapfiller = SequenceConsistentGapfiller::new(
            rocks_storage.clone(),
            slots_collector,
            metrics_state.sequence_consistent_gapfill_metrics.clone(),
            rpc_backfiller.clone(),
        );
        let mut rx = shutdown_rx.resubscribe();
        let metrics = metrics_state.sequence_consistent_gapfill_metrics.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Start collecting sequences gaps...");
            loop {
                let start = Instant::now();
                sequence_consistent_gapfiller
                    .collect_sequences_gaps(rx.resubscribe())
                    .await;
                metrics.set_scans_latency(start.elapsed().as_secs_f64());
                metrics.inc_total_scans();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.sequence_consistent_checker_wait_period_sec)) => {},
                    _ = rx.recv() => {
                        info!("Received stop signal, stopping collecting sequences gaps");
                        return;
                    }
                };
            }
        }));

        // run an additional direct slot persister
        let rx = shutdown_rx.resubscribe();
        let producer = backfiller_source.clone();
        let metrics = Arc::new(BackfillerMetricsConfig::new());
        metrics.register_with_prefix(&mut metrics_state.registry, "force_slot_persister_");

        let transactions_parser = Arc::new(TransactionsParser::new(
            rocks_storage.clone(),
            force_reingestable_slot_processor.clone(),
            force_reingestable_slot_processor.clone(),
            producer.clone(),
            metrics.clone(),
            backfiller_config.workers_count,
            backfiller_config.chunk_size,
        ));

        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Running slot force persister...");
            transactions_parser.parse_transactions(rx).await;
            info!("Force slot persister finished working");
        }));

        let fork_cleaner = ForkCleaner::new(
            rocks_storage.clone(),
            rocks_storage.clone(),
            metrics_state.fork_cleaner_metrics.clone(),
        );
        let mut rx = shutdown_rx.resubscribe();
        let metrics = metrics_state.fork_cleaner_metrics.clone();
        mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Start cleaning forks...");
            loop {
                let start = Instant::now();
                fork_cleaner
                    .clean_forks(rx.resubscribe())
                    .await;
                metrics.set_scans_latency(start.elapsed().as_secs_f64());
                metrics.inc_total_scans();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.sequence_consistent_checker_wait_period_sec)) => {},
                    _ = rx.recv() => {
                        info!("Received stop signal, stopping cleaning forks");
                        return;
                    }
                };
            }
        }));
    }

    start_metrics(
        metrics_state.registry,
        config.get_metrics_port(config.consumer_number)?,
    )
    .await;

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
