use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::Arweave;
use async_trait::async_trait;
use nft_ingester::batch_mint::batch_mint_persister::{self, BatchMintPersister};
use nft_ingester::scheduler::Scheduler;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::FutureExt;
use grpc::asseturls::asset_url_service_server::AssetUrlServiceServer;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use nft_ingester::{backfiller, config, json_worker, transaction_ingester};
use plerkle_messenger::ConsumptionType;
use rocks_db::bubblegum_slots::{BubblegumSlotGetter, IngestableSlotGetter};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_transaction_status::UiConfirmedBlock;
use tempfile::TempDir;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, Mutex};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{error, info, warn};

use backfill_rpc::rpc::BackfillRPC;
use grpc::client::Client;
use interface::error::{StorageError, UsecaseError};
use interface::signature_persistence::{BlockProducer, UnprocessedTransactionsGetter};
use interface::slots_dumper::SlotsDumper;
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use metrics_utils::utils::start_metrics;
use metrics_utils::{
    BackfillerMetricsConfig, IngesterMetricsConfig, MetricState, MetricStatus, MetricsTrait,
    SequenceConsistentGapfillMetricsConfig,
};
use nft_ingester::accounts_processor::AccountsProcessor;
use nft_ingester::ack::create_ack_channel;
use nft_ingester::api::account_balance::AccountBalanceGetterImpl;
use nft_ingester::api::service::start_api;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{
    setup_config, ApiConfig, BackfillerConfig, BackfillerSourceMode, IngesterConfig, MessageSource,
    INGESTER_BACKUP_NAME, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::index_syncronizer::Synchronizer;
use nft_ingester::init::graceful_stop;
use nft_ingester::json_worker::JsonWorker;
use nft_ingester::message_handler::MessageHandlerIngester;
use nft_ingester::tcp_receiver::TcpReceiver;
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
use nft_ingester::batch_mint::batch_mint_processor::{BatchMintProcessor, NoopBatchMintTxSender};
use nft_ingester::fork_cleaner::ForkCleaner;
use nft_ingester::gapfiller::{process_asset_details_stream, process_raw_blocks_stream};
use nft_ingester::price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater};
use nft_ingester::redis_receiver::RedisReceiver;
use nft_ingester::sequence_consistent::SequenceConsistentGapfiller;
use rocks_db::migrator::MigrationState;
use usecase::bigtable::BigTableClient;
use usecase::proofs::MaybeProofChecker;
use usecase::slots_collector::{SlotsCollector, SlotsGetter};

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const PG_MIGRATIONS_PATH: &str = "./migrations";
pub const ARWEAVE_WALLET_PATH: &str = "./arweave_wallet.json";
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
    // start parsers
    let storage = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
        mutexed_tasks.clone(),
        metrics_state.red_metrics.clone(),
        MigrationState::Last,
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
    let message_handler = Arc::new(MessageHandlerIngester::new(buffer.clone()));

    let geyser_tcp_receiver = TcpReceiver::new(
        message_handler.clone(),
        config.tcp_config.get_tcp_receiver_reconnect_interval()?,
    );
    // For now there is no snapshot mechanism via Redis so we use snapshot_tcp_receiver for this purpose
    let snapshot_tcp_receiver = TcpReceiver::new(
        message_handler.clone(),
        config.tcp_config.get_tcp_receiver_reconnect_interval()? * 2,
    );

    let cloned_rx = shutdown_rx.resubscribe();
    let ack_channel = create_ack_channel(
        cloned_rx,
        config.redis_messenger_config.clone(),
        mutexed_tasks.clone(),
    )
    .await;

    let snapshot_addr = config.tcp_config.get_snapshot_addr_ingester()?;
    let geyser_addr = config.tcp_config.get_tcp_receiver_addr_ingester()?;

    let cloned_rx = shutdown_rx.resubscribe();
    if matches!(config.message_source, MessageSource::TCP) {
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
    }

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

    let cloned_buffer = buffer.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_metrics = metrics_state.ingester_metrics.clone();
    mutexed_tasks.lock().await.spawn(async move {
        while cloned_rx.is_empty() {
            cloned_buffer.debug().await;
            cloned_buffer.capture_metrics(&cloned_metrics).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    });

    // start backup service
    let backup_cfg = backup_service::load_config()?;
    let mut backup_service = BackupService::new(rocks_storage.db.clone(), &backup_cfg)?;
    let cloned_metrics = metrics_state.ingester_metrics.clone();

    if config.store_db_backups() {
        let cloned_rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(async move {
            backup_service
                .perform_backup(cloned_metrics, cloned_rx)
                .await;
            Ok(())
        });
    }

    let rpc_client = Arc::new(RpcClient::new(config.rpc_host.clone()));
    for _ in 0..config.parsing_workers {
        match config.message_source {
            MessageSource::Redis => {
                let redis_receiver = Arc::new(
                    RedisReceiver::new(
                        config.redis_messenger_config.clone(),
                        ConsumptionType::All,
                        ack_channel.clone(),
                    )
                    .await
                    .unwrap(),
                );
                run_accounts_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    redis_receiver,
                    rocks_storage.clone(),
                    config.accounts_buffer_size,
                    config.mpl_core_fees_buffer_size,
                    metrics_state.ingester_metrics.clone(),
                    index_storage.clone(),
                    rpc_client.clone(),
                    mutexed_tasks.clone(),
                )
                .await;
            }
            MessageSource::TCP => {
                run_accounts_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    buffer.clone(),
                    rocks_storage.clone(),
                    config.accounts_buffer_size,
                    config.mpl_core_fees_buffer_size,
                    metrics_state.ingester_metrics.clone(),
                    index_storage.clone(),
                    rpc_client.clone(),
                    mutexed_tasks.clone(),
                )
                .await;
            }
        }
    }
    // Workers for snapshot parsing
    for _ in 0..config.snapshot_parsing_workers {
        run_accounts_processor(
            shutdown_rx.resubscribe(),
            mutexed_tasks.clone(),
            buffer.clone(),
            rocks_storage.clone(),
            config.snapshot_parsing_batch_size,
            config.mpl_core_fees_buffer_size,
            metrics_state.ingester_metrics.clone(),
            index_storage.clone(),
            rpc_client.clone(),
            mutexed_tasks.clone(),
        )
        .await;
    }

    let first_processed_slot = Arc::new(AtomicU64::new(0));
    let first_processed_slot_clone = first_processed_slot.clone();
    let cloned_rocks_storage = rocks_storage.clone();

    let cloned_rx = shutdown_rx.resubscribe();

    let cloned_tx = shutdown_tx.clone();

    mutexed_tasks.lock().await.spawn(async move {
        while cloned_rx.is_empty() {
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
                    let _ = cloned_tx.send(());
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    });

    let json_processor = Arc::new(
        JsonWorker::new(
            index_storage.clone(),
            rocks_storage.clone(),
            metrics_state.json_downloader_metrics.clone(),
        )
        .await,
    );
    let grpc_client = match Client::connect(config.clone()).await {
        Ok(client) => Some(client),
        Err(e) => {
            error!("GRPC Client new: {}", e);
            None
        }
    };
    if let Some(gaped_data_client) = grpc_client.clone() {
        while first_processed_slot.load(Ordering::SeqCst) == 0 && shutdown_rx.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await
        }
        let cloned_rocks_storage = rocks_storage.clone();
        if shutdown_rx.is_empty() {
            let gaped_data_client_clone = gaped_data_client.clone();
            let first_processed_slot_value = first_processed_slot.load(Ordering::SeqCst);
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks.lock().await.spawn(async move {
                let processed_assets = process_asset_details_stream(
                    cloned_rx,
                    cloned_rocks_storage.clone(),
                    last_saved_slot,
                    first_processed_slot_value,
                    gaped_data_client_clone,
                )
                .await;
                info!("Processed {} gaped assets", processed_assets);
                Ok(())
            });
            let cloned_rocks_storage = rocks_storage.clone();
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks.lock().await.spawn(async move {
                let processed_raw_blocks = process_raw_blocks_stream(
                    cloned_rx,
                    cloned_rocks_storage,
                    last_saved_slot,
                    first_processed_slot_value,
                    gaped_data_client,
                )
                .await;
                info!("Processed {} raw blocks", processed_raw_blocks);
                Ok(())
            });
        }
    };

    let cloned_rocks_storage = rocks_storage.clone();
    let cloned_api_metrics = metrics_state.api_metrics.clone();
    let account_balance_getter = Arc::new(AccountBalanceGetterImpl::new(rpc_client.clone()));
    let mut proof_checker = None;
    if config.check_proofs {
        proof_checker = Some(Arc::new(MaybeProofChecker::new(
            rpc_client.clone(),
            config.check_proofs_probability,
            config.check_proofs_commitment,
        )))
    }
    let tasks_clone = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();

    let middleware_json_downloader = config
        .json_middleware_config
        .as_ref()
        .filter(|conf| conf.is_enabled)
        .map(|_| json_processor.clone());

    let api_config: ApiConfig = setup_config(INGESTER_CONFIG_PREFIX);

    let cloned_index_storage = index_storage.clone();
    let file_storage_path = api_config.file_storage_path_container.clone();
    mutexed_tasks.lock().await.spawn(async move {
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
            account_balance_getter,
            api_config.storage_service_base_url,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Start API: {}", e);
                Ok(())
            }
        }
    });

    let geyser_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));

    for _ in 0..config.parsing_workers {
        match config.message_source {
            MessageSource::Redis => {
                let redis_receiver = Arc::new(
                    RedisReceiver::new(
                        config.redis_messenger_config.clone(),
                        ConsumptionType::All,
                        ack_channel.clone(),
                    )
                    .await
                    .unwrap(),
                );
                run_transaction_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    redis_receiver,
                    geyser_bubblegum_updates_processor.clone(),
                )
                .await;
            }
            MessageSource::TCP => {
                run_transaction_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    buffer.clone(),
                    geyser_bubblegum_updates_processor.clone(),
                )
                .await;
            }
        }
    }

    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_js = json_processor.clone();
    mutexed_tasks.lock().await.spawn(async move {
        json_worker::run(cloned_js, cloned_rx).await;
        Ok(())
    });

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
                mutexed_tasks.lock().await.spawn(async move {
                    info!("Running transactions parser...");

                    transactions_parser
                        .parse_raw_transactions(
                            cloned_rx,
                            backfiller_config.permitted_tasks,
                            backfiller_config.slot_until,
                        )
                        .await;

                    Ok(())
                });

                info!("running backfiller on persisted raw data");
            }
            config::BackfillerMode::PersistAndIngest => {
                let rx = shutdown_rx.resubscribe();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_fetcher_");
                let backfiller_clone = backfiller.clone();
                let rpc_backfiller_clone = rpc_backfiller.clone();
                mutexed_tasks.lock().await.spawn(async move {
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

                    Ok(())
                });

                // run perpetual slot persister
                let rx = shutdown_rx.resubscribe();
                let consumer = rocks_storage.clone();
                let producer = backfiller_source.clone();
                let metrics: Arc<BackfillerMetricsConfig> =
                    Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_persister_");
                let slot_getter = Arc::new(BubblegumSlotGetter::new(rocks_storage.clone()));
                let backfiller_clone = backfiller.clone();
                mutexed_tasks.lock().await.spawn(async move {
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

                    Ok(())
                });
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
                mutexed_tasks.lock().await.spawn(async move {
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

                    Ok(())
                });
            }
            config::BackfillerMode::None => {
                info!("not running backfiller");
            }
        };
    }

    if !config.disable_synchronizer {
        let rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(async move {
            synchronizer
                .run(
                    &rx,
                    config.dump_sync_threshold,
                    tokio::time::Duration::from_secs(5),
                )
                .await;

            Ok(())
        });
    }
    // setup dependencies for grpc server
    let uc = usecase::asset_streamer::AssetStreamer::new(
        config.peer_grpc_max_gap_slots,
        rocks_storage.clone(),
    );
    let bs = usecase::raw_blocks_streamer::BlocksStreamer::new(
        config.peer_grpc_max_gap_slots,
        rocks_storage.clone(),
    );
    let serv = grpc::service::PeerGapFillerServiceImpl::new(
        Arc::new(uc),
        Arc::new(bs),
        rocks_storage.clone(),
    );
    let asset_url_serv = grpc::asseturls_impl::AssetUrlServiceImpl::new(rocks_storage.clone());
    let addr = format!("0.0.0.0:{}", config.peer_grpc_port).parse()?;
    // Spawn the gRPC server task and add to JoinSet
    let mut rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(GapFillerServiceServer::new(serv))
            .add_service(AssetUrlServiceServer::new(asset_url_serv))
            .serve_with_shutdown(addr, rx.recv().map(|_| ()))
            .await
        {
            eprintln!("Server error: {}", e);
        }
        Ok(())
    });

    Scheduler::run_in_background(Scheduler::new(rocks_storage.clone())).await;

    let rocks_clone = rocks_storage.clone();
    let signature_fetcher = usecase::signature_fetcher::SignatureFetcher::new(
        rocks_clone,
        rpc_backfiller.clone(),
        tx_ingester.clone(),
        metrics_state.rpc_backfiller_metrics.clone(),
    );

    let cloned_rx = shutdown_rx.resubscribe();

    let metrics_clone = metrics_state.rpc_backfiller_metrics.clone();
    mutexed_tasks.lock().await.spawn(async move {
        let program_id = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
        while cloned_rx.is_empty() {
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

        Ok(())
    });

    if config.run_sequence_consistent_checker {
        let force_reingestable_slot_processor = Arc::new(ForceReingestableSlotGetter::new(
            rocks_storage.clone(),
            Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                rocks_storage.clone(),
                metrics_state.backfiller_metrics.clone(),
            )),
        ));
        run_sequence_consistent_gapfiller(
            SlotsCollector::new(
                force_reingestable_slot_processor.clone(),
                backfiller_source.clone(),
                metrics_state.backfiller_metrics.clone(),
            ),
            rocks_storage.clone(),
            metrics_state.sequence_consistent_gapfill_metrics.clone(),
            shutdown_rx.resubscribe(),
            rpc_backfiller.clone(),
            mutexed_tasks.clone(),
            config.sequence_consistent_checker_wait_period_sec,
        )
        .await;

        // run an additional direct slot persister
        let rx = shutdown_rx.resubscribe();
        let producer = backfiller_source.clone();
        let metrics = Arc::new(BackfillerMetricsConfig::new());
        metrics.register_with_prefix(&mut metrics_state.registry, "force_slot_persister_");
        if let Some(client) = grpc_client {
            let force_reingestable_transactions_parser = Arc::new(TransactionsParser::new(
                rocks_storage.clone(),
                force_reingestable_slot_processor.clone(),
                force_reingestable_slot_processor.clone(),
                Arc::new(client),
                metrics.clone(),
                backfiller_config.workers_count,
                backfiller_config.chunk_size,
            ));
            mutexed_tasks.lock().await.spawn(async move {
                info!("Running slot force persister...");
                force_reingestable_transactions_parser
                    .parse_transactions(rx)
                    .await;
                info!("Force slot persister finished working");

                Ok(())
            });
        } else {
            let force_reingestable_transactions_parser = Arc::new(TransactionsParser::new(
                rocks_storage.clone(),
                force_reingestable_slot_processor.clone(),
                force_reingestable_slot_processor.clone(),
                producer.clone(),
                metrics.clone(),
                backfiller_config.workers_count,
                backfiller_config.chunk_size,
            ));
            mutexed_tasks.lock().await.spawn(async move {
                info!("Running slot force persister...");
                force_reingestable_transactions_parser
                    .parse_transactions(rx)
                    .await;
                info!("Force slot persister finished working");

                Ok(())
            });
        }

        let fork_cleaner = ForkCleaner::new(
            rocks_storage.clone(),
            rocks_storage.clone(),
            metrics_state.fork_cleaner_metrics.clone(),
        );
        let mut rx = shutdown_rx.resubscribe();
        let metrics = metrics_state.fork_cleaner_metrics.clone();
        mutexed_tasks.lock().await.spawn(async move {
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
                        break;
                    }
                };
            }

            Ok(())
        });
    }

    if let Ok(arweave) = Arweave::from_keypair_path(
        PathBuf::from_str(ARWEAVE_WALLET_PATH).unwrap(),
        ARWEAVE_BASE_URL.parse().unwrap(),
    ) {
        let arweave = Arc::new(arweave);
        let batch_mint_processor = Arc::new(BatchMintProcessor::new(
            index_storage.clone(),
            rocks_storage.clone(),
            Arc::new(NoopBatchMintTxSender {}),
            arweave,
            file_storage_path,
            metrics_state.batch_mint_processor_metrics.clone(),
        ));
        let rx = shutdown_rx.resubscribe();
        let processor_clone = batch_mint_processor.clone();
        mutexed_tasks.lock().await.spawn(async move {
            info!("Start processing batch_mints...");
            processor_clone.process_batch_mints(rx).await;
            info!("Finish processing batch_mints...");

            Ok(())
        });
    }

    let batch_mint_persister = BatchMintPersister::new(
        rocks_storage.clone(),
        batch_mint_persister::BatchMintDownloaderForPersister {},
        metrics_state.batch_mint_persisting_metrics.clone(),
    );
    let rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        info!("Start batch_mint persister...");
        batch_mint_persister.persist_batch_mints(rx).await
    });

    let solana_price_updater = SolanaPriceUpdater::new(
        rocks_storage.clone(),
        CoinGeckoPriceFetcher::new(),
        config.price_monitoring_interval_sec,
    );
    let rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        info!("Start monitoring solana price...");
        solana_price_updater.start_price_monitoring(rx).await;
        info!("Stop monitoring solana price...");
        Ok(())
    });

    start_metrics(metrics_state.registry, config.metrics_port).await;

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

async fn run_sequence_consistent_gapfiller<T, R>(
    slots_collector: SlotsCollector<T, R>,
    rocks_storage: Arc<Storage>,
    sequence_consistent_gapfill_metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
    rx: Receiver<()>,
    rpc_backfiller: Arc<BackfillRPC>,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    sequence_consistent_checker_wait_period_sec: u64,
) where
    T: SlotsDumper + Sync + Send + 'static,
    R: SlotsGetter + Sync + Send + 'static,
{
    let sequence_consistent_gapfiller = SequenceConsistentGapfiller::new(
        rocks_storage.clone(),
        slots_collector,
        sequence_consistent_gapfill_metrics.clone(),
        rpc_backfiller.clone(),
    );
    let mut rx = rx.resubscribe();
    let metrics = sequence_consistent_gapfill_metrics.clone();
    mutexed_tasks.lock().await.spawn(async move {
        info!("Start collecting sequences gaps...");
        loop {
            let start = Instant::now();
            sequence_consistent_gapfiller
                .collect_sequences_gaps(rx.resubscribe())
                .await;
            metrics.set_scans_latency(start.elapsed().as_secs_f64());
            metrics.inc_total_scans();
            tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(sequence_consistent_checker_wait_period_sec)) => {},
                    _ = rx.recv() => {
                        info!("Received stop signal, stopping collecting sequences gaps");
                        break;
                    }
                };
        }

        Ok(())
    });
}

const TRANSACTIONS_GETTER_IDLE_TIMEOUT_MILLIS: u64 = 250;

// todo: move all inner processing logic into separate file
async fn run_transaction_processor<TG: UnprocessedTransactionsGetter + Send + Sync + 'static>(
    rx: Receiver<()>,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    unprocessed_transactions_getter: Arc<TG>,
    geyser_bubblegum_updates_processor: Arc<BubblegumTxProcessor>,
) {
    mutexed_tasks.lock().await.spawn(async move {
        while rx.is_empty() {
            let txs = match unprocessed_transactions_getter.next_transactions().await {
                Ok(txs) => txs,
                Err(err) => {
                    error!("Get next transactions: {}", err);
                    tokio::time::sleep(Duration::from_millis(
                        TRANSACTIONS_GETTER_IDLE_TIMEOUT_MILLIS,
                    ))
                    .await;
                    continue;
                }
            };
            for tx in txs {
                match geyser_bubblegum_updates_processor
                    .process_transaction(tx.tx)
                    .await
                {
                    Ok(_) => unprocessed_transactions_getter.ack(tx.id),
                    Err(err) => {
                        if err != IngesterError::NotImplemented {
                            error!("Background saver could not process received data: {}", err);
                        }
                    }
                }
            }
        }

        Ok(())
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_accounts_processor<AG: UnprocessedAccountsGetter + Sync + Send + 'static>(
    rx: Receiver<()>,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    unprocessed_transactions_getter: Arc<AG>,
    rocks_storage: Arc<Storage>,
    account_buffer_size: usize,
    fees_buffer_size: usize,
    metrics: Arc<IngesterMetricsConfig>,
    postgre_client: Arc<PgClient>,
    rpc_client: Arc<RpcClient>,
    join_set: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) {
    mutexed_tasks.lock().await.spawn(async move {
        let account_processor = AccountsProcessor::build(
            rx.resubscribe(),
            fees_buffer_size,
            unprocessed_transactions_getter,
            metrics,
            postgre_client,
            rpc_client,
            join_set,
        )
        .await
        .unwrap();
        account_processor
            .process_accounts(rx, rocks_storage, account_buffer_size)
            .await;
        Ok(())
    });
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
