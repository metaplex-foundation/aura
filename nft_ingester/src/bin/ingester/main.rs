use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::Arweave;
use entities::enums::{AssetType, ASSET_TYPES};
use nft_ingester::batch_mint::batch_mint_persister::{BatchMintDownloaderForPersister, BatchMintPersister};
use nft_ingester::cleaners::indexer_cleaner::clean_syncronized_idxs;
use nft_ingester::scheduler::Scheduler;
use postgre_client::PG_MIGRATIONS_PATH;
use std::panic;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use clap::Parser;
use futures::FutureExt;
use grpc::asseturls::asset_url_service_server::AssetUrlServiceServer;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use nft_ingester::json_worker;
use plerkle_messenger::ConsumptionType;
use pprof::ProfilerGuardBuilder;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;
use tokio::time::sleep as tokio_sleep;
use tracing::{error, info, warn};

use backfill_rpc::rpc::BackfillRPC;
use grpc::asseturls_impl::AssetUrlServiceImpl;
use grpc::client::Client;
use grpc::service::PeerGapFillerServiceImpl;
use metrics_utils::utils::start_metrics;
use metrics_utils::{BackfillerMetricsConfig, MetricState, MetricStatus, MetricsTrait};
use nft_ingester::ack::create_ack_channel;
use nft_ingester::api::account_balance::AccountBalanceGetterImpl;
use nft_ingester::api::service::start_api;
use nft_ingester::backfiller::{
    run_slot_force_persister, BackfillSource, DirectBlockParser, ForceReingestableSlotGetter, TransactionsParser,
};
use nft_ingester::batch_mint::batch_mint_processor::{process_batch_mints, BatchMintProcessor, NoopBatchMintTxSender};
use nft_ingester::buffer::{debug_buffer, Buffer};
use nft_ingester::cleaners::fork_cleaner::{run_fork_cleaner, ForkCleaner};
use nft_ingester::config::{
    setup_config, ApiConfig, BackfillerConfig, BackfillerMode, IngesterConfig, MessageSource, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::gapfiller::{process_asset_details_stream_wrapper, run_sequence_consistent_gapfiller};
use nft_ingester::init::{graceful_stop, init_index_storage_with_migration, init_primary_storage};
use nft_ingester::json_worker::JsonWorker;
use nft_ingester::message_handler::MessageHandlerIngester;
use nft_ingester::processors::accounts_processor::run_accounts_processor;
use nft_ingester::processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::processors::transaction_processor::run_transaction_processor;
use nft_ingester::redis_receiver::RedisReceiver;
use nft_ingester::rocks_db::{perform_backup, receive_last_saved_slot, restore_rocksdb};
use nft_ingester::tcp_receiver::{connect_to_geyser, connect_to_snapshot_receiver, TcpReceiver};
use nft_ingester::transaction_ingester::BackfillTransactionIngester;
use nft_ingester::{config::init_logger, error::IngesterError};
use rocks_db::backup_service::BackupService;
use rocks_db::storage_traits::AssetSlotStorage;
use rocks_db::{backup_service, SlotStorage};
use tonic::transport::Server;
use usecase::asset_streamer::AssetStreamer;
use usecase::proofs::MaybeProofChecker;
use usecase::raw_blocks_streamer::BlocksStreamer;
use usecase::signature_fetcher::SignatureFetcher;
use usecase::slots_collector::SlotsCollector;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const ARWEAVE_WALLET_PATH: &str = "./arweave_wallet.json";
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;
pub const SECONDS_TO_RETRY_IDXS_CLEANUP: u64 = 15 * 60; // 15 minutes

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,

    /// Path to the RocksDB instance with slots
    #[arg(short, long)]
    slots_db_path: PathBuf,

    /// Path to the secondary RocksDB instance with slots
    #[arg(long)]
    secondary_slots_db_path: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let args = Args::parse();

    let config = setup_config::<IngesterConfig>(INGESTER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());
    info!("Starting Ingester...");
    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    let guard = config.get_is_run_profiling().then(|| {
        ProfilerGuardBuilder::default()
            .frequency(100)
            .build()
            .expect("Failed to build 'ProfilerGuardBuilder'!")
    });

    // try to restore rocksDB first
    if args.restore_rocks_db {
        restore_rocksdb(&config).await?;
    }

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let primary_rocks_storage =
        Arc::new(init_primary_storage(&config, &metrics_state, mutexed_tasks.clone(), DEFAULT_ROCKSDB_PATH).await?);
    let index_pg_storage = Arc::new(
        init_index_storage_with_migration(
            &config.database_config.get_database_url()?,
            config
                .database_config
                .get_max_postgres_connections()
                .unwrap_or(DEFAULT_MAX_POSTGRES_CONNECTIONS),
            metrics_state.red_metrics.clone(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            PG_MIGRATIONS_PATH,
        )
        .await?,
    );

    // todo: remove backup service from here and move it to a separate process with a secondary db - verify it's possible first!
    // start backup service
    if config.store_db_backups() {
        let backup_service = BackupService::new(primary_rocks_storage.db.clone(), &backup_service::load_config()?)?;
        let cloned_metrics = metrics_state.ingester_metrics.clone();
        let cloned_rx = shutdown_rx.resubscribe();
        mutexed_tasks
            .lock()
            .await
            .spawn(perform_backup(backup_service, cloned_rx, cloned_metrics));
    }

    let geyser_bubblegum_updates_processor =
        Arc::new(BubblegumTxProcessor::new(primary_rocks_storage.clone(), metrics_state.ingester_metrics.clone()));
    let rpc_client = Arc::new(RpcClient::new(config.rpc_host.clone()));

    match config.message_source {
        MessageSource::TCP => {
            let buffer = Arc::new(Buffer::new());
            // setup receiver
            let message_handler = Arc::new(MessageHandlerIngester::new(buffer.clone()));
            let (geyser_tcp_receiver, geyser_addr) = (
                TcpReceiver::new(message_handler.clone(), config.tcp_config.get_tcp_receiver_reconnect_interval()?),
                config.tcp_config.get_tcp_receiver_addr_ingester()?,
            );
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks
                .lock()
                .await
                .spawn(connect_to_geyser(geyser_tcp_receiver, geyser_addr, cloned_rx));
            let (snapshot_tcp_receiver, snapshot_addr) = (
                TcpReceiver::new(message_handler.clone(), config.tcp_config.get_tcp_receiver_reconnect_interval()? * 2),
                config.tcp_config.get_snapshot_addr_ingester()?,
            );
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks.lock().await.spawn(connect_to_snapshot_receiver(
                snapshot_tcp_receiver,
                snapshot_addr,
                cloned_rx,
            ));

            let cloned_buffer = buffer.clone();
            let cloned_rx = shutdown_rx.resubscribe();
            let cloned_metrics = metrics_state.ingester_metrics.clone();
            mutexed_tasks
                .lock()
                .await
                .spawn(debug_buffer(cloned_rx, cloned_buffer, cloned_metrics));

            // Workers for snapshot parsing
            for _ in 0..config.snapshot_parsing_workers {
                run_accounts_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    buffer.clone(),
                    primary_rocks_storage.clone(),
                    config.snapshot_parsing_batch_size,
                    config.mpl_core_fees_buffer_size,
                    metrics_state.ingester_metrics.clone(),
                    // during snapshot parsing we don't want to collect message process metrics
                    None,
                    index_pg_storage.clone(),
                    rpc_client.clone(),
                    mutexed_tasks.clone(),
                )
                .await;
            }
            for _ in 0..config.accounts_parsing_workers {
                run_accounts_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    buffer.clone(),
                    primary_rocks_storage.clone(),
                    config.accounts_buffer_size,
                    config.mpl_core_fees_buffer_size,
                    metrics_state.ingester_metrics.clone(),
                    // TCP sender does not send any ids with timestamps so we may not pass message process metrics here
                    None,
                    index_pg_storage.clone(),
                    rpc_client.clone(),
                    mutexed_tasks.clone(),
                )
                .await;
            }

            run_transaction_processor(
                shutdown_rx.resubscribe(),
                mutexed_tasks.clone(),
                buffer.clone(),
                geyser_bubblegum_updates_processor.clone(),
                // TCP sender does not send any ids with timestamps so we may not pass message process metrics here
                None,
            )
            .await;
        }
        MessageSource::Redis => {
            let cloned_rx = shutdown_rx.resubscribe();
            let ack_channel =
                create_ack_channel(cloned_rx, config.redis_messenger_config.clone(), mutexed_tasks.clone()).await;

            for _ in 0..config.accounts_parsing_workers {
                let redis_receiver = Arc::new(
                    RedisReceiver::new(
                        config.redis_messenger_config.clone(),
                        ConsumptionType::All,
                        ack_channel.clone(),
                    )
                    .await?,
                );
                run_accounts_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    redis_receiver,
                    primary_rocks_storage.clone(),
                    config.accounts_buffer_size,
                    config.mpl_core_fees_buffer_size,
                    metrics_state.ingester_metrics.clone(),
                    Some(metrics_state.message_process_metrics.clone()),
                    index_pg_storage.clone(),
                    rpc_client.clone(),
                    mutexed_tasks.clone(),
                )
                .await;
            }
            for _ in 0..config.transactions_parsing_workers {
                let redis_receiver = Arc::new(
                    RedisReceiver::new(
                        config.redis_messenger_config.clone(),
                        ConsumptionType::All,
                        ack_channel.clone(),
                    )
                    .await?,
                );
                run_transaction_processor(
                    shutdown_rx.resubscribe(),
                    mutexed_tasks.clone(),
                    redis_receiver,
                    geyser_bubblegum_updates_processor.clone(),
                    Some(metrics_state.message_process_metrics.clone()),
                )
                .await;
            }
        }
    }

    let last_saved_slot = primary_rocks_storage.last_saved_slot()?.unwrap_or_default();
    let first_processed_slot = Arc::new(AtomicU64::new(0));
    let first_processed_slot_clone = first_processed_slot.clone();
    let cloned_rocks_storage = primary_rocks_storage.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_tx = shutdown_tx.clone();

    mutexed_tasks.lock().await.spawn(receive_last_saved_slot(
        cloned_rx,
        cloned_tx,
        cloned_rocks_storage,
        first_processed_slot_clone,
        last_saved_slot,
    ));

    let json_processor = Arc::new(
        JsonWorker::new(
            index_pg_storage.clone(),
            primary_rocks_storage.clone(),
            metrics_state.json_downloader_metrics.clone(),
            metrics_state.red_metrics.clone(),
        )
        .await,
    );

    let grpc_client = Client::connect(config.clone())
        .await
        .map_err(|e| error!("GRPC Client new: {e}"))
        .ok();

    if let Some(gaped_data_client) = grpc_client.clone() {
        while first_processed_slot.load(Ordering::Relaxed) == 0 && shutdown_rx.is_empty() {
            tokio_sleep(Duration::from_millis(100)).await
        }

        let cloned_rocks_storage = primary_rocks_storage.clone();
        if shutdown_rx.is_empty() {
            let gaped_data_client_clone = gaped_data_client.clone();

            let first_processed_slot_value = first_processed_slot.load(Ordering::Relaxed);
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks.lock().await.spawn(process_asset_details_stream_wrapper(
                cloned_rx,
                cloned_rocks_storage,
                last_saved_slot,
                first_processed_slot_value,
                gaped_data_client_clone.clone(),
                false,
            ));

            let cloned_rocks_storage = primary_rocks_storage.clone();
            let cloned_rx = shutdown_rx.resubscribe();
            mutexed_tasks.lock().await.spawn(process_asset_details_stream_wrapper(
                cloned_rx,
                cloned_rocks_storage,
                last_saved_slot,
                first_processed_slot_value,
                gaped_data_client_clone,
                true,
            ));
        }
    };

    let cloned_rocks_storage = primary_rocks_storage.clone();
    let cloned_api_metrics = metrics_state.api_metrics.clone();
    let account_balance_getter = Arc::new(AccountBalanceGetterImpl::new(rpc_client.clone()));
    let proof_checker = config.check_proofs.then_some(Arc::new(MaybeProofChecker::new(
        rpc_client.clone(),
        config.check_proofs_probability,
        config.check_proofs_commitment,
    )));
    let tasks_clone = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();

    let middleware_json_downloader = config
        .json_middleware_config
        .as_ref()
        .filter(|conf| conf.is_enabled)
        .map(|_| json_processor.clone());

    let api_config = setup_config::<ApiConfig>(INGESTER_CONFIG_PREFIX);

    // it will check if asset which was requested is from the tree which has gaps in sequences
    // gap in sequences means missed transactions and  as a result incorrect asset data
    let tree_gaps_checker = {
        if api_config.skip_check_tree_gaps {
            None
        } else {
            Some(cloned_rocks_storage.clone())
        }
    };

    let cloned_index_storage = index_pg_storage.clone();
    let file_storage_path = api_config.file_storage_path_container.clone();
    let red_metrics = metrics_state.red_metrics.clone();
    mutexed_tasks.lock().await.spawn(async move {
        match start_api(
            cloned_index_storage,
            cloned_rocks_storage.clone(),
            cloned_rx,
            cloned_api_metrics,
            Some(red_metrics),
            api_config.server_port,
            proof_checker,
            tree_gaps_checker,
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
            api_config.native_mint_pubkey,
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

    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_jp = json_processor.clone();
    mutexed_tasks
        .lock()
        .await
        .spawn(json_worker::run(cloned_jp, cloned_rx).map(|_| Ok(())));

    let backfill_bubblegum_updates_processor =
        Arc::new(BubblegumTxProcessor::new(primary_rocks_storage.clone(), metrics_state.ingester_metrics.clone()));
    let tx_ingester = Arc::new(BackfillTransactionIngester::new(backfill_bubblegum_updates_processor.clone()));
    let backfiller_config = setup_config::<BackfillerConfig>(INGESTER_CONFIG_PREFIX);
    let backfiller_source = Arc::new(
        BackfillSource::new(
            &config.backfiller_source_mode,
            config.rpc_host.clone(),
            &backfiller_config.big_table_config,
        )
        .await,
    );

    let slot_db = Arc::new(
        SlotStorage::open_secondary(
            args.slots_db_path,
            args.secondary_slots_db_path,
            mutexed_tasks.clone(),
            metrics_state.red_metrics.clone(),
        )
        .expect("Failed to open slot storage"),
    );
    let shutdown_token = CancellationToken::new();
    if config.run_bubblegum_backfiller {
        if backfiller_config.should_reingest {
            warn!("'Reingest' flag is set, deleting last backfilled slot.");
            primary_rocks_storage
                .delete_parameter::<u64>(rocks_db::parameters::Parameter::LastBackfilledSlot)
                .await?;
        }

        match backfiller_config.backfiller_mode {
            BackfillerMode::IngestDirectly => {
                panic!("IngestDirectly mode is not supported any more.");
            }
            BackfillerMode::Persist => {
                panic!("Persist mode is not supported any more. Use slot_persister binary instead.");
            }
            BackfillerMode::IngestPersisted => {
                panic!("IngestDirectly mode is not supported any more. Use backfill binary instead.");
            }
            BackfillerMode::PersistAndIngest => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    primary_rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                let shutdown_token = shutdown_token.clone();
                let db = primary_rocks_storage.clone();
                let metrics: Arc<BackfillerMetricsConfig> = metrics_state.backfiller_metrics.clone();
                let slot_db = slot_db.clone();
                mutexed_tasks.lock().await.spawn(async move {
                    nft_ingester::backfiller::run_backfill_slots(shutdown_token, db, slot_db, consumer, metrics).await;
                    Ok(())
                });
            }
            BackfillerMode::None => {
                info!("Not running Backfiller.");
            }
        };
    }

    // setup dependencies for grpc server
    let uc = AssetStreamer::new(config.peer_grpc_max_gap_slots, primary_rocks_storage.clone());
    let bs = BlocksStreamer::new(config.peer_grpc_max_gap_slots, slot_db.clone());
    let serv = PeerGapFillerServiceImpl::new(Arc::new(uc), Arc::new(bs), primary_rocks_storage.clone());
    let asset_url_serv = AssetUrlServiceImpl::new(primary_rocks_storage.clone());
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
            error!("Server error: {}", e);
        }

        Ok(())
    });

    Scheduler::run_in_background(Scheduler::new(primary_rocks_storage.clone())).await;

    let rpc_backfiller = Arc::new(BackfillRPC::connect(config.backfill_rpc_address.clone()));

    let rocks_clone = primary_rocks_storage.clone();
    let signature_fetcher = SignatureFetcher::new(
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
            match signature_fetcher
                .fetch_signatures(program_id, config.rpc_retry_interval_millis)
                .await
            {
                Ok(_) => {
                    metrics_clone.inc_run_fetch_signatures("fetch_signatures", MetricStatus::SUCCESS);
                    info!("signatures sync finished successfully for program_id: {}", program_id);
                }
                Err(e) => {
                    metrics_clone.inc_run_fetch_signatures("fetch_signatures", MetricStatus::FAILURE);
                    error!("signatures sync failed: {:?} for program_id: {}", e, program_id);
                }
            }

            tokio_sleep(Duration::from_secs(60)).await;
        }

        Ok(())
    });

    if config.run_sequence_consistent_checker {
        let force_reingestable_slot_processor = Arc::new(ForceReingestableSlotGetter::new(
            primary_rocks_storage.clone(),
            Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                primary_rocks_storage.clone(),
                metrics_state.backfiller_metrics.clone(),
            )),
        ));
        run_sequence_consistent_gapfiller(
            SlotsCollector::new(
                force_reingestable_slot_processor.clone(),
                backfiller_source.clone(),
                metrics_state.backfiller_metrics.clone(),
            ),
            primary_rocks_storage.clone(),
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
                primary_rocks_storage.clone(),
                force_reingestable_slot_processor.clone(),
                force_reingestable_slot_processor.clone(),
                Arc::new(client),
                metrics.clone(),
                backfiller_config.workers_count,
                backfiller_config.chunk_size,
            ));
            mutexed_tasks
                .lock()
                .await
                .spawn(run_slot_force_persister(force_reingestable_transactions_parser, rx));
        } else {
            let force_reingestable_transactions_parser = Arc::new(TransactionsParser::new(
                primary_rocks_storage.clone(),
                force_reingestable_slot_processor.clone(),
                force_reingestable_slot_processor.clone(),
                producer.clone(),
                metrics.clone(),
                backfiller_config.workers_count,
                backfiller_config.chunk_size,
            ));
            mutexed_tasks
                .lock()
                .await
                .spawn(run_slot_force_persister(force_reingestable_transactions_parser, rx));
        }

        if config.run_fork_cleaner {
            let fork_cleaner = ForkCleaner::new(
                primary_rocks_storage.clone(),
                slot_db.clone(),
                metrics_state.fork_cleaner_metrics.clone(),
            );
            let rx = shutdown_rx.resubscribe();
            let metrics = metrics_state.fork_cleaner_metrics.clone();
            mutexed_tasks.lock().await.spawn(run_fork_cleaner(
                fork_cleaner,
                metrics,
                rx,
                config.sequence_consistent_checker_wait_period_sec,
            ));
        }
    }
    if let Ok(arweave) =
        Arweave::from_keypair_path(PathBuf::from_str(ARWEAVE_WALLET_PATH).unwrap(), ARWEAVE_BASE_URL.parse().unwrap())
    {
        let arweave = Arc::new(arweave);
        let batch_mint_processor = Arc::new(BatchMintProcessor::new(
            index_pg_storage.clone(),
            primary_rocks_storage.clone(),
            Arc::new(NoopBatchMintTxSender),
            arweave,
            file_storage_path,
            metrics_state.batch_mint_processor_metrics.clone(),
        ));
        let rx = shutdown_rx.resubscribe();
        let processor_clone = batch_mint_processor.clone();
        mutexed_tasks
            .lock()
            .await
            .spawn(process_batch_mints(processor_clone, rx));
    }

    let batch_mint_persister = BatchMintPersister::new(
        primary_rocks_storage.clone(),
        BatchMintDownloaderForPersister,
        metrics_state.batch_mint_persisting_metrics.clone(),
    );
    let rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        info!("Start batch_mint persister...");
        batch_mint_persister.persist_batch_mints(rx).await
    });

    // clean indexes
    for asset_type in ASSET_TYPES {
        let primary_rocks_storage = primary_rocks_storage.clone();
        let mut rx = shutdown_rx.resubscribe();
        mutexed_tasks.lock().await.spawn(async move {
            tokio::select! {
                _ = rx.recv() => {}
                _ = async move {
                    loop {
                        match clean_syncronized_idxs(primary_rocks_storage.clone(), asset_type) {
                            Ok(_) => {
                                info!("Cleaned synchronized indexes for {:?}", asset_type);
                            }
                            Err(e) => {
                                error!("Failed to clean synchronized indexes for {:?} with error {}", asset_type, e);
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_IDXS_CLEANUP)).await;
                    }
                } => {}
            }

            Ok(())
        });
    }

    start_metrics(metrics_state.registry, config.metrics_port).await;

    // --stop
    graceful_stop(
        mutexed_tasks,
        shutdown_tx,
        Some(shutdown_token),
        guard,
        config.profiling_file_path_container,
        &config.heap_path,
    )
    .await;

    Ok(())
}
