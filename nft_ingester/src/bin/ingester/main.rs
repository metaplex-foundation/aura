use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::Arweave;
use entities::enums::{AssetType, ASSET_TYPES};
use nft_ingester::batch_mint::batch_mint_persister::{BatchMintDownloaderForPersister, BatchMintPersister};
use nft_ingester::scheduler::Scheduler;
use postgre_client::PG_MIGRATIONS_PATH;
use rocks_db::key_encoders::encode_u64x2_pubkey;
use std::panic;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::FutureExt;
use grpc::asseturls::asset_url_service_server::AssetUrlServiceServer;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use nft_ingester::json_worker;
use plerkle_messenger::ConsumptionType;
use pprof::ProfilerGuardBuilder;
use rocks_db::bubblegum_slots::{BubblegumSlotGetter, IngestableSlotGetter};
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
    run_perpetual_slot_collection, run_perpetual_slot_processing, run_slot_force_persister, BackfillSource, Backfiller,
    DirectBlockParser, ForceReingestableSlotGetter, TransactionsParser,
};
use nft_ingester::batch_mint::batch_mint_processor::{process_batch_mints, BatchMintProcessor, NoopBatchMintTxSender};
use nft_ingester::buffer::{debug_buffer, Buffer};
use nft_ingester::config::{
    setup_config, ApiConfig, BackfillerConfig, BackfillerMode, IngesterConfig, MessageSource, INGESTER_CONFIG_PREFIX,
};
use nft_ingester::fork_cleaner::{run_fork_cleaner, ForkCleaner};
use nft_ingester::gapfiller::{process_asset_details_stream_wrapper, run_sequence_consistent_gapfiller};
use nft_ingester::index_syncronizer::Synchronizer;
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
use rocks_db::backup_service;
use rocks_db::backup_service::BackupService;
use rocks_db::storage_traits::{AssetSlotStorage, AssetUpdateIndexStorage};
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
pub const SECONDS_TO_RETRY_IDXS_CLEANUP: u64 = 15;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting Ingester...");

    let config = setup_config::<IngesterConfig>(INGESTER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());
    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    let guard = config.get_is_run_profiling().then(|| {
        ProfilerGuardBuilder::default()
            .frequency(100)
            .build()
            .expect("Failed to build 'ProfilerGuardBuilder'!")
    });

    // try to restore rocksDB first
    if Args::parse().restore_rocks_db {
        restore_rocksdb(&config).await?;
    }

    let buffer = Arc::new(Buffer::new());
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

    let synchronizer = Arc::new(Synchronizer::new(
        primary_rocks_storage.clone(),
        index_pg_storage.clone(),
        index_pg_storage.clone(),
        config.dump_synchronizer_batch_size,
        config.dump_path.to_string(),
        metrics_state.synchronizer_metrics.clone(),
        config.synchronizer_parallel_tasks,
        config.run_temp_sync_during_dump,
    ));

    if config.run_dump_synchronize_on_start {
        info!("Running dump synchronizer on start!");
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let shutdown_rx = shutdown_rx.resubscribe();

            mutexed_tasks.lock().await.spawn(async move {
                if let Err(e) = synchronizer
                    .full_syncronize(&shutdown_rx.resubscribe(), asset_type)
                    .await
                {
                    error!("Failed to syncronize on {:?} with error {}", asset_type, e);
                    panic!("Failed to syncronize on {:?} with error {}", asset_type, e);
                }

                Ok(())
            });
        }
    }
    while let Some(res) = mutexed_tasks.lock().await.join_next().await {
        match res {
            Ok(_) => {}
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => panic!("{err}"),
        }
    }

    // setup receiver
    let message_handler = Arc::new(MessageHandlerIngester::new(buffer.clone()));
    let (geyser_tcp_receiver, geyser_addr) = (
        TcpReceiver::new(message_handler.clone(), config.tcp_config.get_tcp_receiver_reconnect_interval()?),
        config.tcp_config.get_tcp_receiver_addr_ingester()?,
    );
    // For now there is no snapshot mechanism via Redis, so we use snapshot_tcp_receiver for this purpose
    let (snapshot_tcp_receiver, snapshot_addr) = (
        TcpReceiver::new(message_handler.clone(), config.tcp_config.get_tcp_receiver_reconnect_interval()? * 2),
        config.tcp_config.get_snapshot_addr_ingester()?,
    );

    let cloned_rx = shutdown_rx.resubscribe();
    let ack_channel = create_ack_channel(cloned_rx, config.redis_messenger_config.clone(), mutexed_tasks.clone()).await;

    let cloned_rx = shutdown_rx.resubscribe();
    if config.message_source == MessageSource::TCP {
        mutexed_tasks
            .lock()
            .await
            .spawn(connect_to_geyser(geyser_tcp_receiver, geyser_addr, cloned_rx));
    }

    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks
        .lock()
        .await
        .spawn(connect_to_snapshot_receiver(snapshot_tcp_receiver, snapshot_addr, cloned_rx));

    let cloned_buffer = buffer.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_metrics = metrics_state.ingester_metrics.clone();
    if config.message_source == MessageSource::TCP {
        mutexed_tasks
            .lock()
            .await
            .spawn(debug_buffer(cloned_rx, cloned_buffer, cloned_metrics));
    }

    // start backup service
    let backup_service = BackupService::new(primary_rocks_storage.db.clone(), &backup_service::load_config()?)?;
    let cloned_metrics = metrics_state.ingester_metrics.clone();
    if config.store_db_backups() {
        let cloned_rx = shutdown_rx.resubscribe();
        mutexed_tasks
            .lock()
            .await
            .spawn(perform_backup(backup_service, cloned_rx, cloned_metrics));
    }

    let rpc_client = Arc::new(RpcClient::new(config.rpc_host.clone()));
    for _ in 0..config.accounts_parsing_workers {
        match config.message_source {
            MessageSource::Redis => {
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
            MessageSource::TCP => {
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
        }
    }

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
    mutexed_tasks.lock().await.spawn(async move {
        match start_api(
            cloned_index_storage,
            cloned_rocks_storage.clone(),
            cloned_rx,
            cloned_api_metrics,
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
        primary_rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));

    for _ in 0..config.transactions_parsing_workers {
        match config.message_source {
            MessageSource::Redis => {
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
            MessageSource::TCP => {
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
        }
    }

    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_jp = json_processor.clone();
    mutexed_tasks
        .lock()
        .await
        .spawn(json_worker::run(cloned_jp, cloned_rx).map(|_| Ok(())));

    let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        primary_rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));
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
    let backfiller =
        Arc::new(Backfiller::new(primary_rocks_storage.clone(), backfiller_source.clone(), backfiller_config.clone()));

    let rpc_backfiller = Arc::new(BackfillRPC::connect(config.backfill_rpc_address.clone()));
    if config.run_bubblegum_backfiller {
        if backfiller_config.should_reingest {
            warn!("'Reingest' flag is set, deleting last fetched slot.");
            primary_rocks_storage
                .delete_parameter::<u64>(rocks_db::parameters::Parameter::LastFetchedSlot)
                .await?;
        }

        match backfiller_config.backfiller_mode {
            BackfillerMode::IngestDirectly => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    primary_rocks_storage.clone(),
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
                    .await?;
                info!("Running Backfiller directly from bigtable to ingester.");
            }
            BackfillerMode::Persist => {
                let consumer = primary_rocks_storage.clone();
                backfiller
                    .start_backfill(
                        mutexed_tasks.clone(),
                        shutdown_rx.resubscribe(),
                        metrics_state.backfiller_metrics.clone(),
                        consumer,
                        backfiller_source.clone(),
                    )
                    .await?;
                info!("Running Backfiller to persist raw data.");
            }
            BackfillerMode::IngestPersisted => {
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    primary_rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                let producer = primary_rocks_storage.clone();

                let transactions_parser = Arc::new(TransactionsParser::new(
                    primary_rocks_storage.clone(),
                    Arc::new(BubblegumSlotGetter::new(primary_rocks_storage.clone())),
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

                info!("Running Backfiller on persisted raw data.");
            }
            BackfillerMode::PersistAndIngest => {
                let rx = shutdown_rx.resubscribe();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_fetcher_");
                let backfiller_clone = backfiller.clone();
                let rpc_backfiller_clone = rpc_backfiller.clone();
                mutexed_tasks.lock().await.spawn(run_perpetual_slot_collection(
                    backfiller_clone,
                    rpc_backfiller_clone,
                    metrics,
                    backfiller_config.wait_period_sec,
                    rx,
                ));

                // run perpetual slot persister
                let rx = shutdown_rx.resubscribe();
                let consumer = primary_rocks_storage.clone();
                let producer = backfiller_source.clone();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_persister_");
                let slot_getter = Arc::new(BubblegumSlotGetter::new(primary_rocks_storage.clone()));
                let backfiller_clone = backfiller.clone();
                mutexed_tasks.lock().await.spawn(run_perpetual_slot_processing(
                    backfiller_clone,
                    metrics,
                    slot_getter,
                    consumer,
                    producer,
                    backfiller_config.wait_period_sec,
                    rx,
                    None,
                ));
                // run perpetual ingester
                let rx = shutdown_rx.resubscribe();
                let consumer = Arc::new(DirectBlockParser::new(
                    tx_ingester.clone(),
                    primary_rocks_storage.clone(),
                    metrics_state.backfiller_metrics.clone(),
                ));
                let producer = primary_rocks_storage.clone();
                let metrics = Arc::new(BackfillerMetricsConfig::new());
                metrics.register_with_prefix(&mut metrics_state.registry, "slot_ingester_");
                let slot_getter = Arc::new(IngestableSlotGetter::new(primary_rocks_storage.clone()));
                let backfiller_clone = backfiller.clone();
                let backup = backfiller_source.clone();
                mutexed_tasks.lock().await.spawn(run_perpetual_slot_processing(
                    backfiller_clone,
                    metrics,
                    slot_getter,
                    consumer,
                    producer,
                    backfiller_config.wait_period_sec,
                    rx,
                    Some(backup),
                ));
            }
            BackfillerMode::None => {
                info!("Not running Backfiller.");
            }
        };
    }

    if !config.disable_synchronizer {
        for asset_type in ASSET_TYPES {
            let rx = shutdown_rx.resubscribe();
            let synchronizer = synchronizer.clone();
            mutexed_tasks.lock().await.spawn(async move {
                match asset_type {
                    AssetType::NonFungible => {
                        synchronizer
                            .nft_run(&rx, config.dump_sync_threshold, Duration::from_secs(5))
                            .await
                    }
                    AssetType::Fungible => {
                        synchronizer
                            .fungible_run(&rx, config.dump_sync_threshold, Duration::from_secs(5))
                            .await
                    }
                }

                Ok(())
            });
        }
    }
    while let Some(res) = mutexed_tasks.lock().await.join_next().await {
        match res {
            Ok(_) => {}
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => panic!("{err}"),
        }
    }

    // setup dependencies for grpc server
    let uc = AssetStreamer::new(config.peer_grpc_max_gap_slots, primary_rocks_storage.clone());
    let bs = BlocksStreamer::new(config.peer_grpc_max_gap_slots, primary_rocks_storage.clone());
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
                primary_rocks_storage.clone(),
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
            loop {
                if rx.try_recv().is_ok() {
                    break;
                }
                let optional_last_synced_key = match asset_type {
                    AssetType::NonFungible => primary_rocks_storage.last_known_nft_asset_updated_key(),
                    AssetType::Fungible => primary_rocks_storage.last_known_fungible_asset_updated_key(),
                };

                if let Ok(Some(last_synced_key)) = optional_last_synced_key {
                    let last_synced_key =
                        encode_u64x2_pubkey(last_synced_key.seq, last_synced_key.slot, last_synced_key.pubkey);
                    primary_rocks_storage
                        .clean_syncronized_idxs(asset_type, last_synced_key)
                        .unwrap();
                };
                tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_IDXS_CLEANUP)).await;
            }

            Ok(())
        });
    }

    start_metrics(metrics_state.registry, config.metrics_port).await;

    // --stop
    graceful_stop(mutexed_tasks, shutdown_tx, guard, config.profiling_file_path_container, &config.heap_path).await;

    Ok(())
}
