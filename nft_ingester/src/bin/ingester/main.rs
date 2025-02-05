use std::{
    panic,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use arweave_rs::{consts::ARWEAVE_BASE_URL, Arweave};
use backfill_rpc::rpc::BackfillRPC;
use clap::Parser;
use entities::enums::ASSET_TYPES;
use futures::FutureExt;
use grpc::{
    asseturls::asset_url_service_server::AssetUrlServiceServer,
    asseturls_impl::AssetUrlServiceImpl, client::Client,
    gapfiller::gap_filler_service_server::GapFillerServiceServer,
    service::PeerGapFillerServiceImpl,
};
use metrics_utils::{
    utils::start_metrics, BackfillerMetricsConfig, MetricState, MetricStatus, MetricsTrait,
};
use nft_ingester::{
    ack::create_ack_channel,
    api::{account_balance::AccountBalanceGetterImpl, service::start_api},
    backfiller::{BackfillSource, DirectBlockParser},
    batch_mint::{
        batch_mint_persister::{BatchMintDownloaderForPersister, BatchMintPersister},
        batch_mint_processor::{process_batch_mints, BatchMintProcessor, NoopBatchMintTxSender},
    },
    cleaners::indexer_cleaner::clean_syncronized_idxs,
    config::{init_logger, IngesterClapArgs},
    error::IngesterError,
    gapfiller::{process_asset_details_stream_wrapper, run_sequence_consistent_gapfiller},
    init::{graceful_stop, init_index_storage_with_migration, init_primary_storage},
    json_worker,
    json_worker::JsonWorker,
    processors::{
        accounts_processor::run_accounts_processor,
        transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
        transaction_processor::run_transaction_processor,
    },
    redis_receiver::RedisReceiver,
    rocks_db::{receive_last_saved_slot, restore_rocksdb},
    scheduler::Scheduler,
    transaction_ingester::BackfillTransactionIngester,
};
use plerkle_messenger::{ConsumptionType, MessengerConfig, MessengerType};
use postgre_client::PG_MIGRATIONS_PATH;
#[cfg(feature = "profiling")]
use pprof::ProfilerGuardBuilder;
use rocks_db::{storage_traits::AssetSlotStorage, SlotStorage};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
    time::sleep as tokio_sleep,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};
use usecase::{
    asset_streamer::AssetStreamer, proofs::MaybeProofChecker, raw_blocks_streamer::BlocksStreamer,
    signature_fetcher::SignatureFetcher,
};
use uuid::Uuid;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const ARWEAVE_WALLET_PATH: &str = "./arweave_wallet.json";
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 8;
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;
pub const SECONDS_TO_RETRY_IDXS_CLEANUP: u64 = 15 * 60; // 15 minutes

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let args = IngesterClapArgs::parse();
    init_logger(&args.log_level);

    info!("Starting Ingester...");
    info!("___________________________________",);
    info!("API: {}", args.run_api.unwrap_or(false));
    if args.run_api.unwrap_or(false) {
        info!("API port: localhost:{}", args.server_port);
    }
    info!("Back Filler: {}", args.run_backfiller.unwrap_or(false));
    info!("Bubblegum BackFiller: {}", args.run_bubblegum_backfiller.unwrap_or(false));
    info!("Gap Filler: {}", args.run_gapfiller);
    info!("Run Profiling: {}", args.run_profiling);
    info!("Sequence Consistent Checker: {}", args.run_sequence_consistent_checker);
    info!("Account redis parsing workers: {}", args.redis_accounts_parsing_workers);
    info!("Account processor buffer size: {}", args.account_processor_buffer_size);
    info!("Tx redis parsing workers: {}", args.redis_transactions_parsing_workers);
    info!("Tx processor buffer size: {}", args.tx_processor_buffer_size);
    info!("___________________________________",);

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    #[cfg(feature = "profiling")]
    let guard = args.run_profiling.then(|| {
        ProfilerGuardBuilder::default()
            .frequency(100)
            .build()
            .expect("Failed to build 'ProfilerGuardBuilder'!")
    });

    // try to restore rocksDB first
    if args.is_restore_rocks_db {
        restore_rocksdb(
            &args
                .rocks_backup_url
                .expect("rocks_backup_url is required for the restore rocks db process"),
            &PathBuf::from_str(
                &args.rocks_backup_archives_dir.expect(
                    "rocks_backup_archives_dir is required for the restore rocks db process",
                ),
            )
            .expect("invalid rocks backup archives dir"),
            &PathBuf::from_str(&args.rocks_db_path)
                .expect("invalid rocks backup archives dir"),
        )
        .await?;
    }

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    info!("Init primary storage...");
    let primary_rocks_storage = Arc::new(
        init_primary_storage(
            &args.rocks_db_path,
            args.enable_rocks_migration.unwrap_or(false),
            &args.rocks_migration_storage_path,
            &metrics_state,
            mutexed_tasks.clone(),
        )
        .await?,
    );

    info!("Init PG storage...");
    let index_pg_storage = Arc::new(
        init_index_storage_with_migration(
            &args.pg_database_url,
            args.pg_max_db_connections,
            metrics_state.red_metrics.clone(),
            DEFAULT_MIN_POSTGRES_CONNECTIONS,
            PG_MIGRATIONS_PATH,
            None,
            Some(args.pg_max_query_statement_timeout_secs),
        )
        .await?,
    );

    let geyser_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        primary_rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
    ));
    let rpc_client = Arc::new(RpcClient::new(args.rpc_host.clone()));

    info!("Init Redis ....");
    let cloned_rx = shutdown_rx.resubscribe();
    let message_config = MessengerConfig {
        messenger_type: MessengerType::Redis,
        connection_config: args.redis_connection_config.clone(),
    };

    let ack_channel =
        create_ack_channel(cloned_rx, message_config.clone(), mutexed_tasks.clone()).await;

    for index in 0..args.redis_accounts_parsing_workers {
        let account_consumer_worker_name = Uuid::new_v4().to_string();
        info!("New Redis account worker {}: {}", index, account_consumer_worker_name);

        let personal_message_config = MessengerConfig {
            messenger_type: MessengerType::Redis,
            connection_config: {
                let mut config = args.redis_connection_config.clone();
                config
                    .insert("consumer_id".to_string(), account_consumer_worker_name.clone().into());
                config
                    .entry("batch_size".to_string())
                    .or_insert_with(|| args.account_processor_buffer_size.into());
                config
                    .entry("retries".to_string())
                    .or_insert_with(|| (args.redis_accounts_parsing_workers + 1).into());
                config
            },
        };
        let redis_receiver = Arc::new(
            RedisReceiver::new(
                personal_message_config,
                ConsumptionType::All,
                ack_channel.clone(),
                metrics_state.redis_receiver_metrics.clone(),
            )
            .await?,
        );

        run_accounts_processor(
            shutdown_rx.resubscribe(),
            mutexed_tasks.clone(),
            redis_receiver,
            primary_rocks_storage.clone(),
            args.account_processor_buffer_size,
            args.account_processor_mpl_fees_buffer_size,
            metrics_state.ingester_metrics.clone(),
            Some(metrics_state.message_process_metrics.clone()),
            index_pg_storage.clone(),
            rpc_client.clone(),
            mutexed_tasks.clone(),
            Some(account_consumer_worker_name.clone()),
        )
        .await;
    }

    for index in 0..args.redis_transactions_parsing_workers {
        let tx_consumer_worker_name = Uuid::new_v4().to_string();
        info!("New Redis tx worker {} : {}", index, tx_consumer_worker_name);

        let personal_message_config = MessengerConfig {
            messenger_type: MessengerType::Redis,
            connection_config: {
                let mut config = args.redis_connection_config.clone();
                config.insert("consumer_id".to_string(), tx_consumer_worker_name.into());
                config
                    .entry("batch_size".to_string())
                    .or_insert_with(|| args.tx_processor_buffer_size.into());
                config
                    .entry("retries".to_string())
                    .or_insert_with(|| (args.redis_transactions_parsing_workers + 1).into());
                config
            },
        };
        let redis_receiver = Arc::new(
            RedisReceiver::new(
                personal_message_config.clone(),
                ConsumptionType::All,
                ack_channel.clone(),
                metrics_state.redis_receiver_metrics.clone(),
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

    info!("MessageSource Redis FINISH");

    //todo Add starting from particular block
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
            args.parallel_json_downloaders,
            args.api_skip_inline_json_refresh.unwrap_or_default(),
        )
        .await,
    );

    if args.run_gapfiller {
        info!("Start gapfiller...");
        let gaped_data_client =
            Client::connect(&args.gapfiller_peer_addr.expect("gapfiller peer address is expected"))
                .await
                .map_err(|e| error!("GRPC Client new: {e}"))
                .expect("Failed to create GRPC Client");

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
    let proof_checker = args.check_proofs.then_some(Arc::new(MaybeProofChecker::new(
        rpc_client.clone(),
        args.check_proofs_probability,
        args.check_proofs_commitment,
    )));
    let tasks_clone = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    let file_storage_path = args.file_storage_path_container.clone();

    if args.run_api.unwrap_or(false) {
        info!("Starting API (Ingester)...");
        let middleware_json_downloader = args
            .json_middleware_config
            .as_ref()
            .filter(|conf| conf.is_enabled)
            .map(|_| json_processor.clone());

        // it will check if asset which was requested is from the tree which has gaps in sequences
        // gap in sequences means missed transactions and  as a result incorrect asset data
        let tree_gaps_checker = {
            if args.skip_check_tree_gaps {
                None
            } else {
                Some(cloned_rocks_storage.clone())
            }
        };

        let cloned_index_storage = index_pg_storage.clone();
        let red_metrics = metrics_state.red_metrics.clone();

        mutexed_tasks.lock().await.spawn(async move {
            match start_api(
                cloned_index_storage,
                cloned_rocks_storage.clone(),
                cloned_rx,
                cloned_api_metrics,
                Some(red_metrics),
                args.server_port,
                proof_checker,
                tree_gaps_checker,
                args.max_page_limit,
                middleware_json_downloader.clone(),
                middleware_json_downloader,
                args.json_middleware_config,
                tasks_clone,
                &args.archives_dir,
                args.consistence_synchronization_api_threshold,
                args.consistence_backfilling_slots_threshold,
                args.batch_mint_service_port,
                args.file_storage_path_container.as_str(),
                account_balance_getter,
                args.storage_service_base_url,
                args.native_mint_pubkey,
            )
            .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("Start API: {}", e);
                    Ok(())
                },
            }
        });
    }

    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_jp = json_processor.clone();
    mutexed_tasks.lock().await.spawn(json_worker::run(cloned_jp, cloned_rx).map(|_| Ok(())));

    let shutdown_token = CancellationToken::new();

    // Backfiller
    if args.run_backfiller.unwrap_or(false) {
        info!("Start backfiller...");

        let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            primary_rocks_storage.clone(),
            metrics_state.ingester_metrics.clone(),
        ));
        let tx_ingester = Arc::new(BackfillTransactionIngester::new(
            backfill_bubblegum_updates_processor.clone(),
        ));

        let slot_db = Arc::new(
            SlotStorage::open_secondary(
                args.rocks_slots_db_path
                    .clone()
                    .expect("slots_db_path is required for SlotStorage"),
                args.rocks_secondary_slots_db_path.clone(),
                mutexed_tasks.clone(),
                metrics_state.red_metrics.clone(),
            )
            .expect("Failed to open slot storage"),
        );

        let rpc_url = &args.backfill_rpc_address.unwrap_or_else(|| args.rpc_host.clone());
        let rpc_backfiller = Arc::new(BackfillRPC::connect(rpc_url.clone()));

        let backfiller_source = Arc::new(
            BackfillSource::new(
                &args.backfiller_source_mode,
                Option::from(rpc_url.clone()),
                args.big_table_config.as_ref(),
            )
            .await,
        );

        if args.run_bubblegum_backfiller.unwrap_or(false) {
            info!("Runing Bubblegum backfiller (ingester)...");

            if args.should_reingest {
                warn!("'Reingest' flag is set, deleting last backfilled slot.");
                primary_rocks_storage
                    .delete_parameter::<u64>(
                        rocks_db::columns::parameters::Parameter::LastBackfilledSlot,
                    )
                    .await?;
            }

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
                nft_ingester::backfiller::run_backfill_slots(
                    shutdown_token,
                    db,
                    slot_db,
                    consumer,
                    metrics,
                )
                .await;
                Ok(())
            });
        }

        if args.run_sequence_consistent_checker {
            info!("Running sequence consistent checker...");

            let direct_block_parser = Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                primary_rocks_storage.clone(),
                metrics_state.backfiller_metrics.clone(),
            ));
            run_sequence_consistent_gapfiller(
                primary_rocks_storage.clone(),
                backfiller_source.clone(),
                metrics_state.backfiller_metrics.clone(),
                metrics_state.sequence_consistent_gapfill_metrics.clone(),
                backfiller_source.clone(),
                direct_block_parser,
                shutdown_rx.resubscribe(),
                rpc_backfiller.clone(),
                mutexed_tasks.clone(),
                args.sequence_consistent_checker_wait_period_sec,
            )
            .await;
        }

        // setup dependencies for grpc server
        let uc = AssetStreamer::new(args.peer_grpc_max_gap_slots, primary_rocks_storage.clone());
        let bs = BlocksStreamer::new(args.peer_grpc_max_gap_slots, slot_db.clone());
        let serv = PeerGapFillerServiceImpl::new(
            Arc::new(uc),
            Arc::new(bs),
            primary_rocks_storage.clone(),
        );
        let asset_url_serv = AssetUrlServiceImpl::new(primary_rocks_storage.clone());
        let addr = format!("0.0.0.0:{}", args.peer_grpc_port).parse()?;
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
                    .fetch_signatures(program_id, args.rpc_retry_interval_millis)
                    .await
                {
                    Ok(_) => {
                        metrics_clone
                            .inc_run_fetch_signatures("fetch_signatures", MetricStatus::SUCCESS);
                        info!(
                            "signatures sync finished successfully for program_id: {}",
                            program_id
                        );
                    },
                    Err(e) => {
                        metrics_clone
                            .inc_run_fetch_signatures("fetch_signatures", MetricStatus::FAILURE);
                        error!("signatures sync failed: {:?} for program_id: {}", e, program_id);
                    },
                }

                tokio_sleep(Duration::from_secs(60)).await;
            }

            Ok(())
        });
    }

    Scheduler::run_in_background(Scheduler::new(primary_rocks_storage.clone())).await;

    if let Ok(arweave) = Arweave::from_keypair_path(
        PathBuf::from_str(ARWEAVE_WALLET_PATH).unwrap(),
        ARWEAVE_BASE_URL.parse().unwrap(),
    ) {
        info!("Running batch mint processor...");

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
        mutexed_tasks.lock().await.spawn(process_batch_mints(processor_clone, rx));
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
        info!("Start cleaning index {:?}", asset_type);

        let primary_rocks_storage = primary_rocks_storage.clone();
        let mut rx = shutdown_rx.resubscribe();
        let index_pg_storage = index_pg_storage.clone();
        mutexed_tasks.lock().await.spawn(async move {
            let index_pg_storage = index_pg_storage.clone();
            tokio::select! {
                _ = rx.recv() => {}
                _ = async move {
                    loop {
                        match clean_syncronized_idxs(index_pg_storage.clone(), primary_rocks_storage.clone(), asset_type).await {
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

    start_metrics(metrics_state.registry, args.metrics_port).await;

    // --stop
    #[cfg(not(feature = "profiling"))]
    graceful_stop(mutexed_tasks, shutdown_tx, Some(shutdown_token)).await;

    #[cfg(feature = "profiling")]
    graceful_stop(
        mutexed_tasks,
        shutdown_tx,
        Some(shutdown_token),
        guard,
        args.profiling_file_path_container,
        &args.heap_path,
    )
    .await;

    Ok(())
}
