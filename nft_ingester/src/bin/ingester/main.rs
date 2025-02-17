use std::{
    collections::HashMap,
    panic,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use backfill_rpc::rpc::BackfillRPC;
use clap::Parser;
use entities::enums::ASSET_TYPES;
use grpc::{
    asseturls::asset_url_service_server::AssetUrlServiceServer,
    asseturls_impl::AssetUrlServiceImpl, client::Client,
    gapfiller::gap_filler_service_server::GapFillerServiceServer,
    service::PeerGapFillerServiceImpl,
};
use interface::unprocessed_data_getter::AccountSource;
use metrics_utils::{
    utils::start_metrics, BackfillerMetricsConfig, MetricState, MetricStatus, MetricsTrait,
};
use nft_ingester::{
    ack::create_ack_channel,
    api::{account_balance::AccountBalanceGetterImpl, service::start_api},
    backfiller::{BackfillSource, DirectBlockParser},
    cleaners::indexer_cleaner::clean_syncronized_idxs,
    config::{init_logger, read_version_info, HealthCheckInfo, IngesterClapArgs},
    consts::{RAYDIUM_API_HOST, VERSION_FILE_PATH},
    error::IngesterError,
    gapfiller::{process_asset_details_stream_wrapper, run_sequence_consistent_gapfiller},
    init::{graceful_stop, init_index_storage_with_migration, init_primary_storage},
    metadata_workers::{
        downloader::MetadataDownloader,
        json_worker::JsonWorker,
        persister::{TasksPersister, JSON_BATCH},
        streamer::TasksStreamer,
    },
    processors::{
        accounts_processor::run_accounts_processor,
        transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
        transaction_processor::run_transaction_processor,
    },
    raydium_price_fetcher::{RaydiumTokenPriceFetcher, CACHE_TTL},
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
    sync::{broadcast, mpsc, Mutex},
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
    const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

    let args = IngesterClapArgs::parse();
    init_logger(&args.log_level);
    let image_info = read_version_info(VERSION_FILE_PATH);

    info!("Starting Ingester...");
    info!("___________________________________",);
    info!("Node name: {:?}", args.node_name);
    info!("APP VERSION: {}", APP_VERSION);
    info!("API: {}", args.run_api.unwrap_or(false));
    info!("Image info: {}", image_info.clone().unwrap_or("No image info available".to_string()));
    if args.run_api.unwrap_or(false) {
        info!("API port: localhost:{}", args.server_port);
    }
    info!("Back Filler: {}", args.run_backfiller.unwrap_or(false));
    info!("Bubblegum BackFiller: {}", args.run_bubblegum_backfiller.unwrap_or(false));
    info!("Gap Filler: {}", args.run_gapfiller);
    info!("Enable rocks migration: {}", args.enable_rocks_migration.unwrap_or(false));
    info!("Run Profiling: {}", args.run_profiling);
    info!("Sequence Consistent Checker: {}", args.run_sequence_consistent_checker);
    info!("Account redis parsing workers: {}", args.redis_accounts_parsing_workers);
    info!("Account processor buffer size: {}", args.account_processor_buffer_size);
    info!("Tx redis parsing workers: {}", args.redis_transactions_parsing_workers);
    info!("Tx processor buffer size: {}", args.tx_processor_buffer_size);
    info!("___________________________________",);

    let health_check_info = HealthCheckInfo {
        app_version: APP_VERSION.to_string(),
        node_name: args.node_name,
        image_info,
    };

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    #[cfg(feature = "profiling")]
    let guard = args.run_profiling.then(|| {
        ProfilerGuardBuilder::default()
            .frequency(100)
            .build()
            .expect("Failed to build 'ProfilerGuardBuilder'!")
    });

    let cancellation_token = CancellationToken::new();

    let stop_handle = tokio::task::spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            // --stop
            #[cfg(not(feature = "profiling"))]
            usecase::graceful_stop::graceful_shutdown(cancellation_token).await;

            #[cfg(feature = "profiling")]
            nft_ingester::init::graceful_stop(
                cancellation_token,
                guard,
                args.profiling_file_path_container,
                &args.heap_path,
            )
            .await;
        }
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
            &PathBuf::from_str(&args.rocks_db_path).expect("invalid rocks backup archives dir"),
        )
        .await
        .inspect_err(|e| {
            error!(error = %e, "Failed to restore rocksdb: {e:?}");
        })?;
    }

    info!("Init primary storage...");
    let primary_rocks_storage = Arc::new(
        init_primary_storage(
            &args.rocks_db_path,
            args.enable_rocks_migration.unwrap_or(false),
            &args.rocks_migration_storage_path,
            &metrics_state,
        )
        .await
        .inspect_err(|e| {
            error!(error = %e, "Failed to init primary storage: {e:?}");
        })?,
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
        .await
        .inspect_err(|e| {
            error!(error = %e, "Failed to init index storage with migration: {e:?}");
        })?,
    );

    let geyser_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        primary_rocks_storage.clone(),
        metrics_state.ingester_metrics.clone(),
    ));
    let rpc_client = Arc::new(RpcClient::new(args.rpc_host.clone()));

    info!("Init token/price information...");
    let token_price_fetcher = Arc::new(RaydiumTokenPriceFetcher::new(
        RAYDIUM_API_HOST.to_string(),
        CACHE_TTL,
        Some(metrics_state.red_metrics.clone()),
    ));
    let tpf = token_price_fetcher.clone();

    cancellation_token.run_until_cancelled(async move {
        if let Err(e) = tpf.warmup().await {
            error!(error = %e, "Failed to warm up Raydium token price fetcher, cache is empty: {:?}", e);
        }
        let (symbol_cache_size, _) = tpf.get_cache_sizes();
        info!(%symbol_cache_size, "Warmed up Raydium token price fetcher with {} symbols", symbol_cache_size);
    }).await;
    let well_known_fungible_accounts =
        token_price_fetcher.get_all_token_symbols().await.unwrap_or_else(|_| HashMap::new());

    info!("Init Redis ....");
    let message_config = MessengerConfig {
        messenger_type: MessengerType::Redis,
        connection_config: args.redis_connection_config.clone(),
    };

    let ack_channel =
        create_ack_channel(message_config.clone(), cancellation_token.child_token()).await;

    for index in 0..args.redis_accounts_parsing_workers {
        let account_consumer_worker_name = Uuid::new_v4().to_string();
        info!("New Redis account stream worker {}: {}", index, account_consumer_worker_name);

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
            .await
            .inspect_err(|e| {
                error!(error = %e, "Failed to create redis receiver: {e:?}");
            })?,
        );

        run_accounts_processor(
            cancellation_token.child_token(),
            redis_receiver,
            primary_rocks_storage.clone(),
            args.account_processor_buffer_size,
            args.account_processor_mpl_fees_buffer_size,
            metrics_state.ingester_metrics.clone(),
            Some(metrics_state.message_process_metrics.clone()),
            index_pg_storage.clone(),
            rpc_client.clone(),
            Some(account_consumer_worker_name.clone()),
            well_known_fungible_accounts.clone(),
            AccountSource::Stream,
        );
    }

    for index in 0..args.redis_account_backfill_parsing_workers {
        let account_consumer_worker_name = Uuid::new_v4().to_string();
        info!("New Redis account backfill worker {}: {}", index, account_consumer_worker_name);

        let personal_message_config = MessengerConfig {
            messenger_type: MessengerType::Redis,
            connection_config: {
                let mut config = args.redis_connection_config.clone();
                config
                    .insert("consumer_id".to_string(), account_consumer_worker_name.clone().into());
                config
                    .entry("batch_size".to_string())
                    .or_insert_with(|| args.account_backfill_processor_buffer_size.into());
                config
                    .entry("retries".to_string())
                    .or_insert_with(|| (args.redis_account_backfill_parsing_workers + 1).into());
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
            .await
            .inspect_err(|e| {
                error!(error = %e, "Failed to create redis receiver: {e:?}");
            })?,
        );

        run_accounts_processor(
            cancellation_token.child_token(),
            redis_receiver,
            primary_rocks_storage.clone(),
            args.account_backfill_processor_buffer_size,
            args.account_processor_mpl_fees_buffer_size,
            metrics_state.ingester_metrics.clone(),
            Some(metrics_state.message_process_metrics.clone()),
            index_pg_storage.clone(),
            rpc_client.clone(),
            Some(account_consumer_worker_name.clone()),
            well_known_fungible_accounts.clone(),
            AccountSource::Backfill,
        );
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
            .await
            .inspect_err(|e| {
                error!(error = %e, "Failed to create redis receiver: {e:?}");
            })?,
        );

        run_transaction_processor(
            cancellation_token.child_token(),
            redis_receiver,
            geyser_bubblegum_updates_processor.clone(),
            Some(metrics_state.message_process_metrics.clone()),
        );
    }

    info!("MessageSource Redis FINISH");

    //todo Add starting from particular block
    let last_saved_slot = primary_rocks_storage
        .last_saved_slot()
        .inspect_err(|e| {
            error!(error = %e, "Failed to get last saved slot: {e:?}");
        })?
        .unwrap_or_default();
    let first_processed_slot = Arc::new(AtomicU64::new(0));
    let first_processed_slot_clone = first_processed_slot.clone();
    let cloned_rocks_storage = primary_rocks_storage.clone();

    usecase::executor::spawn(receive_last_saved_slot(
        // NOTE: the clone here is important to bubble up the cancellation
        // from this function to other child tokens.
        cancellation_token.clone(),
        cloned_rocks_storage,
        first_processed_slot_clone,
        last_saved_slot,
    ));

    if args.run_gapfiller {
        info!("Start gapfiller...");
        let gaped_data_client =
            Client::connect(&args.gapfiller_peer_addr.expect("gapfiller peer address is expected"))
                .await
                .map_err(|e| error!("GRPC Client new: {e}"))
                .expect("Failed to create GRPC Client");

        while first_processed_slot.load(Ordering::Relaxed) == 0
            && !cancellation_token.is_cancelled()
        {
            tokio::time::sleep(Duration::from_millis(100)).await
        }

        let cloned_rocks_storage = primary_rocks_storage.clone();
        if !cancellation_token.is_cancelled() {
            let gaped_data_client_clone = gaped_data_client.clone();

            let first_processed_slot_value = first_processed_slot.load(Ordering::Relaxed);
            usecase::executor::spawn(process_asset_details_stream_wrapper(
                cancellation_token.child_token(),
                cloned_rocks_storage,
                last_saved_slot,
                first_processed_slot_value,
                gaped_data_client_clone.clone(),
                false,
            ));

            let cloned_rocks_storage = primary_rocks_storage.clone();
            usecase::executor::spawn(process_asset_details_stream_wrapper(
                cancellation_token.child_token(),
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

    if args.run_api.unwrap_or_default() {
        info!("Starting API (Ingester)...");
        let middleware_json_downloader = args
            .json_middleware_config
            .as_ref()
            .filter(|conf| conf.is_enabled)
            .map(|_| json_worker.clone());

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

        usecase::executor::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                start_api(
                    cloned_index_storage,
                    cloned_rocks_storage.clone(),
                    health_check_info,
                    cancellation_token,
                    cloned_api_metrics,
                    args.server_port,
                    proof_checker,
                    tree_gaps_checker,
                    args.max_page_limit,
                    middleware_json_downloader.clone(),
                    middleware_json_downloader,
                    args.json_middleware_config,
                    &args.archives_dir,
                    args.consistence_synchronization_api_threshold,
                    args.consistence_backfilling_slots_threshold,
                    account_balance_getter,
                    args.storage_service_base_url,
                    args.native_mint_pubkey,
                    token_price_fetcher,
                    args.api_maximum_healthy_desync,
                )
                .await
                .inspect_err(|e| error!("Start API: {}", e))
            }
        });
    }

    let (pending_tasks_sender, pending_tasks_receiver) = mpsc::channel(JSON_BATCH);
    let (refresh_tasks_sender, refresh_tasks_receiver) = mpsc::channel(JSON_BATCH);

    let metadata_streamer = TasksStreamer::new(
        json_worker.db_client.clone(),
        shutdown_rx.resubscribe(),
        pending_tasks_sender,
        refresh_tasks_sender,
    );

    let (metadata_to_persist_sender, metadata_to_persist_receiver) = mpsc::channel(JSON_BATCH);
    let pending_tasks_receiver = Arc::new(Mutex::new(pending_tasks_receiver));
    let refresh_tasks_receiver = Arc::new(Mutex::new(refresh_tasks_receiver));
    let metadata_dowloader = MetadataDownloader::new(
        json_worker.clone(),
        metadata_to_persist_sender,
        pending_tasks_receiver,
        refresh_tasks_receiver,
        shutdown_rx.resubscribe(),
        mutexed_tasks.clone(),
    );

    let metadata_persister = TasksPersister::new(
        json_worker.clone(),
        metadata_to_persist_receiver,
        shutdown_rx.resubscribe(),
    );

    mutexed_tasks.lock().await.spawn(async move {
        tokio::join!(
            metadata_streamer.run(json_worker.num_of_parallel_workers),
            metadata_dowloader.run(),
            metadata_persister.run(),
        );

        Ok(())
    });

    let shutdown_token = CancellationToken::new();

    // Backfiller
    if args.run_backfiller.unwrap_or_default() {
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

        if args.run_bubblegum_backfiller.unwrap_or_default() {
            info!("Runing Bubblegum backfiller (ingester)...");

            if args.should_reingest {
                warn!("'Reingest' flag is set, deleting last backfilled slot.");
                primary_rocks_storage
                    .delete_parameter::<u64>(
                        rocks_db::columns::parameters::Parameter::LastBackfilledSlot,
                    )
                    .await
                    .inspect_err(|e| {
                        error!(error = %e, "Failed to delete last saved slot: {e:?}");
                    })?;
            }

            let consumer = Arc::new(DirectBlockParser::new(
                tx_ingester.clone(),
                primary_rocks_storage.clone(),
                metrics_state.backfiller_metrics.clone(),
            ));
            let db = primary_rocks_storage.clone();
            let metrics: Arc<BackfillerMetricsConfig> = metrics_state.backfiller_metrics.clone();
            let slot_db = slot_db.clone();
            usecase::executor::spawn({
                let cancellation_token = cancellation_token.child_token();
                async move {
                    nft_ingester::backfiller::run_backfill_slots(
                        cancellation_token,
                        db,
                        slot_db,
                        consumer,
                        metrics,
                    )
                    .await;
                }
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
                cancellation_token.child_token(),
                rpc_backfiller.clone(),
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
        let addr = format!("0.0.0.0:{}", args.peer_grpc_port).parse().inspect_err(|e| {
            error!(error = %e, "Failed to parse peer grpc address: {e:?}");
        })?;
        // Spawn the gRPC server task and add to JoinSet

        usecase::executor::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                if let Err(e) = Server::builder()
                    .add_service(GapFillerServiceServer::new(serv))
                    .add_service(AssetUrlServiceServer::new(asset_url_serv))
                    .serve_with_shutdown(addr, cancellation_token.cancelled())
                    .await
                {
                    error!("Server error: {}", e);
                }
            }
        });

        let rocks_clone = primary_rocks_storage.clone();
        let signature_fetcher = SignatureFetcher::new(
            rocks_clone,
            rpc_backfiller.clone(),
            tx_ingester.clone(),
            metrics_state.rpc_backfiller_metrics.clone(),
        );
        let metrics_clone = metrics_state.rpc_backfiller_metrics.clone();

        usecase::executor::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                let program_id = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
                while !cancellation_token.is_cancelled() {
                    match signature_fetcher
                        .fetch_signatures(
                            program_id,
                            args.rpc_retry_interval_millis,
                            cancellation_token.child_token(),
                        )
                        .await
                    {
                        Ok(_) => {
                            metrics_clone.inc_run_fetch_signatures(
                                "fetch_signatures",
                                MetricStatus::SUCCESS,
                            );
                            info!(
                                "signatures sync finished successfully for program_id: {}",
                                program_id
                            );
                        },
                        Err(e) => {
                            metrics_clone.inc_run_fetch_signatures(
                                "fetch_signatures",
                                MetricStatus::FAILURE,
                            );
                            error!(
                                "signatures sync failed: {:?} for program_id: {}",
                                e, program_id
                            );
                        },
                    }

                    tokio::select! {
                        _ = cancellation_token.cancelled() => {}
                        _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                    }
                }
            }
        });
    }

    Scheduler::run_in_background(
        Scheduler::new(
            primary_rocks_storage.clone(),
            Some(well_known_fungible_accounts.keys().cloned().collect()),
        ),
        cancellation_token.child_token(),
    )
    .await;

    // clean indexes
    for asset_type in ASSET_TYPES {
        info!("Start cleaning index {:?}", asset_type);

        let primary_rocks_storage = primary_rocks_storage.clone();
        let index_pg_storage = index_pg_storage.clone();
        usecase::executor::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                let index_pg_storage = index_pg_storage.clone();
                tokio::select! {
                    _ = cancellation_token.cancelled() => {}
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
            }
        });
    }

    start_metrics(metrics_state.registry, args.metrics_port).await;
    if stop_handle.await.is_err() {
        error!("Error joining graceful shutdown!");
    }

    Ok(())
}
