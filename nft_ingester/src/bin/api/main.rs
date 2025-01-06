use std::cmp::min;
use std::sync::Arc;
use clap::Parser;
use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use nft_ingester::api::service::start_api;
use nft_ingester::config::{init_logger, setup_config, ApiClapArgs, SynchronizerClapArgs};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use nft_ingester::json_worker::JsonWorker;
use prometheus_client::registry::Registry;
use tracing::{error, info};

use metrics_utils::utils::setup_metrics;
use metrics_utils::{ApiMetricsConfig, JsonDownloaderMetricsConfig};
use nft_ingester::api::account_balance::AccountBalanceGetterImpl;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;
use tonic::transport::Server;
use usecase::proofs::MaybeProofChecker;

#[cfg(feature = "profiling")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let args = ApiClapArgs::parse();
    init_logger(&args.log_level);

    tracing_subscriber::fmt::init();
    info!("Starting API server...");

    let guard = if args.is_run_profiling {
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(100)
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    let mut registry = Registry::default();
    let metrics = Arc::new(ApiMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);
    let json_downloader_metrics = Arc::new(JsonDownloaderMetricsConfig::new());
    json_downloader_metrics.register(&mut registry);
    tokio::spawn(async move {
        match setup_metrics(registry, args.metrics_port).await {
            Ok(_) => {
                info!("Setup metrics successfully")
            }
            Err(e) => {
                error!("Setup metrics failed: {:?}", e)
            }
        }
    });

    let pg_client = postgre_client::PgClient::new(
        &args.pg_database_url,
        args.pg_min_db_connections,
        args.pg_max_db_connections,
        None,
        red_metrics.clone(),
    )
    .await?;
    let pg_client = Arc::new(pg_client);
    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let storage = Storage::open_secondary(
        &args.rocks_db_path_container,
        &args.rocks_db_secondary_path,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let rpc_client = Arc::new(RpcClient::new(args.rpc_host.clone()));
    let account_balance_getter = Arc::new(AccountBalanceGetterImpl::new(rpc_client.clone()));
    let cloned_rocks_storage = rocks_storage.clone();
    let mut proof_checker = None;

    if args.check_proofs {
        proof_checker = Some(Arc::new(MaybeProofChecker::new(
            rpc_client.clone(),
            args.check_proofs_probability,
            args.check_proofs_commitment,
        )));
    }

    let json_worker = {
        if let Some(middleware_config) = &args.json_middleware_config {
            if middleware_config.is_enabled {
                Some(Arc::new(
                    JsonWorker::new(
                        pg_client.clone(),
                        rocks_storage.clone(),
                        json_downloader_metrics.clone(),
                        red_metrics.clone(),
                        args.parallel_json_downloaders,
                    )
                    .await,
                ))
            } else {
                None
            }
        } else {
            None
        }
    };

    // it will check if asset which was requested is from the tree which has gaps in sequences
    // gap in sequences means missed transactions and  as a result incorrect asset data
    let tree_gaps_checker = {
        if args.skip_check_tree_gaps {
            None
        } else {
            Some(cloned_rocks_storage.clone())
        }
    };

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let cloned_tasks = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(async move {
        match start_api(
            pg_client.clone(),
            cloned_rocks_storage.clone(),
            cloned_rx,
            metrics.clone(),
            Some(red_metrics.clone()),
            args.server_port,
            proof_checker,
            tree_gaps_checker,
            args.max_page_limit,
            json_worker,
            None,
            args.json_middleware_config.clone(),
            cloned_tasks,
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
                // cannot return JointError here
                Ok(())
            }
        }
    });

    // setup dependencies for grpc server
    // TODO: add slots db to API server configuration and use it here to create BlocksStreamer, enable the grpc again
    // let uc = usecase::asset_streamer::AssetStreamer::new(
    //     config.peer_grpc_max_gap_slots,
    //     rocks_storage.clone(),
    // );
    // let bs = usecase::raw_blocks_streamer::BlocksStreamer::new(
    //     config.peer_grpc_max_gap_slots,
    //     rocks_storage.clone(),
    // );
    // let serv = grpc::service::PeerGapFillerServiceImpl::new(
    //     Arc::new(uc),
    //     Arc::new(bs),
    //     rocks_storage.clone(),
    // );
    // let addr = format!("0.0.0.0:{}", config.peer_grpc_port).parse()?;
    // let mut cloned_rx = shutdown_rx.resubscribe();
    // // Spawn the gRPC server task and add to JoinSet
    // mutexed_tasks.lock().await.spawn(async move {
    //     if let Err(e) = Server::builder()
    //         .add_service(GapFillerServiceServer::new(serv))
    //         .serve_with_shutdown(addr, async {
    //             cloned_rx.recv().await.unwrap();
    //         })
    //         .await
    //     {
    //         eprintln!("GRPC Server error: {}", e);
    //     }
    //     Ok(())
    // });

    // try synchronizing secondary rocksdb instance every config.rocks_sync_interval_seconds
    let cloned_rx = shutdown_rx.resubscribe();
    let cloned_rocks_storage = rocks_storage.clone();
    let dur = tokio::time::Duration::from_secs(args.rocks_sync_interval_seconds);
    mutexed_tasks.lock().await.spawn(async move {
        while cloned_rx.is_empty() {
            if let Err(e) = cloned_rocks_storage.db.try_catch_up_with_primary() {
                error!("Sync rocksdb error: {}", e);
            }
            tokio::time::sleep(dur).await;
        }

        Ok(())
    });

    // --stop
    graceful_stop(
        mutexed_tasks,
        shutdown_tx,
        None,
        guard,
        args.profiling_file_path_container,
        &args.heap_path,
    )
    .await;

    Ok(())
}
