use std::cmp::min;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use grpc::gapfiller::gap_filler_service_server::GapFillerServiceServer;
use log::{error, info};
use nft_ingester::api::service::start_api;
use nft_ingester::config::{init_logger, setup_config, ApiConfig};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use nft_ingester::json_worker::JsonWorker;
use prometheus_client::registry::Registry;

use metrics_utils::utils::setup_metrics;
use metrics_utils::{ApiMetricsConfig, JsonDownloaderMetricsConfig};
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

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const DEFAULT_SECONDARY_ROCKSDB_PATH: &str = "./my_rocksdb_secondary";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting API server...");

    let config: ApiConfig = setup_config("API_");
    init_logger(&config.get_log_level());

    let guard = if config.run_profiling {
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
        match setup_metrics(registry, config.metrics_port).await {
            Ok(_) => {
                info!("Setup metrics successfully")
            }
            Err(e) => {
                error!("Setup metrics failed: {:?}", e)
            }
        }
    });

    let max_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(250);
    let min_connections = min(10, max_connections / 2);
    let pg_client = postgre_client::PgClient::new(
        config.database_config.get_database_url()?.as_str(),
        min_connections,
        max_connections,
        red_metrics.clone(),
    )
    .await?;
    let pg_client = Arc::new(pg_client);
    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let keep_running = Arc::new(AtomicBool::new(true));

    let primary_storage_path = config
        .rocks_db_path_container
        .clone()
        .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string());
    let secondary_storage_path = config
        .rocks_db_secondary_path_container
        .clone()
        .unwrap_or(DEFAULT_SECONDARY_ROCKSDB_PATH.to_string());
    let storage = Storage::open_secondary(
        &primary_storage_path,
        &secondary_storage_path,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    let rocks_storage = Arc::new(storage);

    let cloned_rocks_storage = rocks_storage.clone();
    let proof_checker = config.rpc_host.map(|host| {
        Arc::new(MaybeProofChecker::new(
            Arc::new(RpcClient::new(host)),
            config.check_proofs_probability,
            config.check_proofs_commitment,
        ))
    });

    let json_worker = {
        if let Some(middleware_config) = &config.json_middleware_config {
            if middleware_config.is_enabled {
                Some(Arc::new(
                    JsonWorker::new(
                        pg_client.clone(),
                        rocks_storage.clone(),
                        json_downloader_metrics.clone(),
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

    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);
    let cloned_tasks = mutexed_tasks.clone();
    let cloned_rx = shutdown_rx.resubscribe();
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        match start_api(
            pg_client.clone(),
            cloned_rocks_storage.clone(),
            cloned_rx,
            metrics.clone(),
            config.server_port,
            proof_checker,
            config.max_page_limit,
            json_worker,
            None,
            config.json_middleware_config.clone(),
            cloned_tasks,
            config.archives_dir.as_ref(),
            config.consistence_synchronization_api_threshold,
            config.consistence_backfilling_slots_threshold,
            config.batch_mint_service_port,
            config.file_storage_path_container.as_str(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("Start API: {}", e);
            }
        };
    }));

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
    let addr = format!("0.0.0.0:{}", config.peer_grpc_port).parse()?;
    // Spawn the gRPC server task and add to JoinSet
    mutexed_tasks.lock().await.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(GapFillerServiceServer::new(serv))
            .serve_with_shutdown(addr, async {
                shutdown_rx.recv().await.unwrap();
            })
            .await
        {
            eprintln!("GRPC Server error: {}", e);
        }
        Ok(())
    });

    // try synchronizing secondary rocksdb instance every config.rocks_sync_interval_seconds
    let cloned_keep_running = keep_running.clone();
    let cloned_rocks_storage = rocks_storage.clone();
    let dur = tokio::time::Duration::from_secs(config.rocks_sync_interval_seconds);
    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            if let Err(e) = cloned_rocks_storage.db.try_catch_up_with_primary() {
                error!("Sync rocksdb error: {}", e);
            }
            tokio::time::sleep(dur).await;
        }
    }));

    // --stop
    graceful_stop(
        mutexed_tasks,
        keep_running.clone(),
        shutdown_tx,
        guard,
        config.profiling_file_path_container,
        &config.heap_path,
    )
    .await;

    Ok(())
}
