use log::info;
use metrics_utils::red::RequestErrorDurationMetrics;
use postgre_client::PgClient;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use usecase::proofs::MaybeProofChecker;

use metrics_utils::ApiMetricsConfig;
use postgre_client::storage_traits::AssetIndexStorage;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::storage_traits::AssetUpdateIndexStorage;
use rocks_db::Storage;
use {crate::api::DasApi, std::env, std::net::SocketAddr};
use {
    jsonrpc_http_server::cors::AccessControlAllowHeaders,
    jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
};

use crate::api::builder::RpcApiBuilder;
use crate::api::config::load_config;
use crate::api::error::DasApiError;
use crate::api::middleware::{RpcRequestMiddleware, RpcResponseMiddleware};

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10);
// 50kB
pub const RUNTIME_WORKER_THREAD_COUNT: usize = 2000;
pub const MAX_CORS_AGE: u32 = 86400;
const CATCH_UP_SEQUENCES_TIMEOUT_SEC: u64 = 30;

#[derive(Clone)]
pub(crate) struct MiddlewaresData {
    response_middleware: RpcResponseMiddleware,
    request_middleware: RpcRequestMiddleware,
    pub(crate) last_primary_storage_seq: Arc<AtomicU64>,
    pub(crate) last_index_storage_seq: Arc<AtomicU64>,
    pub(crate) synchronization_api_threshold: u64,
}

pub async fn start_api(
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    metrics: Arc<ApiMetricsConfig>,
    red_metrics: Arc<RequestErrorDurationMetrics>,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<(), DasApiError> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV)
            .unwrap_or_else(|| "info,sqlx::query=warn".into()),
    );
    let config = load_config()?;
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let response_middleware = RpcResponseMiddleware {};
    let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::from_config(
        config.clone(),
        metrics,
        red_metrics,
        rocks_db.clone(),
        proof_checker,
    )
    .await?;
    let last_primary_storage_seq = Arc::new(AtomicU64::new(0));
    let last_index_storage_seq = Arc::new(AtomicU64::new(0));

    let last_primary_storage_seq_clone = last_primary_storage_seq.clone();
    let last_index_storage_seq_clone = last_index_storage_seq.clone();
    let pg_clone = api.pg_client.clone();
    let cloned_keep_running = keep_running.clone();
    tasks.lock().await.spawn(async move {
        while cloned_keep_running.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)).await;
            let Ok(Some(index_seq)) = pg_clone.fetch_last_synced_id().await else {
                continue;
            };
            let Ok(decoded_index_seq) = decode_u64x2_pubkey(index_seq) else {
                continue;
            };
            let Ok(Some(primary_seq)) = rocks_db.last_known_asset_updated_key() else {
                continue;
            };
            last_primary_storage_seq_clone.store(primary_seq.0, Ordering::SeqCst);
            last_index_storage_seq_clone.store(decoded_index_seq.0, Ordering::SeqCst);
        }
        Ok(())
    });

    run_api(
        api,
        Some(MiddlewaresData {
            response_middleware,
            request_middleware,
            last_primary_storage_seq,
            last_index_storage_seq,
            synchronization_api_threshold: config.synchronization_api_threshold,
        }),
        addr,
        keep_running,
    )
    .await
}

pub async fn start_api_v2(
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    metrics: Arc<ApiMetricsConfig>,
    port: u16,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    max_page_limit: usize,
) -> Result<(), DasApiError> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    // todo: setup middleware, looks like too many shit related to backups are there
    // let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::new(pg_client, rocks_db, metrics, proof_checker, max_page_limit);

    run_api(api, None, addr, keep_running).await
}

async fn run_api(
    api: DasApi<MaybeProofChecker>,
    middlewares_data: Option<MiddlewaresData>,
    addr: SocketAddr,
    keep_running: Arc<AtomicBool>,
) -> Result<(), DasApiError> {
    let rpc = RpcApiBuilder::build(api, &middlewares_data)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREAD_COUNT)
        .enable_all()
        .build()
        .expect("Runtime");

    let mut builder = ServerBuilder::new(rpc)
        .event_loop_executor(runtime.handle().clone())
        .threads(1)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Any,
        ]))
        .cors_allow_headers(AccessControlAllowHeaders::Any)
        .cors_max_age(Some(MAX_CORS_AGE))
        .max_request_body_size(MAX_REQUEST_BODY_SIZE)
        .health_api(("/health", "health"));
    if let Some(mw) = middlewares_data.clone() {
        builder = builder.request_middleware(mw.request_middleware);
    }
    if let Some(mw) = middlewares_data {
        builder = builder.response_middleware(mw.response_middleware);
    }
    let server = builder.start_http(&addr);

    let server = server.unwrap();
    info!("API Server Started");

    loop {
        if !keep_running.load(Ordering::SeqCst) {
            info!("Shutting down server");
            runtime.shutdown_background();
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    server.wait();

    info!("API Server ended");

    Ok(())
}
