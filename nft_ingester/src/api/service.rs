use log::info;
use metrics_utils::red::RequestErrorDurationMetrics;
use postgre_client::PgClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use usecase::proofs::MaybeProofChecker;

use metrics_utils::ApiMetricsConfig;
use rocks_db::Storage;

use {crate::api::DasApi, std::env, std::net::SocketAddr};
use {
    jsonrpc_http_server::cors::AccessControlAllowHeaders,
    jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
};

use crate::api::builder::RpcApiBuilder;
use crate::api::error::DasApiError;
use crate::api::middleware::RpcRequestMiddleware;
use crate::config::{setup_config, ApiConfig, JsonMiddlewareConfig};
use crate::json_worker::JsonWorker;

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10);
// 50kB
pub const RUNTIME_WORKER_THREAD_COUNT: usize = 2000;
pub const MAX_CORS_AGE: u32 = 86400;

pub async fn start_api(
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    metrics: Arc<ApiMetricsConfig>,
    red_metrics: Arc<RequestErrorDurationMetrics>,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    json_downloader: Option<Arc<JsonWorker>>,
    json_persister: Option<Arc<JsonWorker>>,
) -> Result<(), DasApiError> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV)
            .unwrap_or_else(|| "info,sqlx::query=warn".into()),
    );

    let config: ApiConfig = setup_config("API_");

    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::from_config(
        config,
        metrics,
        red_metrics,
        rocks_db,
        proof_checker,
        json_downloader,
        json_persister,
    )
    .await?;

    run_api(api, Some(request_middleware), addr, keep_running).await
}

#[allow(clippy::too_many_arguments)]
pub async fn start_api_v2(
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    metrics: Arc<ApiMetricsConfig>,
    port: u16,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    max_page_limit: usize,
    json_downloader: Option<Arc<JsonWorker>>,
    json_persister: Option<Arc<JsonWorker>>,
    json_middleware_config: Option<JsonMiddlewareConfig>,
) -> Result<(), DasApiError> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    // todo: setup middleware, looks like too many shit related to backups are there
    // let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::new(
        pg_client,
        rocks_db,
        metrics,
        proof_checker,
        max_page_limit,
        json_downloader,
        json_persister,
        json_middleware_config.unwrap_or_default(),
    );

    run_api(api, None, addr, keep_running).await
}

async fn run_api(
    api: DasApi<MaybeProofChecker, JsonWorker, JsonWorker>,
    request_middleware: Option<RpcRequestMiddleware>,
    addr: SocketAddr,
    keep_running: Arc<AtomicBool>,
) -> Result<(), DasApiError> {
    let rpc = RpcApiBuilder::build(api)?;
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
    if let Some(mw) = request_middleware {
        builder = builder.request_middleware(mw);
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
