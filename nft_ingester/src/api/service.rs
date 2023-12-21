use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use metrics_utils::utils::setup_metrics;
use metrics_utils::{ApiMetricsConfig, MetricState, MetricsTrait};
use rocks_db::Storage;
use {crate::api::DasApi, std::env, std::net::SocketAddr};
use {
    jsonrpc_http_server::cors::AccessControlAllowHeaders,
    jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
};

use crate::api::builder::RpcApiBuilder;
use crate::api::config::load_config;
use crate::api::error::DasApiError;
use crate::api::middleware::RpcRequestMiddleware;

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10);
// 50kB
pub const RUNTIME_WORKER_THREAD_COUNT: usize = 2000;
pub const MAX_CORS_AGE: u32 = 86400;

pub async fn start_api(
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    metrics: Arc<ApiMetricsConfig>,
) -> Result<(), DasApiError> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV)
            .unwrap_or_else(|| "info,sqlx::query=warn".into()),
    );
    let config = load_config()?;
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::from_config(config, metrics, rocks_db).await?;
    let rpc = RpcApiBuilder::build(api)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREAD_COUNT)
        .enable_all()
        .build()
        .expect("Runtime");

    let server = ServerBuilder::new(rpc)
        .event_loop_executor(runtime.handle().clone())
        .threads(1)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Any,
        ]))
        .request_middleware(request_middleware)
        .cors_allow_headers(AccessControlAllowHeaders::Any)
        .cors_max_age(Some(MAX_CORS_AGE))
        .max_request_body_size(MAX_REQUEST_BODY_SIZE)
        .health_api(("/health", "health"))
        .start_http(&addr);

    let server = server.unwrap();
    info!("API Server Started");

    while keep_running.load(Ordering::SeqCst) {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    info!("Shutting down server");
    runtime.shutdown_background();
    server.wait();

    info!("API Server ended");

    Ok(())
}
