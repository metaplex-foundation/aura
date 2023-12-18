use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cadence_macros::is_global_default_set;
use log::{error, info};

use {crate::api::DasApi, std::env, std::net::SocketAddr};
use {
    crossbeam_channel::unbounded,
    jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
    jsonrpc_http_server::cors::AccessControlAllowHeaders,
};
use metrics_utils::{ApiMetricsConfig, MetricState, MetricsTrait};
use metrics_utils::utils::setup_metrics;
use rocks_db::Storage;

use crate::api::builder::RpcApiBuilder;
use crate::api::config::load_config;
use crate::api::error::DasApiError;
use crate::api::middleware::RpcRequestMiddleware;

pub fn safe_metric<F: Fn()>(f: F) {
    if is_global_default_set() {
        f()
    }
}

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10);
// 50kB
pub const RUNTIME_WORKER_THREAD_COUNT: usize = 2000;
pub const MAX_CORS_AGE: usize = 86400;

pub async fn start_api(
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), DasApiError> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV)
            .unwrap_or_else(|| "info,sqlx::query=warn".into()),
    );
    let config = load_config()?;
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let mut metrics_state = MetricState::new(ApiMetricsConfig::new());
    metrics_state.register_metrics();
    tokio::spawn(async move {
        match setup_metrics(metrics_state.registry, config.metrics_port).await {
            Ok(_) => {
                info!("Setup metrics successfully")
            }
            Err(e) => {
                error!("Setup metrics failed: {}", e)
            }
        }
    });

    let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::from_config(config, metrics_state.metrics, rocks_db).await?;
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
        .cors_max_age(MAX_CORS_AGE as u32)
        .max_request_body_size(MAX_REQUEST_BODY_SIZE)
        .health_api(("/health", "health"))
        .start_http(&addr);

    let (close_handle_sender, _close_handle_receiver) = unbounded();

    if let Err(e) = server {
        close_handle_sender.send(Err(e.to_string())).unwrap();
        panic!("{}", e);
    }

    let server = server.unwrap();
    close_handle_sender.send(Ok(server.close_handle())).unwrap();

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
