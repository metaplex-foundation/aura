use log::info;
use postgre_client::PgClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use usecase::proofs::MaybeProofChecker;

use crate::api::backfilling_state_consistency::BackfillingStateConsistencyChecker;
use interface::consistency_check::ConsistencyChecker;
use metrics_utils::ApiMetricsConfig;
use rocks_db::Storage;

use {crate::api::DasApi, std::net::SocketAddr};
use {
    jsonrpc_http_server::cors::AccessControlAllowHeaders,
    jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder},
};

use crate::api::builder::RpcApiBuilder;
use crate::api::error::DasApiError;
use crate::api::middleware::{RpcRequestMiddleware, RpcResponseMiddleware};
use crate::api::synchronization_state_consistency::SynchronizationStateConsistencyChecker;
use crate::config::JsonMiddlewareConfig;
use crate::json_worker::JsonWorker;

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10);
// 50kB
pub const RUNTIME_WORKER_THREAD_COUNT: usize = 2000;
pub const MAX_CORS_AGE: u32 = 86400;

#[derive(Clone)]
pub(crate) struct MiddlewaresData {
    response_middleware: RpcResponseMiddleware,
    request_middleware: RpcRequestMiddleware,
    pub(crate) consistency_checkers: Vec<Arc<dyn ConsistencyChecker>>,
}

#[allow(clippy::too_many_arguments)]
pub async fn start_api(
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    keep_running: Arc<AtomicBool>,
    rx: tokio::sync::broadcast::Receiver<()>,
    metrics: Arc<ApiMetricsConfig>,
    port: u16,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    max_page_limit: usize,
    json_downloader: Option<Arc<JsonWorker>>,
    json_persister: Option<Arc<JsonWorker>>,
    json_middleware_config: Option<JsonMiddlewareConfig>,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    archives_dir: &str,
    consistence_synchronization_api_threshold: u64,
    consistence_backfilling_slots_threshold: u64,
) -> Result<(), DasApiError> {
    let response_middleware = RpcResponseMiddleware {};
    let request_middleware = RpcRequestMiddleware::new(archives_dir);
    let synchronization_state_consistency_checker =
        Arc::new(SynchronizationStateConsistencyChecker::new());
    synchronization_state_consistency_checker
        .run(
            tasks.clone(),
            rx.resubscribe(),
            pg_client.clone(),
            rocks_db.clone(),
            consistence_synchronization_api_threshold,
        )
        .await;

    let backfilling_state_consistency_checker = Arc::new(BackfillingStateConsistencyChecker::new());
    backfilling_state_consistency_checker
        .run(
            tasks.clone(),
            rx.resubscribe(),
            rocks_db.clone(),
            consistence_backfilling_slots_threshold,
        )
        .await;

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

    run_api(
        api,
        Some(MiddlewaresData {
            response_middleware,
            request_middleware,
            consistency_checkers: vec![
                synchronization_state_consistency_checker,
                backfilling_state_consistency_checker,
            ],
        }),
        addr,
        keep_running,
        tasks,
    )
    .await
}

async fn run_api(
    api: DasApi<MaybeProofChecker, JsonWorker, JsonWorker>,
    middlewares_data: Option<MiddlewaresData>,
    addr: SocketAddr,
    keep_running: Arc<AtomicBool>,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<(), DasApiError> {
    let rpc = RpcApiBuilder::build(
        api,
        middlewares_data
            .clone()
            .map(|m| m.consistency_checkers)
            .unwrap_or_default(),
        tasks,
    )?;
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
