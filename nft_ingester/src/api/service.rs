use std::{net::SocketAddr, sync::Arc};

use interface::consistency_check::ConsistencyChecker;
use jsonrpc_http_server::{
    cors::AccessControlAllowHeaders, AccessControlAllowOrigin, DomainsValidation, ServerBuilder,
};
use metrics_utils::ApiMetricsConfig;
use postgre_client::PgClient;
use rocks_db::Storage;
use tokio_util::sync::CancellationToken;
use tracing::info;
use usecase::proofs::MaybeProofChecker;

use crate::{
    api::{
        account_balance::AccountBalanceGetterImpl,
        backfilling_state_consistency::BackfillingStateConsistencyChecker,
        builder::RpcApiBuilder,
        error::DasApiError,
        middleware::{RpcRequestMiddleware, RpcResponseMiddleware},
        synchronization_state_consistency::SynchronizationStateConsistencyChecker,
        DasApi,
    },
    config::{HealthCheckInfo, JsonMiddlewareConfig},
    json_worker::JsonWorker,
    raydium_price_fetcher::RaydiumTokenPriceFetcher,
};

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
    health_check_info: HealthCheckInfo,
    cancellation_token: CancellationToken,
    metrics: Arc<ApiMetricsConfig>,
    port: u16,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    tree_gaps_checker: Option<Arc<Storage>>,
    max_page_limit: usize,
    json_downloader: Option<Arc<JsonWorker>>,
    json_persister: Option<Arc<JsonWorker>>,
    json_middleware_config: Option<JsonMiddlewareConfig>,
    archives_dir: &str,
    consistence_synchronization_api_threshold: Option<u64>,
    consistence_backfilling_slots_threshold: Option<u64>,
    account_balance_getter: Arc<AccountBalanceGetterImpl>,
    storage_service_base_url: Option<String>,
    native_mint_pubkey: String,
    token_price_fetcher: Arc<RaydiumTokenPriceFetcher>,
    maximum_healthy_desync: u64,
) -> Result<(), DasApiError> {
    let response_middleware = RpcResponseMiddleware {};
    let request_middleware = RpcRequestMiddleware::new(archives_dir);

    let mut consistency_checkers: Vec<Arc<dyn ConsistencyChecker>> = vec![];

    if let Some(consistence_synchronization_api_threshold) =
        consistence_synchronization_api_threshold
    {
        let synchronization_state_consistency_checker =
            Arc::new(SynchronizationStateConsistencyChecker::new());
        synchronization_state_consistency_checker
            .run(
                cancellation_token.child_token(),
                pg_client.clone(),
                rocks_db.clone(),
                consistence_synchronization_api_threshold,
            )
            .await;
        consistency_checkers.push(synchronization_state_consistency_checker);
    }

    if let Some(consistence_backfilling_slots_threshold) = consistence_backfilling_slots_threshold {
        let backfilling_state_consistency_checker =
            Arc::new(BackfillingStateConsistencyChecker::new());
        backfilling_state_consistency_checker
            .run(
                cancellation_token.child_token(),
                rocks_db.clone(),
                consistence_backfilling_slots_threshold,
            )
            .await;
        consistency_checkers.push(backfilling_state_consistency_checker);
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let api = DasApi::new(
        pg_client.clone(),
        rocks_db,
        health_check_info,
        metrics,
        proof_checker,
        tree_gaps_checker,
        max_page_limit,
        json_downloader,
        json_persister,
        json_middleware_config.unwrap_or_default(),
        account_balance_getter,
        storage_service_base_url,
        token_price_fetcher,
        native_mint_pubkey,
        maximum_healthy_desync,
    );

    run_api(
        api,
        Some(MiddlewaresData { response_middleware, request_middleware, consistency_checkers }),
        addr,
        cancellation_token.child_token(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_api(
    api: DasApi<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        AccountBalanceGetterImpl,
        RaydiumTokenPriceFetcher,
        Storage,
    >,
    middlewares_data: Option<MiddlewaresData>,
    addr: SocketAddr,
    cancellation_token: CancellationToken,
) -> Result<(), DasApiError> {
    let rpc = RpcApiBuilder::build(
        api,
        middlewares_data.clone().map(|m| m.consistency_checkers).unwrap_or_default(),
    )?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREAD_COUNT)
        .enable_all()
        .build()
        .expect("Runtime");

    let mut builder = ServerBuilder::new(rpc)
        .event_loop_executor(runtime.handle().clone())
        .threads(1)
        .cors(DomainsValidation::AllowOnly(vec![AccessControlAllowOrigin::Any]))
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
    let server = builder.start_http(&addr).unwrap();

    info!("API Server Started {}", server.address().to_string());

    loop {
        if cancellation_token.is_cancelled() {
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
