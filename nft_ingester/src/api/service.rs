use std::{net::SocketAddr, sync::Arc};

use hyper::{header::CONTENT_TYPE, Body, Method, Request, Response, Server, StatusCode};
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_http_server::{
    cors::AccessControlAllowHeaders,
    hyper,
    hyper::service::{make_service_fn, service_fn},
    AccessControlAllowOrigin, DomainsValidation, ServerBuilder,
};
use metrics_utils::ApiMetricsConfig;
use multer::Multipart;
use postgre_client::PgClient;
use rocks_db::Storage;
use tokio::{fs::File, io::AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::proofs::MaybeProofChecker;
use uuid::Uuid;

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
const BATCH_MINT_REQUEST_PATH: &str = "/batch_mint";

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
    batch_mint_service_port: Option<u16>,
    file_storage_path: &str,
    account_balance_getter: Arc<AccountBalanceGetterImpl>,
    storage_service_base_url: Option<String>,
    native_mint_pubkey: String,
    token_price_fetcher: Arc<RaydiumTokenPriceFetcher>,
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
    );

    run_api(
        api,
        Some(MiddlewaresData { response_middleware, request_middleware, consistency_checkers }),
        addr,
        batch_mint_service_port,
        file_storage_path,
        pg_client,
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
    batch_mint_service_port: Option<u16>,
    file_storage_path: &str,
    pg_client: Arc<PgClient>,
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
    let server = builder.start_http(&addr);
    if let Some(port) = batch_mint_service_port {
        run_batch_mint_service(
            cancellation_token.child_token(),
            port,
            file_storage_path.to_string(),
            pg_client,
        )
        .await;
    }

    let server = server.unwrap();
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

struct BatchMintService {
    pg_client: Arc<PgClient>,
    file_storage_path: String,
}

impl BatchMintService {
    fn new(pg_client: Arc<PgClient>, file_storage_path: String) -> Self {
        Self { pg_client, file_storage_path }
    }

    fn upload_file_page() -> Response<Body> {
        let html = r#"<!DOCTYPE html>
                        <html>
                        <body>

                        <form action="/batch_mint" method="post" enctype="multipart/form-data">
                          <input type="file" name="file" id="file">
                          <input type="submit" value="Upload file">
                        </form>

                        </body>
                        </html>"#;
        Response::new(Body::from(html))
    }

    async fn save_file(file_name: &str, file_bytes: &[u8]) -> std::io::Result<()> {
        let mut file = File::create(file_name).await?;
        file.write_all(file_bytes).await?;
        Ok(())
    }

    async fn request_handler(
        self: Arc<Self>,
        req: Request<Body>,
    ) -> Result<Response<Body>, hyper::Error> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, BATCH_MINT_REQUEST_PATH) => Ok(Self::upload_file_page()),
            (&Method::POST, BATCH_MINT_REQUEST_PATH) => {
                let boundary = req
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|ct| ct.to_str().ok())
                    .and_then(|ct| multer::parse_boundary(ct).ok());

                match boundary {
                    Some(boundary) => {
                        let mut multipart = Multipart::new(req.into_body(), boundary);
                        let file_name = format!("{}.json", Uuid::new_v4());
                        let full_file_path = format!("{}/{}", self.file_storage_path, &file_name);
                        while let Ok(Some(field)) = multipart.next_field().await {
                            let bytes = match field.bytes().await {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return Ok(Response::builder()
                                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                                        .body(Body::from(format!("Failed to read file: {}", e)))
                                        .unwrap())
                                },
                            };
                            if let Err(e) = Self::save_file(&full_file_path, bytes.as_ref()).await {
                                return Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(format!("Failed to save file: {}", e)))
                                    .unwrap());
                            }
                        }
                        if let Err(e) = self.pg_client.insert_new_batch_mint(&file_name).await {
                            error!("Failed to save batch mint state: {}", e);
                            return Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from("Failed to save file"))
                                .unwrap());
                        }
                        Ok(Response::new(Body::from("File uploaded successfully")))
                    },
                    None => Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("BAD REQUEST"))
                        .unwrap()),
                }
            },
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Page not found"))
                .unwrap()),
        }
    }
}

async fn run_batch_mint_service(
    cancellation_token: CancellationToken,
    port: u16,
    file_storage_path: String,
    pg_client: Arc<PgClient>,
) {
    let addr = ([0, 0, 0, 0], port).into();
    let batch_mint_service = Arc::new(BatchMintService::new(pg_client, file_storage_path));
    let make_svc = make_service_fn(move |_conn| {
        let batch_mint_service = batch_mint_service.clone();
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                batch_mint_service.clone().request_handler(req)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc).with_graceful_shutdown(async {
        cancellation_token.cancelled().await;
    });
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}
