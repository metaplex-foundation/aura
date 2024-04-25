use hyper::{header::CONTENT_TYPE, Body, Method, Request, Response, Server, StatusCode};
use jsonrpc_http_server::hyper;
use jsonrpc_http_server::hyper::service::{make_service_fn, service_fn};
use log::info;
use multer::Multipart;
use postgre_client::PgClient;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::error;
use usecase::proofs::MaybeProofChecker;
use uuid::Uuid;

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
    rx: Receiver<()>,
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
    batch_mint_service_port: u16,
    file_storage_path: &str,
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
        tasks,
        batch_mint_service_port,
        file_storage_path,
        rx,
    )
    .await
}

async fn run_api(
    api: DasApi<MaybeProofChecker, JsonWorker, JsonWorker>,
    middlewares_data: Option<MiddlewaresData>,
    addr: SocketAddr,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    batch_mint_service_port: u16,
    file_storage_path: &str,
    shutdown_rx: Receiver<()>,
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
    run_batch_mint_service(
        shutdown_rx.resubscribe(),
        batch_mint_service_port,
        file_storage_path.to_string(),
    )
    .await;

    let server = server.unwrap();
    info!("API Server Started");

    loop {
        if !shutdown_rx.is_empty() {
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

async fn save_file(path: &str, file_bytes: &[u8]) -> std::io::Result<()> {
    let new_file_name = format!("{}/{}.json", path, Uuid::new_v4());

    let mut file = File::create(new_file_name).await?;
    file.write_all(file_bytes).await?;
    Ok(())
}

async fn batch_mint_request_handler(
    file_storage_path: String,
    req: Request<Body>,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, BATCH_MINT_REQUEST_PATH) => Ok(upload_file_page()),
        (&Method::POST, BATCH_MINT_REQUEST_PATH) => {
            let boundary = req
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|ct| ct.to_str().ok())
                .and_then(|ct| multer::parse_boundary(ct).ok());

            return match boundary {
                Some(boundary) => {
                    let mut multipart = Multipart::new(req.into_body(), boundary);
                    while let Ok(Some(field)) = multipart.next_field().await {
                        let bytes = match field.bytes().await {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                return Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(format!("Failed to read file: {}", e)))
                                    .unwrap())
                            }
                        };
                        if let Err(e) = save_file(&file_storage_path, bytes.as_ref()).await {
                            return Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from(format!("Failed to save file: {}", e)))
                                .unwrap());
                        }
                    }
                    Ok(Response::new(Body::from("File uploaded successfully")))
                }
                None => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("BAD REQUEST"))
                    .unwrap()),
            };
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Page not found"))
            .unwrap()),
    }
}

async fn run_batch_mint_service(
    mut shutdown_rx: Receiver<()>,
    port: u16,
    file_storage_path: String,
) {
    let addr = ([0, 0, 0, 0], port).into();
    let file_storage_path = Arc::new(file_storage_path);
    let make_svc = make_service_fn(move |_conn| {
        let file_storage_path = file_storage_path.clone();
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                batch_mint_request_handler(file_storage_path.to_string(), req)
            }))
        }
    });
    let server = Server::bind(&addr)
        .serve(make_svc)
        .with_graceful_shutdown(async {
            shutdown_rx.recv().await.unwrap();
        });
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}
