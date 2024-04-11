use hyper::{header::CONTENT_TYPE, Body, Method, Request, Response, Server, StatusCode};
use jsonrpc_http_server::hyper;
use jsonrpc_http_server::hyper::service::{make_service_fn, service_fn};
use log::info;
use metrics_utils::red::RequestErrorDurationMetrics;
use multer::Multipart;
use postgre_client::PgClient;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::Receiver;
use tracing::error;
use usecase::proofs::MaybeProofChecker;
use uuid::Uuid;

use metrics_utils::ApiMetricsConfig;
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
const BATCH_MINT_REQUEST_PATH: &str = "/batch_mint";

pub async fn start_api(
    rocks_db: Arc<Storage>,
    metrics: Arc<ApiMetricsConfig>,
    red_metrics: Arc<RequestErrorDurationMetrics>,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    shutdown_rx: Receiver<()>,
) -> Result<(), DasApiError> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV)
            .unwrap_or_else(|| "info,sqlx::query=warn".into()),
    );
    let config = load_config()?;
    let (batch_mint_service_port, file_storage_path) = (
        config.batch_mint_service_port,
        &config.file_storage_path_container.clone(),
    );
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::from_config(config, metrics, red_metrics, rocks_db, proof_checker).await?;

    run_api(
        api,
        Some(request_middleware),
        addr,
        batch_mint_service_port,
        file_storage_path,
        shutdown_rx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn start_api_v2(
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    metrics: Arc<ApiMetricsConfig>,
    port: u16,
    batch_mint_service_port: u16,
    file_storage_path: &str,
    proof_checker: Option<Arc<MaybeProofChecker>>,
    max_page_limit: usize,
    shutdown_rx: Receiver<()>,
) -> Result<(), DasApiError> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    // todo: setup middleware, looks like too many shit related to backups are there
    // let request_middleware = RpcRequestMiddleware::new(config.archives_dir.as_str());
    let api = DasApi::new(pg_client, rocks_db, metrics, proof_checker, max_page_limit);

    run_api(
        api,
        None,
        addr,
        batch_mint_service_port,
        file_storage_path,
        shutdown_rx,
    )
    .await
}

async fn run_api(
    api: DasApi<MaybeProofChecker>,
    request_middleware: Option<RpcRequestMiddleware>,
    addr: SocketAddr,
    batch_mint_service_port: u16,
    file_storage_path: &str,
    shutdown_rx: Receiver<()>,
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

            if let Some(boundary) = boundary {
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
                return Ok(Response::new(Body::from("File uploaded successfully")));
            }

            Ok(Response::new(Body::from("File uploaded")))
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
