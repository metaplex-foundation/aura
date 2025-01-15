use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

use crate::errors::MetricsError;

pub async fn setup_metrics(registry: Registry, port: Option<u16>) -> Result<(), MetricsError> {
    if let Some(port) = port {
        let metrics_addr = SocketAddr::from(([0, 0, 0, 0], port));
        start_metrics_server(metrics_addr, registry).await
    } else {
        warn!("Metrics port is missing");
        Ok(())
    }
}

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(
    metrics_addr: SocketAddr,
    registry: Registry,
) -> Result<(), MetricsError> {
    let mut shutdown_stream =
        signal(SignalKind::interrupt()).map_err(|e| MetricsError::Unexpected(e.to_string()))?;

    info!("Starting metrics server on {metrics_addr}");

    let registry = Arc::new(registry);
    Server::bind(&metrics_addr)
        .serve(make_service_fn(move |_conn| {
            let registry = registry.clone();
            async move {
                let handler = make_handler(registry);
                Ok::<_, io::Error>(service_fn(handler))
            }
        }))
        .with_graceful_shutdown(async move {
            shutdown_stream.recv().await;
        })
        .await
        .map_err(|err| {
            error!("Failed to starting metrics server {}", err);
            MetricsError::MetricsServer(err.to_string())
        })?;

    Ok(())
}

type HttpResponseFuture = Box<dyn Future<Output = io::Result<Response<Body>>> + Send>;

/// This function returns a HTTP handler (i.e. another function)
pub fn make_handler(registry: Arc<Registry>) -> impl Fn(Request<Body>) -> Pin<HttpResponseFuture> {
    // This closure accepts a request and responds with the OpenMetrics encoding of our metrics.
    move |_req: Request<Body>| {
        let reg = registry.clone();
        Box::pin(async move {
            let mut buf = String::new();
            encode(&mut buf, &reg.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .map(|_| {
                    let body = Body::from(buf);
                    Response::builder()
                        .header(
                            hyper::header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )
                        .body(body)
                        .unwrap_or_default()
                })
        })
    }
}

pub async fn start_metrics(register: Registry, port: Option<u16>) {
    tokio::spawn(async move {
        match setup_metrics(register, port).await {
            Ok(_) => {
                info!("Metrics server stopped successfully");
            },
            Err(e) => {
                error!("Metrics server stopped with an error: {:?}", e)
            },
        }
    });
}
