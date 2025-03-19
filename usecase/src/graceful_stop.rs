use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub async fn listen_shutdown() {
    match signal::ctrl_c().await {
        Ok(()) => {},
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        },
    }
}

pub async fn graceful_shutdown(cancellation_token: CancellationToken) {
    listen_shutdown().await;
    cancellation_token.cancel();
    crate::executor::shutdown().await;
}
