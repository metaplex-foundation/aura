use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub async fn graceful_stop(
    tasks: Arc<Mutex<JoinSet<core::result::Result<(), JoinError>>>>,
    wait_all: bool,
    keep_running: Arc<AtomicBool>,
) {
    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    keep_running.store(false, Ordering::SeqCst);

    while let Some(task) = tasks.lock().await.join_next().await {
        match task {
            Ok(_) => {
                if wait_all {
                    info!("One of the tasks was finished")
                } else {
                    break;
                }
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Task error: {}", err);
            }
        }
    }
}
