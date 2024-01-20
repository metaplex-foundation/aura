use tokio::signal;
use tokio::task::{JoinError, JoinSet};
use tracing::{error, info};

pub async fn listen_shutdown() {
    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
}

pub async fn graceful_stop(tasks: &mut JoinSet<Result<(), JoinError>>) {
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the tasks was finished")
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
