use log::{error, info};
use pprof::protos::Message;
use pprof::ProfilerGuard;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub async fn graceful_stop(
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    wait_all: bool,
    keep_running: Arc<AtomicBool>,
    shutdown_tx: Sender<()>,
    guard: Option<ProfilerGuard<'_>>,
    profile_path: Option<String>,
) {
    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    keep_running.store(false, Ordering::SeqCst);

    if let Some(guard) = guard {
        if let Ok(report) = guard.report().build() {
            // This code will be called only for profiling, so unwraps is used
            let mut file = File::create(format!("{}/profile.pb", profile_path.unwrap())).unwrap();
            let profile = report.pprof().unwrap();

            let content = profile.write_to_bytes().unwrap();
            file.write_all(&content).unwrap();
        }
    }

    let _ = shutdown_tx.send(());
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
