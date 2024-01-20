use pprof::protos::Message;
use pprof::ProfilerGuard;
use std::fs::File;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub async fn graceful_stop(
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    keep_running: Arc<AtomicBool>,
    shutdown_tx: Sender<()>,
    guard: Option<ProfilerGuard<'_>>,
    profile_path: Option<String>,
) {
    usecase::graceful_stop::listen_shutdown().await;
    keep_running.store(false, Ordering::SeqCst);
    let _ = shutdown_tx.send(());

    if let Some(guard) = guard {
        if let Ok(report) = guard.report().build() {
            // This code will be called only for profiling, so unwraps is used
            let mut file = File::create(format!("{}/profile.pb", profile_path.unwrap())).unwrap();
            let profile = report.pprof().unwrap();

            let content = profile.write_to_bytes().unwrap();
            file.write_all(&content).unwrap();
        }
    }

    usecase::graceful_stop::graceful_stop(tasks.lock().await.deref_mut()).await
}
