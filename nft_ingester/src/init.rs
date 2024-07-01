use crate::error::IngesterError;
use pprof::protos::Message;
use pprof::ProfilerGuard;
use std::fs::File;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::error;

const MALLOC_CONF_ENV: &str = "MALLOC_CONF";

pub async fn graceful_stop(
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    shutdown_tx: Sender<()>,
    guard: Option<ProfilerGuard<'_>>,
    profile_path: Option<String>,
    heap_path: &str,
) {
    usecase::graceful_stop::listen_shutdown().await;
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
    if std::env::var(MALLOC_CONF_ENV).is_ok() {
        generate_profiling_gif(heap_path).await;
    }

    usecase::graceful_stop::graceful_stop(tasks.lock().await.deref_mut()).await
}

async fn generate_profiling_gif(heap_path: &str) {
    let program = match std::env::current_exe()
        .map_err(|e| IngesterError::Usecase(e.to_string()))
        .and_then(|exe| {
            exe.as_path()
                .to_str()
                .ok_or(IngesterError::Usecase("Cannot cast to string".to_string()))
                .map(|s| s.to_string())
        }) {
        Ok(program) => program,
        Err(e) => {
            error!("Cannot get program path: {}", e);
            return;
        }
    };

    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "jeprof --show_bytes --gif {0} {1}/.*.*.*.heap > {1}/profile.gif",
            program, heap_path
        ))
        .output()
        .await
        .expect("failed to execute process");

    if !output.status.success() {
        error!("jeprof: {}", String::from_utf8_lossy(&output.stderr));
    }
}
