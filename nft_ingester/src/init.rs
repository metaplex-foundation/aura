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
) {
    usecase::graceful_stop::listen_shutdown().await;
    keep_running.store(false, Ordering::SeqCst);
    let _ = shutdown_tx.send(());

    usecase::graceful_stop::graceful_stop(tasks.lock().await.deref_mut()).await
}
