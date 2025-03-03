use std::{future::Future, sync::OnceLock};

use tokio::{
    sync::Mutex,
    task::{AbortHandle, JoinSet},
};

static EXECUTOR: Mutex<OnceLock<JoinSet<()>>> = Mutex::const_new(OnceLock::new());

pub fn spawn<T, F: Future<Output = T> + Send + 'static>(future: F) -> AbortHandle {
    tokio::task::block_in_place(|| {
        let mut executor_guard = EXECUTOR.blocking_lock();
        let _ = executor_guard.get_or_init(JoinSet::new);
        let executor =
            executor_guard.get_mut().expect("executor join set to be initialized upon access");
        executor.spawn(async move {
            let _ = future.await;
        })
    })
}

pub(crate) async fn shutdown() {
    let mut executor_guard = EXECUTOR.lock().await;
    if let Some(executor) = executor_guard.get_mut() {
        while executor.join_next().await.is_some() {}
    }
}
