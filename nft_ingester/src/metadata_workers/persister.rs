use std::sync::Arc;

use interface::{
    error::JsonDownloaderError,
    json::{JsonDownloadResult, JsonPersister},
};
use tokio::{sync::mpsc::Receiver, task::JoinSet, time::Duration};
use tracing::{debug, error};

pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SEC: u64 = 60;
pub const SLEEP_TIME: u64 = 1;

pub struct TasksPersister<T: JsonPersister + Send + Sync + 'static> {
    pub persister: Arc<T>,
    pub json_receiver: Receiver<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
}

impl<T: JsonPersister + Send + Sync + 'static> TasksPersister<T> {
    pub fn new(
        persister: Arc<T>,
        json_receiver: Receiver<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
    ) -> Self {
        Self { persister, json_receiver }
    }

    pub async fn run(
        mut self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        tasks: &mut JoinSet<()>,
    ) {
        tasks.spawn(async move {
            let mut buffer = vec![];
            let mut clock = tokio::time::Instant::now();
            let persister = self.persister.clone();

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    if let Err(e) = self.persister.persist_response(buffer).await {
                        error!("Could not save JSONs to the storage: {:?}", e);
                    } else {
                        debug!("Saved metadata successfully...");
                    }
                }
                _ = async {
                    loop {
                        if buffer.len() > JSON_BATCH
                            || tokio::time::Instant::now() - clock
                                > Duration::from_secs(WIPE_PERIOD_SEC)
                        {
                            if let Err(e) = persister.persist_response(std::mem::take(&mut buffer)).await {
                                error!("Could not save JSONs to the storage: {:?}", e);
                            } else {
                                debug!("Saved metadata successfully...");
                            }

                            clock = tokio::time::Instant::now();
                        }

                        if let Some(data_to_persist) = self.json_receiver.recv().await {
                            buffer.push(data_to_persist);
                        } else {
                            error!("Could not get JSON data to save from the channel because it was closed");
                        }

                        tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                    }
                } => {}
            }
        });
    }
}
