use std::sync::Arc;

use interface::{
    error::JsonDownloaderError,
    json_metadata::{JsonPersister, MetadataDownloadResult},
};
use tokio::{
    sync::{mpsc::Receiver, Mutex},
    task::JoinSet,
    time::{sleep, Duration},
};
use tracing::{debug, error};

pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SEC: u64 = 60;
pub const SLEEP_TIME: u64 = 1;

pub struct TasksPersister<T: JsonPersister + Send + Sync + 'static> {
    pub persister: Arc<T>,
    pub json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    pub shutdown_rx: tokio::sync::broadcast::Receiver<()>,
}

impl<T: JsonPersister + Send + Sync + 'static> TasksPersister<T> {
    pub fn new(
        persister: Arc<T>,
        json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
        shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        Self { persister, json_receiver, shutdown_rx }
    }

    pub async fn run(mut self, tasks: Arc<Mutex<JoinSet<()>>>) {
        tasks.lock().await.spawn(async move {
            let mut buffer = vec![];
            let persister = self.persister.clone();

            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    if let Err(e) = self.persister.persist_response(buffer).await {
                        error!("Could not save JSONs to the storage: {:?}", e);
                    } else {
                        debug!("Saved metadata successfully...");
                    }
                }
                _ = Self::process_persisting(persister, self.json_receiver, &mut buffer) => {}
            }
        });
    }

    async fn process_persisting(
        persister: Arc<T>,
        mut json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
        buffer: &mut Vec<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    ) {
        loop {
            tokio::select! {
                _ = sleep(Duration::from_secs(WIPE_PERIOD_SEC)) => {
                    if let Err(e) = persister.persist_response(std::mem::take(buffer)).await {
                        error!("Could not save JSONs to the storage: {:?}", e);
                    } else {
                        debug!("Saved metadata successfully...");
                    }
                }
                received_metadata = json_receiver.recv() => {
                    if let Some(data_to_persist) = received_metadata {
                        buffer.push(data_to_persist);

                        if buffer.len() > JSON_BATCH {
                            if let Err(e) = persister.persist_response(std::mem::take(buffer)).await {
                                error!("Could not save JSONs to the storage: {:?}", e);
                            } else {
                                debug!("Saved metadata successfully...");
                            }
                        }
                    } else {
                        error!("Could not get JSON data to save from the channel because it was closed");
                        break;
                    }
                    sleep(Duration::from_secs(SLEEP_TIME)).await;
                }
            }
        }
    }
}
