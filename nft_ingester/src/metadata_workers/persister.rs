use std::sync::Arc;

use interface::{
    error::JsonDownloaderError,
    json_metadata::{JsonPersister, MetadataDownloadResult},
};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SECS: u64 = 60;
pub const SLEEP_TIME_SECS: u64 = 1;

pub struct TasksPersister<T: JsonPersister + Send + Sync + 'static> {
    persister: Arc<T>,
    json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    cancellation_token: CancellationToken,
}

impl<T: JsonPersister + Send + Sync + 'static> TasksPersister<T> {
    pub fn new(
        persister: Arc<T>,
        json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self { persister, json_receiver, cancellation_token }
    }

    pub async fn run(self) {
        let mut buffer = vec![];
        let persister = self.persister.clone();

        tokio::select! {
            _ = self.cancellation_token.cancelled() => {
                if let Err(e) = self.persister.persist_response(buffer).await {
                    error!("Could not save JSONs to the storage: {:?}", e);
                } else {
                    debug!("Saved metadata successfully...");
                }
            }
            _ = Self::process_persisting(persister, self.json_receiver, &mut buffer) => {}
        }
    }

    async fn process_persisting(
        persister: Arc<T>,
        mut json_receiver: Receiver<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
        buffer: &mut Vec<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    ) {
        loop {
            tokio::select! {
                _ = sleep(Duration::from_secs(WIPE_PERIOD_SECS)) => {
                    Self::persist_metadata(persister.clone(), std::mem::take(buffer)).await;
                }
                received_metadata = json_receiver.recv() => {
                    if let Some(data_to_persist) = received_metadata {
                        buffer.push(data_to_persist);

                        if buffer.len() > JSON_BATCH {
                            Self::persist_metadata(persister.clone(), std::mem::take(buffer)).await;
                        }
                    } else {
                        Self::persist_metadata(persister.clone(), std::mem::take(buffer)).await;
                        error!("Could not get JSON data to save from the channel because it was closed");
                        break;
                    }
                }
            }
        }
    }

    async fn persist_metadata(
        persister: Arc<T>,
        jsons: Vec<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    ) {
        if let Err(e) = persister.persist_response(jsons).await {
            error!(error = ?e, "Could not save JSONs to the storage: {:?}", e);
        } else {
            debug!("Saved metadata successfully...");
        }
    }
}
