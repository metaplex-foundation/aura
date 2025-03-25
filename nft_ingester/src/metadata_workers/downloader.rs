use std::sync::Arc;

use entities::models::MetadataDownloadTask;
use interface::{
    error::JsonDownloaderError,
    json_metadata::{JsonDownloader, MetadataDownloadResult},
};
use tokio::{
    sync::{
        mpsc::{error::TryRecvError, Receiver, Sender},
        Mutex,
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::metadata_workers::{json_worker::JsonWorker, TaskType};

pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
pub const PENDING_TASKS_WORKERS_RATIO: i32 = 5;

pub struct MetadataDownloader {
    pub json_worker: Arc<JsonWorker>,
    pub metadata_to_persist_tx:
        Sender<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    pub pending_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub refresh_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub cancellation_token: CancellationToken,
}

impl MetadataDownloader {
    pub fn new(
        json_worker: Arc<JsonWorker>,
        metadata_to_persist_tx: Sender<(
            String,
            Result<MetadataDownloadResult, JsonDownloaderError>,
        )>,
        pending_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        refresh_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            json_worker,
            metadata_to_persist_tx,
            pending_metadata_tasks_rx,
            refresh_metadata_tasks_rx,
            cancellation_token,
        }
    }

    pub async fn run(self) {
        let parallel_tasks_for_refresh =
            self.json_worker.num_of_parallel_workers / PENDING_TASKS_WORKERS_RATIO + 1;
        let parallel_tasks_for_pending =
            self.json_worker.num_of_parallel_workers - parallel_tasks_for_refresh + 1;

        tokio::join!(
            self.download_tasks(parallel_tasks_for_pending, TaskType::Pending),
            self.download_tasks(parallel_tasks_for_refresh, TaskType::Refresh),
        );
    }

    pub async fn download_tasks(&self, parallel_tasks: i32, task_type: TaskType) {
        let task_receiver = match task_type {
            TaskType::Pending => self.pending_metadata_tasks_rx.clone(),
            TaskType::Refresh => self.refresh_metadata_tasks_rx.clone(),
        };

        for _ in 0..parallel_tasks {
            let cancellation_token = self.cancellation_token.child_token();
            let task_receiver = task_receiver.clone();
            let metadata_to_persist_tx = self.metadata_to_persist_tx.clone();
            let json_worker = self.json_worker.clone();
            usecase::executor::spawn(async move {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("Shutting down metadata downloader worker");
                    },
                    _ = Self::process_downloading(task_receiver, metadata_to_persist_tx, json_worker) => {},
                }
            });
        }
    }

    async fn process_downloading(
        tasks_receiver: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        metadata_to_persist_tx: Sender<(
            String,
            Result<MetadataDownloadResult, JsonDownloaderError>,
        )>,
        json_worker: Arc<JsonWorker>,
    ) {
        loop {
            match tasks_receiver.lock().await.try_recv() {
                Ok(task) => {
                    let begin_processing = Instant::now();
                    let response = json_worker.download_file(&task, CLIENT_TIMEOUT).await;
                    json_worker.metrics.set_latency_task_executed(
                        "json_downloader",
                        begin_processing.elapsed().as_millis() as f64,
                    );
                    if let Err(err) =
                        metadata_to_persist_tx.send((task.metadata_url, response)).await
                    {
                        error!(
                            "Error during sending JSON download result to the channel: {}",
                            err.to_string()
                        );
                    }
                },
                Err(TryRecvError::Disconnected) => {
                    break;
                },
                Err(TryRecvError::Empty) => {},
            }
        }
    }
}
