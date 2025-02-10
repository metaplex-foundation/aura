use crate::metadata_workers::json_worker::JsonWorker;
use crate::metadata_workers::TaskType;
use entities::models::MetadataDownloadTask;
use interface::{
    error::JsonDownloaderError,
    json::{JsonDownloader, MetadataDownloadResult},
};
use std::sync::Arc;
use tokio::{
    sync::{
        broadcast::Receiver as ShutdownReceiver,
        mpsc::{error::TryRecvError, Receiver, Sender},
        Mutex,
    },
    task::JoinSet,
    time::{Duration, Instant},
};
use tracing::{debug, error};

pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
pub const PENDING_TASKS_WORKERS_RATIO: i32 = 5;

pub struct MetadataDownloader {
    pub worker: Arc<JsonWorker>,
    pub metadata_to_persist_tx:
        Sender<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    pub pending_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub refresh_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub shutdown_rx: ShutdownReceiver<()>,
}

impl MetadataDownloader {
    pub fn new(
        worker: Arc<JsonWorker>,
        metadata_to_persist_tx: Sender<(
            String,
            Result<MetadataDownloadResult, JsonDownloaderError>,
        )>,
        pending_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        refresh_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        shutdown_rx: ShutdownReceiver<()>,
    ) -> Self {
        Self {
            worker,
            metadata_to_persist_tx,
            pending_metadata_tasks_rx,
            refresh_metadata_tasks_rx,
            shutdown_rx,
        }
    }

    pub async fn run(self, tasks: &mut JoinSet<()>) {
        let parallel_tasks_for_refresh =
            self.worker.num_of_parallel_workers / PENDING_TASKS_WORKERS_RATIO + 1;
        let parallel_tasks_for_pending =
            self.worker.num_of_parallel_workers - parallel_tasks_for_refresh + 1;

        self.download_tasks(parallel_tasks_for_pending, TaskType::Pending, tasks).await;
        self.download_tasks(parallel_tasks_for_refresh, TaskType::Refresh, tasks).await;
    }

    pub async fn download_tasks(
        &self,
        parallel_tasks: i32,
        task_type: TaskType,
        tasks: &mut JoinSet<()>,
    ) {
        let task_receiver = match task_type {
            TaskType::Pending => self.pending_metadata_tasks_rx.clone(),
            TaskType::Refresh => self.refresh_metadata_tasks_rx.clone(),
        };

        for _ in 0..parallel_tasks {
            let mut shutdown_rx = self.shutdown_rx.resubscribe();
            let task_receiver = task_receiver.clone();
            let metadata_to_persist_tx = self.metadata_to_persist_tx.clone();
            let worker = self.worker.clone();
            tasks.spawn(async move {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        debug!("Shutting down metadata downloader worker");
                    },
                    _ = async move {
                        loop {
                            match task_receiver.lock().await.try_recv() {
                            Ok(task) => {
                                let begin_processing = Instant::now();
                                let response = worker.download_file(&task, CLIENT_TIMEOUT).await;
                                worker.metrics.set_latency_task_executed(
                                    "json_downloader",
                                    begin_processing.elapsed().as_millis() as f64,
                                );
                                if let Err(err) = metadata_to_persist_tx.send((task.metadata_url, response)).await {
                                    error!(
                                        "Error during sending JSON download result to the channel: {}",
                                        err.to_string()
                                    );
                                }
                            },
                            Err(err) if err == TryRecvError::Disconnected => {
                                error!("Could not get JSON task from the channel because it was closed");
                            },
                            Err(_) => {},
                        }
                    }} => {},
                }
            });
        }
    }
}
