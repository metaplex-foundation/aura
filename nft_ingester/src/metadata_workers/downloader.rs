use std::sync::Arc;

use async_trait::async_trait;
use entities::models::MetadataDownloadTask;
use interface::{
    error::JsonDownloaderError,
    json::{JsonDownloadResult, NewJsonDownloader},
};
use metrics_utils::{red::RequestErrorDurationMetrics, JsonDownloaderMetricsConfig};
use reqwest::ClientBuilder;
use serde_json::Value;
use tokio::{
    sync::{
        broadcast::{error::RecvError, Receiver},
        mpsc::Sender,
        Mutex,
    },
    task::JoinSet,
    time::{Duration, Instant},
};
use tracing::{debug, error};
use url::Url;

use crate::json_worker::JsonWorker;
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct MetadataDownloader {
    pub worker: Arc<JsonWorker>,
    pub metadata_to_persist_tx: Sender<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
    pub pending_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub refresh_metadata_tasks_rx: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    pub shutdown_rx: Receiver<()>,
    pub parallel_tasks: i32,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
    pub red_metrics: Arc<RequestErrorDurationMetrics>,
}

impl MetadataDownloader {
    pub async fn download_metadata(self) {
        let parallel_tasks_for_refresh = self.parallel_tasks / 5;
        let parallel_tasks_for_pending = self.parallel_tasks - parallel_tasks_for_refresh;

        Self::download_tasks(
            parallel_tasks_for_pending,
            self.shutdown_rx.resubscribe(),
            self.pending_metadata_tasks_rx.clone(),
            self.worker.clone(),
            self.metadata_to_persist_tx.clone(),
        )
        .await;
        Self::download_tasks(
            parallel_tasks_for_refresh,
            self.shutdown_rx.resubscribe(),
            self.refresh_metadata_tasks_rx.clone(),
            self.worker.clone(),
            self.metadata_to_persist_tx.clone(),
        )
        .await;
    }

    pub async fn download_tasks(
        parallel_tasks: i32,
        shutdown_rx: Receiver<()>,
        task_receiver: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
        worker: Arc<JsonWorker>,
        metadata_to_persist_tx: Sender<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
    ) {
        let mut workers_pool = JoinSet::new();
        for _ in 0..parallel_tasks {
            let mut shutdown_rx = shutdown_rx.resubscribe();
            let mut metadata_task_rx = task_receiver.lock().await.resubscribe();
            let worker = worker.clone();
            let metadata_to_persist_tx = metadata_to_persist_tx.clone();

            workers_pool.spawn(async move {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        debug!("Shutting down metadata downloader worker");
                    },
                    _ = async move {
                        loop {
                            match metadata_task_rx.recv().await {
                            Ok(task) => {
                                let begin_processing = Instant::now();

                                let response = NewJsonDownloader::download_file(&*worker, &task, CLIENT_TIMEOUT)
                                    .await;

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
                            Err(err) => {
                                match err {
                                    RecvError::Lagged(lagged) => {
                                        error!("Download Metadada receiver lagged by {} messages", lagged);
                                    }
                                    RecvError::Closed => {
                                        error!("Could not get JSON task from the channel because it was closed");
                                    }
                                }
                            },
                        }
                    }} => {},
                }
            });
        }
    }
}

#[async_trait]
impl NewJsonDownloader for JsonWorker {
    async fn download_file(
        &self,
        download_task: &MetadataDownloadTask,
        timeout: Duration,
    ) -> Result<JsonDownloadResult, JsonDownloaderError> {
        let start_time = chrono::Utc::now();
        let client = ClientBuilder::new().timeout(timeout).build().map_err(|e| {
            JsonDownloaderError::ErrorDownloading(format!("Failed to create client: {:?}", e))
        })?;

        // Detect if the URL is an IPFS link
        let parsed_url = if download_task.metadata_url.starts_with("ipfs://") {
            // Extract the IPFS hash or path
            let ipfs_path = download_task.metadata_url.trim_start_matches("ipfs://");
            // Choose an IPFS gateway (you can change this to your preferred gateway)
            let gateway_url = format!("https://ipfs.io/ipfs/{}", ipfs_path);
            // Parse the rewritten URL
            Url::parse(&gateway_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse IPFS URL: {:?}", e))
            })?
        } else {
            // Parse the original URL
            Url::parse(&download_task.metadata_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse URL: {:?}", e))
            })?
        };

        let host = parsed_url.host_str().unwrap_or("no_host");

        let mut request_builder = client.get(parsed_url.clone());
        match (download_task.etag.clone(), download_task.last_modified_at.clone()) {
            (Some(etag), Some(last_modified_at)) => {
                request_builder = request_builder.header("If-None-Match", etag);
                request_builder = request_builder.header("If-Modified-Since", last_modified_at);
            },
            (Some(etag), _) => {
                request_builder = request_builder.header("If-None-Match", etag);
            },
            (_, Some(last_modified_at)) => {
                request_builder = request_builder.header("If-Modified-Since", last_modified_at);
            },
            _ => {},
        }

        let response =
            request_builder.send().await.map_err(|e| format!("Failed to make request: {:?}", e));

        match response {
            Ok(response) => {
                self.red_metrics.observe_request(
                    "json_downloader",
                    "download_file",
                    host,
                    start_time,
                );

                if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                    return Ok(JsonDownloadResult::NotModified);
                }

                if response.status() != reqwest::StatusCode::OK {
                    return Err(JsonDownloaderError::ErrorStatusCode(
                        response.status().as_str().to_string(),
                    ));
                }

                // Get the Content-Type header
                let content_type = response
                    .headers()
                    .get("Content-Type")
                    .and_then(|ct| ct.to_str().ok())
                    .unwrap_or("");

                // Excluded content types that are definitely not JSON
                let excluded_types = ["audio/", "application/octet-stream"];
                if excluded_types.iter().any(|&t| content_type.starts_with(t)) {
                    return Err(JsonDownloaderError::GotNotJsonFile);
                }

                // Check if the content type is image or video
                if content_type.starts_with("image/") || content_type.starts_with("video/") {
                    // Return the URL and MIME type
                    return Ok(JsonDownloadResult::MediaUrlAndMimeType {
                        url: download_task.metadata_url.clone(),
                        mime_type: content_type.to_string(),
                    });
                }

                let metadata_body = response.text().await;
                if let Ok(metadata) = metadata_body {
                    // Attempt to parse the response as JSON
                    if serde_json::from_str::<Value>(&metadata).is_ok() {
                        return Ok(JsonDownloadResult::JsonContent(
                            metadata.trim().replace('\0', ""),
                        ));
                    } else {
                        return Err(JsonDownloaderError::CouldNotDeserialize);
                    }
                } else {
                    Err(JsonDownloaderError::CouldNotDeserialize)
                }
            },
            Err(e) => {
                self.red_metrics.observe_error("json_downloader", "download_file", host);
                Err(JsonDownloaderError::ErrorDownloading(e.to_string()))
            },
        }
    }

    fn skip_refresh(&self) -> bool {
        self.should_skip_refreshes
    }
}
