use crate::api::dapi::rpc_asset_convertors::parse_files;
use crate::config::{setup_config, IngesterConfig, INGESTER_CONFIG_PREFIX};
use async_trait::async_trait;
use entities::enums::TaskStatus;
use entities::models::{JsonDownloadTask, OffChainData};
use interface::error::JsonDownloaderError;
use interface::json::{JsonDownloadResult, JsonDownloader, JsonPersister};
use metrics_utils::red::RequestErrorDurationMetrics;
use metrics_utils::{JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::tasks::UpdatedTask;
use postgre_client::PgClient;
use reqwest::ClientBuilder;
use rocks_db::asset_previews::UrlToDownload;
use rocks_db::Storage;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error};
use url::Url;

pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SEC: u64 = 60;
pub const SLEEP_TIME: u64 = 1;
pub const CLIENT_TIMEOUT: u64 = 5;

pub struct JsonWorker {
    pub db_client: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub num_of_parallel_workers: i32,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
    pub red_metrics: Arc<RequestErrorDurationMetrics>,
}

impl JsonWorker {
    pub async fn new(
        db_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<JsonDownloaderMetricsConfig>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        parallel_json_downloaders: i32,
    ) -> Self {
        Self {
            db_client,
            num_of_parallel_workers: parallel_json_downloaders,
            metrics,
            red_metrics,
            rocks_db,
        }
    }
}

pub struct TasksStreamer {
    pub db_conn: Arc<PgClient>,
    pub sender: tokio::sync::mpsc::Sender<JsonDownloadTask>,
    // to check if it's empty and populate it
    pub receiver: Arc<Mutex<tokio::sync::mpsc::Receiver<JsonDownloadTask>>>,
}

impl TasksStreamer {
    pub fn new(
        db_conn: Arc<PgClient>,
        sender: tokio::sync::mpsc::Sender<JsonDownloadTask>,
        receiver: Arc<Mutex<tokio::sync::mpsc::Receiver<JsonDownloadTask>>>,
    ) -> Self {
        Self {
            db_conn,
            sender,
            receiver,
        }
    }

    pub async fn run(self, rx: Receiver<()>, num_of_tasks: i32, tasks: &mut JoinSet<()>) {
        tasks.spawn(async move {
            while rx.is_empty() {
                let locked_receiver = self.receiver.lock().await;
                let is_empty = locked_receiver.is_empty();
                drop(locked_receiver);

                if is_empty {
                    let tasks = self.db_conn.get_pending_tasks(num_of_tasks).await;

                    match tasks {
                        Ok(tasks) => {
                            for task in tasks.iter() {
                                if let Err(err) = self.sender.send(task.clone()).await {
                                    error!(
                                        "Error during sending task to the tasks channel: {}",
                                        err.to_string()
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            error!("Error while selecting tasks for JsonDownloader: {}", err);
                            tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                            continue;
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                }
            }
        });
    }
}

pub struct TasksPersister<T: JsonPersister + Send + Sync + 'static> {
    pub persister: Arc<T>,
    pub receiver:
        tokio::sync::mpsc::Receiver<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
}

impl<T: JsonPersister + Send + Sync + 'static> TasksPersister<T> {
    pub fn new(
        persister: Arc<T>,
        receiver: tokio::sync::mpsc::Receiver<(
            String,
            Result<JsonDownloadResult, JsonDownloaderError>,
        )>,
    ) -> Self {
        Self {
            persister,
            receiver,
        }
    }

    pub async fn run(mut self, rx: Receiver<()>, tasks: &mut JoinSet<()>) {
        tasks.spawn(async move {
            let mut buffer = vec![];
            let mut clock = tokio::time::Instant::now();

            while rx.is_empty() {
                if buffer.len() > JSON_BATCH
                    || tokio::time::Instant::now() - clock
                        > Duration::from_secs(WIPE_PERIOD_SEC)
                {
                    match self.persister
                        .persist_response(std::mem::take(&mut buffer))
                        .await
                    {
                        Ok(_) => {
                            debug!("Saved metadata successfully...");
                        }
                        Err(e) => {
                            error!("Could not save JSONs to the storage: {:?}", e);
                        }
                    }

                    clock = tokio::time::Instant::now();
                }

                let new_result = self.receiver.try_recv();

                match new_result {
                    Ok(result) => {buffer.push(result)}
                    Err(recv_err) => {
                        if recv_err == TryRecvError::Disconnected {
                            error!("Could not get JSON data to save from the channel because it was closed");
                            break;
                        } else {
                            // it's just empty
                            tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                match self.persister
                        .persist_response(buffer)
                        .await
                    {
                        Ok(_) => {
                            debug!("Saved metadata successfully...");
                        }
                        Err(e) => {
                            error!("Could not save JSONs to the storage: {:?}", e);
                        }
                    }
            }
        });
    }
}

pub async fn run(json_downloader: Arc<JsonWorker>, rx: Receiver<()>) {
    let mut workers_pool = JoinSet::new();

    let num_of_tasks = json_downloader.num_of_parallel_workers;

    let (tasks_tx, tasks_rx) = mpsc::channel(num_of_tasks as usize);
    let tasks_rx = Arc::new(Mutex::new(tasks_rx));

    let (result_tx, result_rx) = mpsc::channel(JSON_BATCH);

    let tasks_streamer = TasksStreamer::new(
        json_downloader.db_client.clone(),
        tasks_tx,
        tasks_rx.clone(),
    );

    let tasks_persister = TasksPersister::new(json_downloader.clone(), result_rx);

    tasks_streamer
        .run(rx.resubscribe(), num_of_tasks, &mut workers_pool)
        .await;
    tasks_persister
        .run(rx.resubscribe(), &mut workers_pool)
        .await;

    for _ in 0..json_downloader.num_of_parallel_workers {
        let cln_rx = rx.resubscribe();
        let json_downloader = json_downloader.clone();
        let tasks_rx = tasks_rx.clone();
        let result_tx = result_tx.clone();

        workers_pool.spawn(async move {
            while cln_rx.is_empty() {
                let mut locked_rx = tasks_rx.lock().await;
                match locked_rx.try_recv() {
                    Ok(task) => {
                        drop(locked_rx);

                        let begin_processing = Instant::now();

                        let response = json_downloader
                            .download_file(task.metadata_url.clone())
                            .await;

                        json_downloader.metrics.set_latency_task_executed(
                            "json_downloader",
                            begin_processing.elapsed().as_millis() as f64,
                        );

                        if let Err(err) = result_tx.send((task.metadata_url, response)).await {
                            error!(
                                "Error during sending JSON download result to the channel: {}",
                                err.to_string()
                            );
                        }
                    }
                    Err(err) => {
                        drop(locked_rx);
                        if err == TryRecvError::Disconnected {
                            error!(
                                "Could not get JSON task from the channel because it was closed"
                            );
                            break;
                        } else {
                            // it's just empty
                            tokio::time::sleep(Duration::from_secs(SLEEP_TIME)).await;
                        }
                    }
                }
            }
        });
    }

    while (workers_pool.join_next().await).is_some() {}
}

#[async_trait]
impl JsonDownloader for JsonWorker {
    async fn download_file(&self, url: String) -> Result<JsonDownloadResult, JsonDownloaderError> {
        let start_time = chrono::Utc::now();
        let client = ClientBuilder::new()
            .timeout(time::Duration::from_secs(CLIENT_TIMEOUT))
            .build()
            .map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to create client: {:?}", e))
            })?;

        // Detect if the URL is an IPFS link
        let parsed_url = if url.starts_with("ipfs://") {
            // Extract the IPFS hash or path
            let ipfs_path = url.trim_start_matches("ipfs://");
            // Choose an IPFS gateway (you can change this to your preferred gateway)
            let gateway_url = format!("https://ipfs.io/ipfs/{}", ipfs_path);
            // Parse the rewritten URL
            let parsed_url = Url::parse(&gateway_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse IPFS URL: {:?}", e))
            })?;
            parsed_url
        } else {
            // Parse the original URL
            let parsed_url = Url::parse(&url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse URL: {:?}", e))
            })?;
            parsed_url
        };

        let host = parsed_url.host_str().unwrap_or("no_host");

        let response = client
            .get(parsed_url.clone())
            .send()
            .await
            .map_err(|e| format!("Failed to make request: {:?}", e));

        match response {
            Ok(response) => {
                self.red_metrics.observe_request(
                    "json_downloader",
                    "download_file",
                    host,
                    start_time,
                );
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
                        url: url.clone(),
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
            }
            Err(e) => {
                self.red_metrics
                    .observe_error("json_downloader", "download_file", host);
                Err(JsonDownloaderError::ErrorDownloading(e.to_string()))
            }
        }
    }
}

#[async_trait]
impl JsonPersister for JsonWorker {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<JsonDownloadResult, JsonDownloaderError>)>,
    ) -> Result<(), JsonDownloaderError> {
        let mut pg_updates = Vec::new();
        let mut rocks_updates = HashMap::new();

        for (metadata_url, result) in results.iter() {
            match result {
                Ok(JsonDownloadResult::JsonContent(json_file)) => {
                    rocks_updates.insert(
                        metadata_url.clone(),
                        OffChainData {
                            url: metadata_url.clone(),
                            metadata: json_file.clone(),
                        },
                    );
                    pg_updates.push(UpdatedTask {
                        status: TaskStatus::Success,
                        metadata_url: metadata_url.clone(),
                        error: "".to_string(),
                    });

                    self.metrics.inc_tasks("json", MetricStatus::SUCCESS);
                }
                Ok(JsonDownloadResult::MediaUrlAndMimeType { url, mime_type }) => {
                    pg_updates.push(UpdatedTask {
                        status: TaskStatus::Success,
                        metadata_url: metadata_url.clone(),
                        error: "".to_string(),
                    });
                    rocks_updates.insert(
                        metadata_url.clone(),
                        OffChainData {
                            url: metadata_url.clone(),
                            metadata: format!(
                                "{{\"image\":\"{}\",\"type\":\"{}\"}}",
                                url, mime_type
                            )
                            .to_string(),
                        },
                    );
                    self.metrics.inc_tasks("media", MetricStatus::SUCCESS);
                }
                Err(json_err) => match json_err {
                    // TODO: this is bullshit, we should handle this in a different way - it's not success
                    JsonDownloaderError::GotNotJsonFile => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Success,
                            metadata_url: metadata_url.clone(),
                            error: "".to_string(),
                        });
                        rocks_updates.insert(
                            metadata_url.clone(),
                            OffChainData {
                                url: metadata_url.clone(),
                                metadata: "".to_string(),
                            },
                        );

                        self.metrics.inc_tasks("media", MetricStatus::SUCCESS);
                    }
                    JsonDownloaderError::CouldNotDeserialize => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            error: "Failed to deserialize metadata body".to_string(),
                        });
                        self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                    }
                    JsonDownloaderError::CouldNotReadHeader => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            error: "Failed to read header".to_string(),
                        });
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                    }
                    JsonDownloaderError::ErrorStatusCode(err) => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            error: err.clone(),
                        });

                        self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                    }
                    JsonDownloaderError::ErrorDownloading(err) => {
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                        // Revert to pending status to retry until max attempts
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Pending,
                            metadata_url: metadata_url.clone(),
                            error: err.clone(),
                        });
                    }
                    _ => {} // No additional processing needed
                },
            }
        }

        if !pg_updates.is_empty() {
            self.db_client
                .update_tasks_and_attempts(pg_updates)
                .await
                .map_err(|e| JsonDownloaderError::IndexStorageError(e.to_string()))?;
        }

        if !rocks_updates.is_empty() {
            let urls_to_download = rocks_updates
                .values()
                .filter(|data| !data.metadata.is_empty())
                .filter_map(|data| parse_files(&data.metadata))
                .flat_map(|files| files.into_iter())
                .filter_map(|file| file.uri)
                .map(|uri| (uri, UrlToDownload::default()))
                .collect::<HashMap<_, _>>();

            self.rocks_db
                .asset_offchain_data
                .put_batch(rocks_updates)
                .await
                .map_err(|e| JsonDownloaderError::MainStorageError(e.to_string()))?;

            if let Err(e) = self
                .rocks_db
                .urls_to_download
                .put_batch(urls_to_download)
                .await
            {
                error!("Unable to persist URLs to download: {e}");
            };
        }

        Ok(())
    }
}
