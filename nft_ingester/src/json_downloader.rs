use crate::config::{
    setup_config, BackgroundTaskConfig, BackgroundTaskRunnerConfig, INGESTER_CONFIG_PREFIX,
};
use async_trait::async_trait;
use entities::enums::TaskStatus;
use entities::models::JsonDownloadTask;
use interface::error::JsonDownloaderError;
use interface::json::JsonProcessor;
use log::{debug, error};
use metrics_utils::{JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::tasks::UpdatedTask;
use postgre_client::PgClient;
use reqwest::{Client, ClientBuilder};
use rocks_db::{offchain_data::OffChainData, Storage};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{self, Instant};

pub const JSON_CONTENT_TYPE: &str = "application/json";

pub struct JsonDownloader {
    pub db_client: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub config: BackgroundTaskRunnerConfig,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
}

impl JsonDownloader {
    pub async fn new(
        db_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<JsonDownloaderMetricsConfig>,
    ) -> Self {
        let config: BackgroundTaskConfig = setup_config(INGESTER_CONFIG_PREFIX);

        Self {
            db_client,
            config: config.background_task_runner_config.unwrap_or_default(),
            metrics,
            rocks_db,
        }
    }
}

pub async fn run(json_downloader: Arc<JsonDownloader>, keep_running: Arc<AtomicBool>) {
    let mut tasks_set = JoinSet::new();

    let tasks = Arc::new(Mutex::new(vec![]));

    for _ in (0..json_downloader.config.num_of_parallel_workers)
        .collect::<std::vec::Vec<i32>>()
        .iter()
    {
        let keep_running = keep_running.clone();
        let tasks = tasks.clone();
        let num_of_tasks = json_downloader.config.num_of_parallel_workers;

        let json_downloader = json_downloader.clone();

        tasks_set.spawn(async move {
            let tasks = tasks.clone();

            while keep_running.load(Ordering::SeqCst) {
                let mut locket_tasks = tasks.lock().await;

                if locket_tasks.is_empty() {
                    let tasks = json_downloader
                        .db_client
                        .get_pending_tasks(num_of_tasks)
                        .await;

                    if let Ok(t) = tasks {
                        *locket_tasks = t;
                    } else {
                        error!("Error getting pending tasks: {}", tasks.err().unwrap());
                        continue;
                    }
                }

                let task_to_process = locket_tasks.pop();

                drop(locket_tasks);

                if let Some(task) = task_to_process {
                    let begin_processing = Instant::now();

                    let response = json_downloader
                        .download_file(task.metadata_url.clone())
                        .await;

                    json_downloader.metrics.set_latency_task_executed(
                        "json_downloader",
                        begin_processing.elapsed().as_millis() as f64,
                    );

                    json_downloader.persist_response(response, task).await;
                } else {
                    error!("Error getting task from array");
                }
            }
        });
    }

    while tasks_set.join_next().await.is_some() {}
}

#[async_trait]
impl JsonProcessor for JsonDownloader {
    async fn download_file(&self, url: String) -> Result<String, JsonDownloaderError> {
        let client = ClientBuilder::new()
            .timeout(time::Duration::from_secs(5))
            .build()
            .map_err(|e| format!("Failed to create client: {:?}", e))
            .unwrap();
        let response = Client::get(&client, url)
            .send()
            .await
            .map_err(|e| format!("Failed to make request: {:?}", e));

        match response {
            Ok(response) => {
                if let Some(content_header) = response.headers().get("Content-Type") {
                    match content_header.to_str() {
                        Ok(header) => {
                            if !header.contains(JSON_CONTENT_TYPE) {
                                return Err(JsonDownloaderError::GotNotJsonFile);
                            }
                        }
                        Err(_) => {
                            return Err(JsonDownloaderError::CouldNotReadHeader);
                        }
                    }
                }

                if response.status() != reqwest::StatusCode::OK {
                    return Err(JsonDownloaderError::ErrorStatusCode(
                        response.status().as_str().to_string(),
                    ));
                } else {
                    let metadata_body = response.text().await;
                    if let Ok(metadata) = metadata_body {
                        return Ok(metadata.trim().replace('\0', ""));
                    } else {
                        Err(JsonDownloaderError::CouldNotDeserialize)
                    }
                }
            }
            Err(e) => Err(JsonDownloaderError::ErrorDownloading(e.to_string())),
        }
    }

    async fn persist_response(
        &self,
        download_response: Result<String, JsonDownloaderError>,
        task: JsonDownloadTask,
    ) -> Option<String> {
        match download_response {
            Ok(json_file) => {
                self.rocks_db
                    .asset_offchain_data
                    .put(
                        task.metadata_url.clone(),
                        OffChainData {
                            url: task.metadata_url.clone(),
                            metadata: json_file.clone(),
                        },
                    )
                    .unwrap();
                let data_to_insert = UpdatedTask {
                    status: TaskStatus::Success,
                    metadata_url: task.metadata_url,
                    attempts: task.attempts + 1,
                    error: "".to_string(),
                };
                self.db_client
                    .update_tasks(vec![data_to_insert])
                    .await
                    .unwrap();

                debug!("Saved metadata successfully...");
                self.metrics.inc_tasks("json", MetricStatus::SUCCESS);

                return Some(json_file);
            }
            Err(json_err) => match json_err {
                JsonDownloaderError::GotNotJsonFile => {
                    let data_to_insert = UpdatedTask {
                        status: TaskStatus::Success,
                        metadata_url: task.metadata_url.clone(),
                        attempts: task.attempts + 1,
                        error: "".to_string(),
                    };
                    self.db_client
                        .update_tasks(vec![data_to_insert])
                        .await
                        .unwrap();

                    self.rocks_db
                        .asset_offchain_data
                        .put(
                            task.metadata_url.clone(),
                            OffChainData {
                                url: task.metadata_url,
                                metadata: "".to_string(),
                            },
                        )
                        .unwrap();

                    self.metrics.inc_tasks("media", MetricStatus::SUCCESS);
                }
                JsonDownloaderError::CouldNotDeserialize => {
                    let data_to_insert = UpdatedTask {
                        status: TaskStatus::Failed,
                        metadata_url: task.metadata_url,
                        attempts: task.attempts + 1,
                        error: "Failed to deserialize metadata body".to_string(),
                    };
                    self.db_client
                        .update_tasks(vec![data_to_insert])
                        .await
                        .unwrap();
                    self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                }
                JsonDownloaderError::CouldNotReadHeader => {
                    error!("Could not convert header into str");
                    self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                }
                JsonDownloaderError::ErrorStatusCode(err) => {
                    let status = {
                        if task.attempts >= task.max_attempts {
                            TaskStatus::Failed
                        } else {
                            TaskStatus::Pending
                        }
                    };
                    let data_to_insert = UpdatedTask {
                        status,
                        metadata_url: task.metadata_url,
                        attempts: task.attempts + 1,
                        error: err,
                    };
                    self.db_client
                        .update_tasks(vec![data_to_insert])
                        .await
                        .unwrap();

                    self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                }
                JsonDownloaderError::ErrorDownloading(err) => {
                    debug!("Error downloading metadata: {}", err);
                    self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                }
            },
        }
        None
    }
}
