use crate::config::{setup_config, IngesterConfig, INGESTER_CONFIG_PREFIX};
use async_trait::async_trait;
use entities::enums::TaskStatus;
use interface::error::JsonDownloaderError;
use interface::json::{JsonDownloader, JsonPersister};
use log::{debug, error};
use metrics_utils::{JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::tasks::UpdatedTask;
use postgre_client::PgClient;
use reqwest::{Client, ClientBuilder};
use rocks_db::{offchain_data::OffChainData, Storage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{self, Duration, Instant};

pub const JSON_CONTENT_TYPE: &str = "application/json";
pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SEC: u64 = 60;

pub struct JsonWorker {
    pub db_client: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub num_of_parallel_workers: i32,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
}

impl JsonWorker {
    pub async fn new(
        db_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<JsonDownloaderMetricsConfig>,
    ) -> Self {
        let config: IngesterConfig = setup_config(INGESTER_CONFIG_PREFIX);

        Self {
            db_client,
            num_of_parallel_workers: config.parallel_json_downloaders,
            metrics,
            rocks_db,
        }
    }
}

pub async fn run(json_downloader: Arc<JsonWorker>, keep_running: Arc<AtomicBool>) {
    let mut workers_pool = JoinSet::new();

    let tasks = Arc::new(Mutex::new(vec![]));
    let results = Arc::new(Mutex::new(vec![]));

    let general_clock = Arc::new(Mutex::new(tokio::time::Instant::now()));

    for _ in (0..json_downloader.num_of_parallel_workers)
        .collect::<std::vec::Vec<i32>>()
        .iter()
    {
        let keep_running = keep_running.clone();
        let tasks = tasks.clone();
        let results = results.clone();
        let num_of_tasks = json_downloader.num_of_parallel_workers;

        let json_downloader = json_downloader.clone();

        let general_clock = general_clock.clone();

        workers_pool.spawn(async move {
            let tasks = tasks.clone();

            while keep_running.load(Ordering::SeqCst) {
                let mut locket_tasks = tasks.lock().await;

                if locket_tasks.is_empty() {
                    let tasks = json_downloader
                        .db_client
                        .get_pending_tasks(num_of_tasks)
                        .await;

                    match tasks {
                        Ok(t) => {
                            *locket_tasks = t;
                        }
                        Err(e) => {
                            error!("Error getting pending tasks: {}", e);
                            continue;
                        }
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

                    let mut result_array = results.lock().await;

                    result_array.push((task.metadata_url, response));

                    let mut clock = general_clock.lock().await;

                    if result_array.len() > JSON_BATCH
                        || tokio::time::Instant::now() - *clock > Duration::from_secs(WIPE_PERIOD_SEC)
                    {
                        match json_downloader
                            .persist_response((*result_array).clone())
                            .await
                        {
                            Ok(_) => {
                                debug!("Saved metadata successfully...");
                            }
                            Err(e) => {
                                error!("Could not save JSONs to the storage: {:?}", e);
                            }
                        }

                        result_array.clear();
                        *clock = tokio::time::Instant::now();
                    }

                    drop(clock);
                    drop(result_array);
                } else {
                    error!("Error getting task from array");
                }
            }
        });
    }

    while workers_pool.join_next().await.is_some() {}
}

#[async_trait]
impl JsonDownloader for JsonWorker {
    async fn download_file(&self, url: String) -> Result<String, JsonDownloaderError> {
        let client = ClientBuilder::new()
            .timeout(time::Duration::from_secs(5))
            .build()
            .map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to create client: {:?}", e))
            })?;
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
}

#[async_trait]
impl JsonPersister for JsonWorker {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<String, JsonDownloaderError>)>,
    ) -> Result<(), JsonDownloaderError> {
        let mut pg_updates = Vec::new();
        let mut rocks_updates = HashMap::new();

        for (metadata_url, result) in results.iter() {
            match &result {
                Ok(json_file) => {
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
                Err(json_err) => match json_err {
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
                    JsonDownloaderError::ErrorDownloading(_err) => {
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                    }
                    _ => {} // intentionally empty because nothing to process
                },
            }
        }

        self.db_client
            .update_tasks_and_attempts(pg_updates)
            .await
            .map_err(|e| JsonDownloaderError::IndexStorageError(e.to_string()))?;

        self.rocks_db
            .asset_offchain_data
            .put_batch(rocks_updates)
            .await
            .map_err(|e| JsonDownloaderError::MainStorageError(e.to_string()))?;

        Ok(())
    }
}
