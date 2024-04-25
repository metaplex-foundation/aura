use crate::config::{setup_config, IngesterConfig, INGESTER_CONFIG_PREFIX};
use async_trait::async_trait;
use entities::enums::TaskStatus;
use entities::models::{JsonDownloadTask, OffChainData};
use interface::error::JsonDownloaderError;
use interface::json::{JsonDownloader, JsonPersister};
use log::{debug, error};
use metrics_utils::{JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::tasks::UpdatedTask;
use postgre_client::PgClient;
use reqwest::{Client, ClientBuilder};
use rocks_db::Storage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{self, Duration, Instant};

pub const JSON_CONTENT_TYPE: &str = "application/json";
pub const JSON_BATCH: usize = 300;
pub const WIPE_PERIOD_SEC: u64 = 60;
pub const SLEEP_TIME: u64 = 1;
pub const CLIENT_TIMEOUT: u64 = 5;

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

    pub async fn run(
        self,
        keep_running: Arc<AtomicBool>,
        num_of_tasks: i32,
        tasks: &mut JoinSet<()>,
    ) {
        tasks.spawn(async move {
            while keep_running.load(Ordering::SeqCst) {
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
    pub receiver: tokio::sync::mpsc::Receiver<(String, Result<String, JsonDownloaderError>)>,
}

impl<T: JsonPersister + Send + Sync + 'static> TasksPersister<T> {
    pub fn new(
        persister: Arc<T>,
        receiver: tokio::sync::mpsc::Receiver<(String, Result<String, JsonDownloaderError>)>,
    ) -> Self {
        Self {
            persister,
            receiver,
        }
    }

    pub async fn run(mut self, keep_running: Arc<AtomicBool>, tasks: &mut JoinSet<()>) {
        tasks.spawn(async move {
            let mut buffer = vec![];
            let mut clock = tokio::time::Instant::now();

            while keep_running.load(Ordering::SeqCst) {
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

pub async fn run(json_downloader: Arc<JsonWorker>, keep_running: Arc<AtomicBool>) {
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
        .run(keep_running.clone(), num_of_tasks, &mut workers_pool)
        .await;
    tasks_persister
        .run(keep_running.clone(), &mut workers_pool)
        .await;

    for _ in 0..json_downloader.num_of_parallel_workers {
        let keep_running = keep_running.clone();
        let json_downloader = json_downloader.clone();
        let tasks_rx = tasks_rx.clone();
        let result_tx = result_tx.clone();

        workers_pool.spawn(async move {
            while keep_running.load(Ordering::SeqCst) {
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
    async fn download_file(&self, url: String) -> Result<String, JsonDownloaderError> {
        let client = ClientBuilder::new()
            .timeout(time::Duration::from_secs(CLIENT_TIMEOUT))
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
                    JsonDownloaderError::ErrorDownloading(err) => {
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                        // back to pending status to try again
                        // until attempts reach its maximum
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Pending,
                            metadata_url: metadata_url.clone(),
                            error: err.clone(),
                        });
                    }
                    _ => {} // intentionally empty because nothing to process
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
            self.rocks_db
                .asset_offchain_data
                .put_batch(rocks_updates)
                .await
                .map_err(|e| JsonDownloaderError::MainStorageError(e.to_string()))?;
        }

        Ok(())
    }
}
