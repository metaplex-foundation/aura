use crate::config::{
    setup_config, BackgroundTaskConfig, BackgroundTaskRunnerConfig, INGESTER_CONFIG_PREFIX,
};
use crate::db_v2::{DBClient, TaskStatus, UpdatedTask};
use log::{debug, error, info};
use metrics_utils::{JsonDownloaderMetricsConfig, MetricStatus};
use reqwest::{Client, ClientBuilder};
use rocks_db::{offchain_data::OffChainData, Storage};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::{self, Instant};

pub struct JsonDownloader {
    pub db_client: Arc<DBClient>,
    pub rocks_db: Arc<Storage>,
    pub config: BackgroundTaskRunnerConfig,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
}

impl JsonDownloader {
    pub async fn new(rocks_db: Arc<Storage>, metrics: Arc<JsonDownloaderMetricsConfig>) -> Self {
        let config: BackgroundTaskConfig = setup_config(INGESTER_CONFIG_PREFIX);
        let database_pool = DBClient::new(&config.database_config).await.unwrap();

        Self {
            db_client: Arc::new(database_pool),
            config: config.background_task_runner_config.unwrap_or_default(),
            metrics,
            rocks_db,
        }
    }

    pub async fn run(&self, keep_running: Arc<AtomicBool>) {
        let retry_interval = tokio::time::Duration::from_secs(self.config.retry_interval.unwrap());

        let mut interval = time::interval(retry_interval);

        let mut tasks_set = JoinSet::new();

        while keep_running.load(Ordering::SeqCst) {
            interval.tick().await; // ticks immediately

            let tasks = self.db_client.get_pending_tasks().await;

            match tasks {
                Ok(tasks) => {
                    debug!("tasks that need to be executed: {}", tasks.len());

                    for task in tasks {
                        let cloned_db_client = self.db_client.clone();
                        let cloned_metrics = self.metrics.clone();
                        let cloned_rocks = self.rocks_db.clone();
                        tasks_set.spawn(async move {
                            let begin_processing = Instant::now();

                            let client = ClientBuilder::new()
                                .timeout(time::Duration::from_secs(5))
                                .build()
                                .map_err(|e| format!("Failed to create client: {:?}", e))
                                .unwrap();
                            let response = Client::get(&client, task.metadata_url.clone())
                                .send()
                                .await
                                .map_err(|e| format!("Failed to make request: {:?}", e));

                            cloned_metrics.set_latency_task_executed(
                                "json_downloader",
                                begin_processing.elapsed().as_millis() as f64,
                            );

                            match response {
                                Ok(response) => {
                                    if response.status() != reqwest::StatusCode::OK {
                                        let status = {
                                            if task.attempts >= task.max_attempts {
                                                TaskStatus::Failed
                                            } else {
                                                TaskStatus::Pending
                                            }
                                        };
                                        let data_to_insert = UpdatedTask {
                                            status,
                                            metadata_url_key: task.metadata_url_key,
                                            attempts: task.attempts + 1,
                                            error: response.status().as_str().to_string(),
                                        };
                                        cloned_db_client
                                            .update_tasks(vec![data_to_insert])
                                            .await
                                            .unwrap();

                                        cloned_metrics.inc_tasks(MetricStatus::FAILURE);
                                    } else {
                                        let metadata_body = response.text().await;
                                        if let Ok(metadata) = metadata_body {
                                            cloned_rocks
                                                .asset_offchain_data
                                                .put(
                                                    task.metadata_url.clone(),
                                                    OffChainData {
                                                        url: task.metadata_url.clone(),
                                                        metadata: metadata.trim().replace('\0', ""),
                                                    },
                                                )
                                                .unwrap();
                                            let data_to_insert = UpdatedTask {
                                                status: TaskStatus::Success,
                                                metadata_url_key: task.metadata_url_key,
                                                attempts: task.attempts + 1,
                                                error: "".to_string(),
                                            };
                                            cloned_db_client
                                                .update_tasks(vec![data_to_insert])
                                                .await
                                                .unwrap();

                                            info!("Saved metadata successfully...");
                                            cloned_metrics.inc_tasks(MetricStatus::SUCCESS);
                                        } else {
                                            let data_to_insert = UpdatedTask {
                                                status: TaskStatus::Failed,
                                                metadata_url_key: task.metadata_url_key,
                                                attempts: task.attempts + 1,
                                                error: "Failed to deserialize metadata body"
                                                    .to_string(),
                                            };
                                            cloned_db_client
                                                .update_tasks(vec![data_to_insert])
                                                .await
                                                .unwrap();
                                            cloned_metrics.inc_tasks(MetricStatus::FAILURE);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Error downloading metadata: {}", e);
                                    cloned_metrics.inc_tasks(MetricStatus::FAILURE);
                                }
                            }
                        });
                    }
                    while tasks_set.join_next().await.is_some() {}
                }
                Err(e) => {
                    error!("Error getting pending tasks: {}", e);
                }
            }
        }
    }
}
