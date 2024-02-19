use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use entities::enums::TaskStatus;
use log::{error, info};
use metrics_utils::utils::start_metrics;
use metrics_utils::{JsonMigratorMetricsConfig, MetricState, MetricStatus, MetricsTrait};
use tokio::sync::{broadcast, Mutex};
use tokio::task::{JoinError, JoinSet};

use nft_ingester::config::{
    init_logger, setup_config, JsonMigratorConfig, JsonMigratorMode, JSON_MIGRATOR_CONFIG_PREFIX,
};
use nft_ingester::db_v2::{DBClient, Task};
use nft_ingester::error::IngesterError;
use nft_ingester::init::graceful_stop;
use rocks_db::offchain_data::OffChainData;
use rocks_db::{AssetDynamicDetails, Storage};

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: JsonMigratorConfig = setup_config(JSON_MIGRATOR_CONFIG_PREFIX);

    init_logger(&config.get_log_level());

    info!("Started...");

    let database_pool = DBClient::new(&config.database_config.clone()).await?;
    let pg_client = Arc::new(database_pool);

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();
    start_metrics(metrics_state.registry, Some(config.metrics_port)).await;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let keep_running = Arc::new(AtomicBool::new(true));

    let storage = Storage::open(&config.json_target_db.clone(), mutexed_tasks.clone()).unwrap();

    let target_storage = Arc::new(storage);

    let source_storage = Storage::open(&config.json_source_db, mutexed_tasks.clone()).unwrap();
    let source_storage = Arc::new(source_storage);

    let json_migrator = JsonMigrator::new(
        pg_client.clone(),
        source_storage.clone(),
        target_storage.clone(),
        metrics_state.json_migrator_metrics.clone(),
        config,
    );

    let cloned_keep_running = keep_running.clone();
    let cloned_tasks = mutexed_tasks.clone();

    mutexed_tasks.lock().await.spawn(tokio::spawn(async move {
        json_migrator.run(cloned_keep_running, cloned_tasks).await;
    }));

    // useless thing in this context
    let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

    graceful_stop(mutexed_tasks, keep_running.clone(), shutdown_tx, None, None).await;

    Ok(())
}

pub struct JsonMigrator {
    pub database_pool: Arc<DBClient>,
    pub source_rocks_db: Arc<Storage>,
    pub target_rocks_db: Arc<Storage>,
    pub metrics: Arc<JsonMigratorMetricsConfig>,
    pub config: JsonMigratorConfig,
}

impl JsonMigrator {
    pub fn new(
        database_pool: Arc<DBClient>,
        source_rocks_db: Arc<Storage>,
        target_rocks_db: Arc<Storage>,
        metrics: Arc<JsonMigratorMetricsConfig>,
        config: JsonMigratorConfig,
    ) -> Self {
        Self {
            database_pool,
            source_rocks_db,
            target_rocks_db,
            metrics,
            config,
        }
    }

    pub async fn run(
        &self,
        keep_running: Arc<AtomicBool>,
        tasks: Arc<Mutex<JoinSet<core::result::Result<(), JoinError>>>>,
    ) {
        match self.config.work_mode {
            JsonMigratorMode::Full => {
                info!("Launch JSON migrator in full mode");

                info!("Start migrate JSONs...");
                self.migrate_jsons(keep_running.clone()).await;

                info!("JSONs are migrated. Start setting tasks...");
                self.set_tasks(keep_running, tasks).await;
                info!("Tasks are set!");
            }
            JsonMigratorMode::JsonsOnly => {
                info!("Launch JSON migrator in jsons only mode");

                info!("Start migrate JSONs...");
                self.migrate_jsons(keep_running.clone()).await;
                info!("JSONs are migrated!");
            }
            JsonMigratorMode::TasksOnly => {
                info!("Launch JSON migrator in tasks only mode");

                info!("Start set tasks...");
                self.set_tasks(keep_running, tasks).await;
                info!("Tasks are set!");
            }
        }
    }

    pub async fn migrate_jsons(&self, keep_running: Arc<AtomicBool>) {
        let all_available_jsons = self.source_rocks_db.asset_offchain_data.iter_end();

        for json in all_available_jsons {
            if !keep_running.load(Ordering::SeqCst) {
                info!("JSON migrator is stopped");
                break;
            }

            match json {
                Ok((_key, value)) => {
                    let metadata = bincode::deserialize::<OffChainData>(&value);

                    match metadata {
                        Ok(metadata) => {
                            match self
                                .target_rocks_db
                                .asset_offchain_data
                                .put(metadata.url.clone(), metadata)
                            {
                                Ok(_) => {
                                    self.metrics
                                        .inc_jsons_migrated("json_migrated", MetricStatus::SUCCESS);
                                }
                                Err(e) => {
                                    self.metrics
                                        .inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                                    error!("offchain_data.put: {}", e)
                                }
                            };
                        }
                        Err(e) => {
                            self.metrics
                                .inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                            error!("bincode::deserialize: {}", e)
                        }
                    }
                }
                Err(e) => {
                    self.metrics
                        .inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                    error!("offchain_data.iter_end: {}", e)
                }
            }
        }
    }

    pub async fn set_tasks(
        &self,
        keep_running: Arc<AtomicBool>,
        tasks: Arc<Mutex<JoinSet<core::result::Result<(), JoinError>>>>,
    ) {
        let dynamic_asset_details = self.target_rocks_db.asset_dynamic_data.iter_end();

        let tasks_buffer = Arc::new(Mutex::new(Vec::new()));

        let cloned_keep_running = keep_running.clone();
        let cloned_tasks_buffer = tasks_buffer.clone();
        let cloned_pg_client = self.database_pool.clone();
        let cloned_metrics = self.metrics.clone();

        let tasks_batch_to_insert = 1000;

        tasks.lock().await.spawn(tokio::spawn(async move {
            loop {
                if !cloned_keep_running.load(Ordering::SeqCst) {
                    info!("Worker to clean tasks buffer is stopped");
                    return;
                }

                let mut tasks_buffer = cloned_tasks_buffer.lock().await;

                if tasks_buffer.is_empty() {
                    drop(tasks_buffer);
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    continue;
                }

                let end_point = {
                    if tasks_buffer.len() < tasks_batch_to_insert {
                        tasks_buffer.len()
                    } else {
                        tasks_batch_to_insert
                    }
                };

                let mut tasks_to_insert = tasks_buffer.drain(0..end_point).collect::<Vec<Task>>();

                drop(tasks_buffer);

                let res = cloned_pg_client.insert_tasks(&mut tasks_to_insert).await;
                match res {
                    Ok(_) => {
                        cloned_metrics.inc_tasks_set(
                            "tasks_set",
                            MetricStatus::SUCCESS,
                            tasks_to_insert.len() as u64,
                        );
                    }
                    Err(e) => {
                        cloned_metrics.inc_tasks_set(
                            "tasks_set",
                            MetricStatus::FAILURE,
                            tasks_to_insert.len() as u64,
                        );
                        error!("insert_tasks: {}", e)
                    }
                }
            }
        }));

        for dynamic_details in dynamic_asset_details {
            if !keep_running.load(Ordering::SeqCst) {
                info!("Setting tasks for JSONs is stopped");
                break;
            }

            match dynamic_details {
                Ok((_key, value)) => {
                    let dynamic_details = bincode::deserialize::<AssetDynamicDetails>(&value);

                    match dynamic_details {
                        Ok(data) => {
                            let downloaded_json = self
                                .target_rocks_db
                                .asset_offchain_data
                                .get(data.url.value.clone());

                            if let Err(e) = downloaded_json {
                                error!("asset_offchain_data.get: {}", e);
                                continue;
                            }

                            let mut task = Task {
                                ofd_metadata_url: data.url.value.clone(),
                                ofd_locked_until: None,
                                ofd_attempts: 0,
                                ofd_max_attempts: 10,
                                ofd_error: None,
                                ..Default::default()
                            };

                            let mut buff = tasks_buffer.lock().await;

                            match downloaded_json.unwrap() {
                                Some(_) => {
                                    task.ofd_status = TaskStatus::Success;

                                    buff.push(task);
                                }
                                None => {
                                    buff.push(task);
                                }
                            }

                            self.metrics
                                .set_tasks_buffer("tasks_buffer", buff.len() as i64);
                        }
                        Err(e) => {
                            error!("bincode::deserialize: {}", e)
                        }
                    }
                }
                Err(e) => {
                    error!("asset_dynamic_data.iter_end: {}", e)
                }
            }
        }
    }
}
