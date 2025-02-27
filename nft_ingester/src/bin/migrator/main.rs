use std::sync::Arc;

use clap::Parser;
use entities::{enums::TaskStatus, models::Task};
use metrics_utils::{
    red::RequestErrorDurationMetrics, utils::start_metrics, JsonMigratorMetricsConfig, MetricState,
    MetricStatus, MetricsTrait,
};
use nft_ingester::{
    config::{init_logger, JsonMigratorMode, MigratorClapArgs},
    error::IngesterError,
};
use postgre_client::PgClient;
use rocks_db::{
    column::TypedColumn,
    columns::{asset::AssetCompleteDetails, offchain_data::OffChainData},
    generated::asset_generated::asset as fb,
    migrator::MigrationState,
    Storage,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let args = MigratorClapArgs::parse();
    init_logger(&args.log_level);

    info!("Migrator started...");

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    let pg_client = Arc::new(
        PgClient::new(
            &args.pg_database_url,
            args.pg_min_db_connections,
            args.pg_max_db_connections,
            None,
            metrics_state.red_metrics.clone(),
            Some(args.pg_max_query_statement_timeout_secs),
        )
        .await?,
    );

    start_metrics(metrics_state.registry, args.metrics_port).await;

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let cancellation_token = CancellationToken::new();

    let storage = Storage::open(
        args.rocks_json_target_db.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )?;

    let target_storage = Arc::new(storage);
    let source_storage = Storage::open(
        args.rocks_json_source_db.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )?;
    let source_storage = Arc::new(source_storage);

    let json_migrator = JsonMigrator::new(
        pg_client.clone(),
        source_storage.clone(),
        target_storage.clone(),
        metrics_state.json_migrator_metrics.clone(),
        args.migrator_mode,
    );

    usecase::executor::spawn({
        let cancellation_token = cancellation_token.child_token();
        async move {
            json_migrator.run(cancellation_token).await;
        }
    });

    #[cfg(not(feature = "profiling"))]
    usecase::graceful_stop::graceful_shutdown(cancellation_token).await;
    #[cfg(feature = "profiling")]
    nft_ingester::init::graceful_stop(cancellation_token, None, None, "").await;

    Ok(())
}

pub struct JsonMigrator {
    pub database_pool: Arc<PgClient>,
    pub source_rocks_db: Arc<Storage>,
    pub target_rocks_db: Arc<Storage>,
    pub metrics: Arc<JsonMigratorMetricsConfig>,
    pub migrator_mode: JsonMigratorMode,
}

impl JsonMigrator {
    pub fn new(
        database_pool: Arc<PgClient>,
        source_rocks_db: Arc<Storage>,
        target_rocks_db: Arc<Storage>,
        metrics: Arc<JsonMigratorMetricsConfig>,
        migrator_mode: JsonMigratorMode,
    ) -> Self {
        Self { database_pool, source_rocks_db, target_rocks_db, metrics, migrator_mode }
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        match self.migrator_mode {
            JsonMigratorMode::Full => {
                info!("Launch JSON migrator in full mode");

                info!("Start migrate JSONs...");
                self.migrate_jsons(cancellation_token.child_token()).await;

                info!("JSONs are migrated. Start setting tasks...");
                self.set_tasks(cancellation_token).await;
                info!("Tasks are set!");
            },
            JsonMigratorMode::JsonsOnly => {
                info!("Launch JSON migrator in jsons only mode");

                info!("Start migrate JSONs...");
                self.migrate_jsons(cancellation_token).await;
                info!("JSONs are migrated!");
            },
            JsonMigratorMode::TasksOnly => {
                info!("Launch JSON migrator in tasks only mode");

                info!("Start set tasks...");
                self.set_tasks(cancellation_token).await;
                info!("Tasks are set!");
            },
        }
    }

    pub async fn migrate_jsons(&self, cancellation_token: CancellationToken) {
        let all_available_jsons = self.source_rocks_db.asset_offchain_data.iter_end();

        for json in all_available_jsons {
            if cancellation_token.is_cancelled() {
                info!("JSON migrator is stopped");
                break;
            }

            match json {
                Ok((_key, value)) => {
                    let metadata = OffChainData::decode(&value);

                    match metadata {
                        Ok(metadata) => {
                            match self.target_rocks_db.asset_offchain_data.put(
                                metadata.url.clone().expect("Metadata URL cannot be empty"),
                                metadata,
                            ) {
                                Ok(_) => {
                                    self.metrics
                                        .inc_jsons_migrated("json_migrated", MetricStatus::SUCCESS);
                                },
                                Err(e) => {
                                    self.metrics
                                        .inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                                    error!("offchain_data.put: {}", e)
                                },
                            };
                        },
                        Err(e) => {
                            self.metrics.inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                            error!("bincode::deserialize: {}", e)
                        },
                    }
                },
                Err(e) => {
                    self.metrics.inc_jsons_migrated("json_migrated", MetricStatus::FAILURE);
                    error!("offchain_data.iter_end: {}", e)
                },
            }
        }
    }

    pub async fn set_tasks(&self, cancellation_token: CancellationToken) {
        let mut assets_iter = self.target_rocks_db.db.raw_iterator_cf(
            &self.target_rocks_db.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
        );

        let tasks_buffer = Arc::new(Mutex::new(Vec::new()));

        let cloned_tasks_buffer = tasks_buffer.clone();
        let cloned_pg_client = self.database_pool.clone();
        let cloned_metrics = self.metrics.clone();

        let tasks_batch_to_insert = 1000;

        usecase::executor::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                loop {
                    if cancellation_token.is_cancelled() {
                        info!("Worker to clean tasks buffer is stopped");
                        break;
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

                    let mut tasks_to_insert =
                        tasks_buffer.drain(0..end_point).collect::<Vec<Task>>();

                    drop(tasks_buffer);

                    let res =
                        cloned_pg_client.insert_json_download_tasks(&mut tasks_to_insert).await;
                    match res {
                        Ok(_) => {
                            cloned_metrics.inc_tasks_set(
                                "tasks_set",
                                MetricStatus::SUCCESS,
                                tasks_to_insert.len() as u64,
                            );
                        },
                        Err(e) => {
                            cloned_metrics.inc_tasks_set(
                                "tasks_set",
                                MetricStatus::FAILURE,
                                tasks_to_insert.len() as u64,
                            );
                            error!("insert_tasks: {}", e)
                        },
                    }
                }
            }
        });

        assets_iter.seek_to_first();
        while assets_iter.valid() {
            if cancellation_token.is_cancelled() {
                info!("Setting tasks for JSONs is stopped");
                break;
            }
            if let Some(value) = assets_iter.value() {
                match fb::root_as_asset_complete_details(value) {
                    Ok(asset) => {
                        if let Some(url) =
                            asset.dynamic_details().and_then(|d| d.url()).and_then(|u| u.value())
                        {
                            let url = url.trim().replace('\0', "").clone();
                            let downloaded_json =
                                self.target_rocks_db.asset_offchain_data.get(url.clone());

                            if let Err(e) = downloaded_json {
                                error!("asset_offchain_data.get: {}", e);
                                continue;
                            }

                            let mut task = Task {
                                ofd_metadata_url: url,
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
                                },
                                None => {
                                    buff.push(task);
                                },
                            }

                            self.metrics.set_tasks_buffer("tasks_buffer", buff.len() as i64);
                        }
                    },
                    Err(e) => {
                        error!("root_as_asset_complete_details: {}", e);
                    },
                }
            }
            assets_iter.next();
        }
    }
}
