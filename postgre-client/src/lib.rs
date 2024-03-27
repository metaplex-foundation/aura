use entities::enums::TaskStatus;
use entities::models::UrlWithStatus;
use metrics_utils::red::RequestErrorDurationMetrics;
use sqlx::Row;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Error, PgPool, Postgres, QueryBuilder, Transaction,
};
use std::{sync::Arc, time::Duration};
use tracing::log::LevelFilter;

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod integrity_verification_client;
pub mod load_client;
pub mod model;
pub mod storage_traits;
pub mod temp_index_client;

pub const SQL_COMPONENT: &str = "sql";
pub const SELECT_ACTION: &str = "select";
pub const INSERT_ACTION: &str = "insert";
pub const UPDATE_ACTION: &str = "update";
pub const BATCH_SELECT_ACTION: &str = "batch_select";
pub const BATCH_UPSERT_ACTION: &str = "batch_upsert";
pub const BATCH_DELETE_ACTION: &str = "batch_delete";
pub const TRANSACTION_ACTION: &str = "transaction";
pub const COPY_ACTION: &str = "copy";
pub const TRUNCATE_ACTION: &str = "truncate";
pub const DROP_ACTION: &str = "drop";
pub const ALTER_ACTION: &str = "alter";
pub const CREATE_ACTION: &str = "create";
pub const POSTGRES_PARAMETERS_COUNT_LIMIT: usize = 65535;
pub const INSERT_TASK_PARAMETERS_COUNT: usize = 3;
pub const TEMP_TABLE_PREFIX: &str = "temp_";

#[derive(Clone)]
pub struct PgClient {
    pub pool: PgPool,
    pub metrics: Arc<RequestErrorDurationMetrics>,
}

impl PgClient {
    pub async fn new(
        url: &str,
        min_connections: u32,
        max_connections: u32,
        metrics: Arc<RequestErrorDurationMetrics>,
    ) -> Result<Self, Error> {
        let mut options: PgConnectOptions = url.parse().unwrap();
        options.log_statements(LevelFilter::Off);
        options.log_slow_statements(LevelFilter::Info, Duration::from_secs(2));

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .connect_with(options)
            .await?;

        Ok(Self { pool, metrics })
    }

    pub fn new_with_pool(pool: PgPool, metrics: Arc<RequestErrorDurationMetrics>) -> Self {
        Self { pool, metrics }
    }

    pub async fn check_health(&self) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        let resp = sqlx::query("SELECT 1;").fetch_one(&self.pool).await;

        match resp {
            Ok(_) => {
                self.metrics
                    .observe_request(SQL_COMPONENT, SELECT_ACTION, "health", start_time);
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "health");
                Err(e.to_string())
            }
        }
    }

    pub async fn get_collection_size(&self, collection_key: &[u8]) -> Result<u64, sqlx::Error> {
        let start_time = chrono::Utc::now();
        let resp = sqlx::query("SELECT COUNT(*) FROM assets_v3 WHERE ast_collection = $1 AND ast_is_collection_verified = true")
            .bind(collection_key)
            .fetch_one(&self.pool).await;

        match resp {
            Ok(size) => {
                self.metrics.observe_request(
                    SQL_COMPONENT,
                    SELECT_ACTION,
                    "get_collection_size",
                    start_time,
                );
                let v: i64 = size.get(0);
                Ok(v as u64)
            }
            Err(e) => {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "get_collection_size");
                Err(e)
            }
        }
    }

    pub(crate) async fn insert_tasks(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: &[UrlWithStatus],
        table: &str,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("INSERT INTO ");
        query_builder.push(table);
        query_builder.push(" (tsk_id, tsk_metadata_url, tsk_status) ");
        query_builder.push_values(metadata_urls.iter(), |mut builder, metadata_url| {
            builder.push_bind(metadata_url.get_metadata_id());
            builder.push_bind(metadata_url.metadata_url.trim().to_owned());
            builder.push_bind(match metadata_url.is_downloaded {
                true => TaskStatus::Success,
                false => TaskStatus::Pending,
            });
        });
        query_builder.push(" ON CONFLICT (tsk_id) DO NOTHING;");

        self.execute_query_with_metrics(transaction, &mut query_builder, BATCH_UPSERT_ACTION, table)
            .await
    }

    async fn start_transaction(&self) -> Result<Transaction<'_, Postgres>, String> {
        let start_time = chrono::Utc::now();
        let transaction = self.pool.begin().await.map_err(|e| {
            self.metrics
                .observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "begin");
            e.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "begin", start_time);
        Ok(transaction)
    }

    async fn commit_transaction(
        &self,
        transaction: Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        transaction.commit().await.map_err(|e| {
            self.metrics
                .observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "commit");
            e.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "commit", start_time);
        Ok(())
    }
    async fn rollback_transaction(
        &self,
        transaction: Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        transaction.rollback().await.map_err(|e| {
            self.metrics
                .observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "rollback");
            e.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "rollback", start_time);
        Ok(())
    }
}
