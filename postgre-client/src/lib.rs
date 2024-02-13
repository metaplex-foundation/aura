use entities::enums::TaskStatus;
use entities::models::UrlWithStatus;
use metrics_utils::red::RequestErrorDurationMetrics;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Error, PgPool, Postgres, QueryBuilder, Row, Transaction,
};
use std::{collections::HashMap, sync::Arc};
use tracing::log::LevelFilter;

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod integrity_verification_client;
pub mod model;
pub mod storage_traits;

pub const SQL_COMPONENT: &str = "sql";
pub const SELECT_ACTION: &str = "select";
pub const UPDATE_ACTION: &str = "update";
pub const BATCH_SELECT_ACTION: &str = "batch_select";
pub const BATCH_UPSERT_ACTION: &str = "batch_upsert";
pub const BATCH_DELETE_ACTION: &str = "batch_delete";
pub const TRANSACTION_ACTION: &str = "transaction";
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

    pub async fn insert_tasks(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: Vec<UrlWithStatus>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO tasks (tsk_metadata_url, tsk_status) ");
        query_builder.push_values(metadata_urls.iter(), |mut builder, metadata_url| {
            builder.push_bind(metadata_url.metadata_url.clone());
            builder.push_bind(match metadata_url.is_downloaded {
                true => TaskStatus::Success,
                false => TaskStatus::Pending,
            });
        });
        query_builder.push(" ON CONFLICT (tsk_metadata_url) DO NOTHING;");

        let query = query_builder.build();
        let start_time = chrono::Utc::now();
        query.execute(transaction).await.map_err(|err| {
            self.metrics
                .observe_error(SQL_COMPONENT, BATCH_UPSERT_ACTION, "tasks");
            err.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, BATCH_UPSERT_ACTION, "tasks", start_time);

        Ok(())
    }

    pub async fn get_tasks_ids(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: Vec<String>,
    ) -> Result<HashMap<String, i64>, String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "SELECT tsk_id, tsk_metadata_url FROM tasks WHERE tsk_metadata_url in (",
        );

        let urls_len = metadata_urls.len();

        for (i, url) in metadata_urls.iter().enumerate() {
            query_builder.push_bind(url);
            if i < urls_len - 1 {
                query_builder.push(",");
            }
        }
        query_builder.push(");");

        let query = query_builder.build();

        let start_time = chrono::Utc::now();
        let rows_result = query.fetch_all(transaction).await.map_err(|err| {
            self.metrics
                .observe_error(SQL_COMPONENT, BATCH_SELECT_ACTION, "tasks");
            err.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, BATCH_SELECT_ACTION, "tasks", start_time);
        let mut metadata_ids_map = HashMap::new();

        for row in rows_result {
            let metadata_id: i64 = row.get("tsk_id");
            let metadata_url: String = row.get("tsk_metadata_url");

            metadata_ids_map.insert(metadata_url, metadata_id);
        }

        Ok(metadata_ids_map)
    }
}
