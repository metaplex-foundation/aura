use entities::enums::TaskStatus;
use entities::models::UrlWithStatus;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, PgPool, Postgres, QueryBuilder, Row, Transaction,
};
use std::collections::HashMap;
use tracing::log::LevelFilter;

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod integrity_verification_client;
pub mod model;
pub mod storage_traits;

#[derive(Clone)]
pub struct PgClient {
    pub pool: PgPool,
}

impl PgClient {
    pub async fn new(url: &str, min_connections: u32, max_connections: u32) -> Self {
        let mut options: PgConnectOptions = url.parse().unwrap();
        options.log_statements(LevelFilter::Off);

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .unwrap();

        Self { pool }
    }

    pub fn new_with_pool(pool: PgPool) -> Self {
        Self { pool }
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
        query
            .execute(transaction)
            .await
            .map_err(|err| err.to_string())?;

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

        let rows_result = query
            .fetch_all(transaction)
            .await
            .map_err(|err| err.to_string())?;

        let mut metadata_ids_map = HashMap::new();

        for row in rows_result {
            let metadata_id: i64 = row.get("tsk_id");
            let metadata_url: String = row.get("tsk_metadata_url");

            metadata_ids_map.insert(metadata_url, metadata_id);
        }

        Ok(metadata_ids_map)
    }
}
