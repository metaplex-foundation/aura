use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Postgres, QueryBuilder, Row, Transaction,
};
use std::collections::HashMap;

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod model;
pub mod storage_traits;
#[derive(Clone)]
pub struct PgClient {
    pub pool: PgPool,
}

impl PgClient {
    pub async fn new(url: &str, min_connections: u32, max_connections: u32) -> Self {
        let options: PgConnectOptions = url.parse().unwrap();

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

    pub async fn insert_metadata(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: Vec<String>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO metadata (mtd_url) ");
        query_builder.push_values(metadata_urls.iter(), |mut builder, metadata_url| {
            builder.push_bind(metadata_url);
        });
        query_builder.push(" ON CONFLICT (mtd_url) DO NOTHING;");

        let query = query_builder.build();
        query
            .execute(transaction)
            .await
            .map_err(|err| err.to_string())?;

        Ok(())
    }

    pub async fn get_metadata_ids(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: Vec<String>,
    ) -> Result<HashMap<String, i64>, String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("SELECT mtd_id, mtd_url FROM metadata WHERE mtd_url in (");

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
            let metadata_id: i64 = row.get("mtd_id");
            let metadata_url: String = row.get("mtd_url");

            metadata_ids_map.insert(metadata_url, metadata_id);
        }

        Ok(metadata_ids_map)
    }
}
