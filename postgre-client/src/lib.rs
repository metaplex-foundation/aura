use std::{path::PathBuf, sync::Arc, time::Duration};

use entities::{enums::TaskStatus, models::UrlWithStatus};
use error::IndexDbError;
use metrics_utils::red::RequestErrorDurationMetrics;
use sqlx::{
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Error, PgPool, Postgres, QueryBuilder, Row, Transaction,
};
use tracing::log::LevelFilter;

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod core_fees;
pub mod error;
pub mod integrity_verification_client;
pub mod load_client;
pub mod model;
pub mod storage_traits;
pub mod tasks;

pub const SQL_COMPONENT: &str = "sql";
pub const SELECT_ACTION: &str = "select";
pub const INSERT_ACTION: &str = "insert";
pub const UPDATE_ACTION: &str = "update";
pub const COUNT_ACTION: &str = "count";
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

pub const PG_MIGRATIONS_PATH: &str = "./migrations";

#[derive(Clone)]
pub struct PgClient {
    pub pool: PgPool,
    pub base_dump_path: Option<PathBuf>,
    pub metrics: Arc<RequestErrorDurationMetrics>,
}

impl PgClient {
    /// If `max_query_statement_timeout_secs` is `None`, the PostgresSQL database does not have any limitation on the query statement timeout. (if this was not specified via the db URL parameter)
    pub async fn new(
        url: &str,
        min_connections: u32,
        max_connections: u32,
        base_dump_path: Option<PathBuf>,
        metrics: Arc<RequestErrorDurationMetrics>,
        max_query_statement_timeout_secs: Option<u32>,
    ) -> Result<Self, Error> {
        let mut options: PgConnectOptions = url.parse::<PgConnectOptions>()?;
        if !url.contains("statement_timeout") && max_query_statement_timeout_secs.is_some() {
            options = options.options([(
                "statement_timeout",
                &format!("{}s", max_query_statement_timeout_secs.unwrap()),
            )]);
        }

        options.log_statements(LevelFilter::Off);
        options.log_slow_statements(LevelFilter::Off, Duration::from_secs(100));

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            // 1 hour of a timeout, this is set specifically due to synchronizer needing up to 200 connections to do a full sync load
            .acquire_timeout(Duration::from_secs(3600))
            .connect_with(options)
            .await?;

        Ok(Self { pool, base_dump_path, metrics })
    }

    pub fn new_with_pool(
        pool: PgPool,
        base_dump_path: Option<PathBuf>,
        metrics: Arc<RequestErrorDurationMetrics>,
    ) -> Self {
        Self { pool, base_dump_path, metrics }
    }

    pub async fn check_health(&self) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        let resp = sqlx::query("SELECT 1;").fetch_one(&self.pool).await;

        match resp {
            Ok(_) => {
                self.metrics.observe_request(SQL_COMPONENT, SELECT_ACTION, "health", start_time);
                Ok(())
            },
            Err(e) => {
                self.metrics.observe_error(SQL_COMPONENT, SELECT_ACTION, "health");
                Err(e.to_string())
            },
        }
    }

    pub async fn get_collection_size(&self, collection_key: &[u8]) -> Result<u64, sqlx::Error> {
        let start_time = chrono::Utc::now();
        let resp = sqlx::query("SELECT COUNT(*) FROM assets_v3 WHERE ast_collection = $1 AND ast_is_collection_verified = true AND ast_supply > 0")
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
            },
            Err(e) => {
                self.metrics.observe_error(SQL_COMPONENT, SELECT_ACTION, "get_collection_size");
                Err(e)
            },
        }
    }

    pub async fn run_migration(&self, migration_path: &str) -> Result<(), String> {
        let m =
            Migrator::new(std::path::Path::new(migration_path)).await.map_err(|e| e.to_string())?;
        m.run(&self.pool).await.map_err(|e| e.to_string())
    }

    pub(crate) async fn insert_tasks(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        metadata_urls: &[UrlWithStatus],
        table: &str,
    ) -> Result<(), IndexDbError> {
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
        query_builder.push(" ON CONFLICT DO NOTHING;");

        self.execute_query_with_metrics(transaction, &mut query_builder, BATCH_UPSERT_ACTION, table)
            .await
    }

    async fn start_transaction(&self) -> Result<Transaction<'_, Postgres>, IndexDbError> {
        let start_time = chrono::Utc::now();
        let transaction = self.pool.begin().await.inspect_err(|_e| {
            self.metrics.observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "begin");
        })?;
        self.metrics.observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "begin", start_time);
        Ok(transaction)
    }

    async fn commit_transaction(
        &self,
        transaction: Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let start_time = chrono::Utc::now();
        transaction.commit().await.inspect_err(|_e| {
            self.metrics.observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "commit");
        })?;
        self.metrics.observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "commit", start_time);
        Ok(())
    }
    async fn rollback_transaction(
        &self,
        transaction: Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let start_time = chrono::Utc::now();
        transaction.rollback().await.inspect_err(|_e| {
            self.metrics.observe_error(SQL_COMPONENT, TRANSACTION_ACTION, "rollback");
        })?;
        self.metrics.observe_request(SQL_COMPONENT, TRANSACTION_ACTION, "rollback", start_time);
        Ok(())
    }

    #[cfg(feature = "integration_tests")]
    pub async fn clean_db(&self) -> Result<(), IndexDbError> {
        let mut transaction = self.pool.begin().await?;

        self.drop_fungible_indexes(&mut transaction).await?;
        self.drop_nft_indexes(&mut transaction).await?;
        self.drop_constraints(&mut transaction).await?;
        for table in [
            "assets_v3",
            "assets_authorities",
            "asset_creators_v3",
            "batch_mints",
            "core_fees",
            "fungible_tokens",
        ] {
            self.truncate_table(&mut transaction, table).await?;
        }

        transaction
            .execute(sqlx::query(
                "update last_synced_key set last_synced_asset_update_key = null where id = 1 or id = 2;",
            ))
            .await?;

        self.recreate_fungible_indexes(&mut transaction).await?;
        self.recreate_asset_indexes(&mut transaction).await?;
        self.recreate_authorities_indexes(&mut transaction).await?;
        self.recreate_creators_indexes(&mut transaction).await?;
        self.recreate_asset_authorities_constraints(&mut transaction).await?;
        self.recreate_asset_creators_constraints(&mut transaction).await?;
        self.recreate_asset_constraints(&mut transaction).await?;

        transaction.commit().await?;
        // those await above will not always roll back the tx
        // take this into account if we use this function somewhere else except the tests

        Ok(())
    }
}
