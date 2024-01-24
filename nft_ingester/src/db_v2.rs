use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, Postgres},
    ConnectOptions, PgPool, QueryBuilder, Row,
};
use std::collections::{HashMap, HashSet};

use crate::config::DatabaseConfig;
use crate::error::IngesterError;

#[derive(Clone)]
pub struct DBClient {
    pub pool: PgPool,
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
    Default,
)]
#[sqlx(type_name = "task_status", rename_all = "lowercase")]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Clone, Default)]
pub struct Task {
    pub ofd_metadata_url: String,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
    pub ofd_status: TaskStatus,
}

pub struct TaskForInsert {
    pub ofd_metadata_url: i64,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
    pub ofd_status: TaskStatus,
}

#[derive(Debug, Clone)]
pub struct JsonDownloadTask {
    pub metadata_url: String,
    pub metadata_url_key: i64,
    pub status: TaskStatus,
    pub attempts: i16,
    pub max_attempts: i16,
}

pub struct UpdatedTask {
    pub status: TaskStatus,
    pub metadata_url_key: i64,
    pub attempts: i16,
    pub error: String,
}

impl DBClient {
    pub async fn new(config: &DatabaseConfig) -> Result<Self, IngesterError> {
        let max = config.get_max_postgres_connections().unwrap_or(100);

        let url = config.get_database_url()?;

        let mut options: PgConnectOptions =
            url.parse()
                .map_err(|err| IngesterError::ConfigurationError {
                    msg: format!("URL parse: {}", err),
                })?;
        options.log_statements(log::LevelFilter::Trace);

        options.log_slow_statements(
            log::LevelFilter::Debug,
            std::time::Duration::from_millis(500),
        );

        let pool = PgPoolOptions::new()
            .min_connections(100)
            .max_connections(max)
            .connect_with(options)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Connect :{}", err)))?;

        Ok(Self { pool })
    }

    pub async fn update_tasks(&self, data: Vec<UpdatedTask>) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE tasks SET tsk_status = tmp.tsk_status, tsk_attempts = tmp.tsk_attempts, tsk_error = tmp.tsk_error FROM (");

        query_builder.push_values(data, |mut b, key| {
            b.push_bind(key.metadata_url_key);
            b.push_bind(key.status);
            b.push_bind(key.attempts);
            b.push_bind(key.error);
        });

        query_builder.push(") as tmp (tsk_metadata_url, tsk_status, tsk_attempts, tsk_error) WHERE tasks.tsk_metadata_url = tmp.tsk_metadata_url;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Update tasks: {}", err)))?;

        Ok(())
    }

    pub async fn get_pending_tasks(&self) -> Result<Vec<JsonDownloadTask>, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("WITH cte AS (
                                        SELECT tsk_id
                                        FROM tasks
                                        WHERE tsk_status != 'success' AND tsk_locked_until < NOW() AND tsk_attempts < tsk_max_attempts
                                        LIMIT 100
                                        FOR UPDATE
                                    )
                                    UPDATE tasks t
                                    SET tsk_status = 'running',
                                    tsk_locked_until = NOW() + INTERVAL '20 seconds'
                                    FROM cte
                                    WHERE t.tsk_id = cte.tsk_id
                                    RETURNING (
                                        SELECT mtd_url FROM metadata m WHERE m.mtd_id = t.tsk_metadata_url) as metadata_url, t.tsk_metadata_url,
                                        t.tsk_status, t.tsk_attempts, t.tsk_max_attempts;");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Get pending tasks: {}", err)))?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("metadata_url");
            let metadata_url_key: i64 = row.get("tsk_metadata_url");
            let status: TaskStatus = row.get("tsk_status");
            let attempts: i16 = row.get("tsk_attempts");
            let max_attempts: i16 = row.get("tsk_max_attempts");

            tasks.push(JsonDownloadTask {
                metadata_url,
                metadata_url_key,
                status,
                attempts,
                max_attempts,
            });
        }

        Ok(tasks)
    }

    pub async fn insert_metadata(
        &self,
        urls: &Vec<&str>,
    ) -> Result<HashMap<String, i64>, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO metadata (mtd_url)");

        query_builder.push_values(urls, |mut b, key| {
            b.push_bind(key);
        });

        query_builder.push("ON CONFLICT (mtd_url) DO NOTHING RETURNING mtd_id, mtd_url;");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert one metadata: {}", err)))?;

        let res: HashMap<String, i64> = rows
            .iter()
            .map(|row| (row.get("mtd_url"), row.get("mtd_id")))
            .collect();

        Ok(res)
    }

    pub async fn insert_tasks(&self, data: &Vec<Task>) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for off_d in data {
            keys.insert(off_d.ofd_metadata_url.as_str());
        }

        let keys = keys.into_iter().collect::<Vec<_>>();
        let ids_keys = self.insert_metadata(&keys).await?;

        let mut offchain_data_to_insert = Vec::new();

        for offchain_d in data.iter() {
            // save tasks only for those links which are new
            if let Some(id) = ids_keys.get(&offchain_d.ofd_metadata_url) {
                offchain_data_to_insert.push(TaskForInsert {
                    ofd_metadata_url: *id,
                    ofd_locked_until: offchain_d.ofd_locked_until,
                    ofd_attempts: offchain_d.ofd_attempts,
                    ofd_max_attempts: offchain_d.ofd_max_attempts,
                    ofd_error: offchain_d.ofd_error.clone(),
                    ofd_status: offchain_d.ofd_status,
                });
            }
        }

        offchain_data_to_insert.sort_by(|a, b| a.ofd_metadata_url.cmp(&b.ofd_metadata_url));

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO tasks (
                tsk_metadata_url,
                tsk_locked_until,
                tsk_attempts,
                tsk_max_attempts,
                tsk_error,
                tsk_status
            ) ",
        );

        query_builder.push_values(offchain_data_to_insert, |mut b, off_d| {
            b.push_bind(off_d.ofd_metadata_url);
            b.push_bind(off_d.ofd_locked_until);
            b.push_bind(off_d.ofd_attempts);
            b.push_bind(off_d.ofd_max_attempts);
            b.push_bind(off_d.ofd_error);
            b.push_bind(off_d.ofd_status);
        });

        query_builder.push("ON CONFLICT (tsk_metadata_url) DO NOTHING;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert tasks: {}", err)))?;

        Ok(())
    }
}
