use entities::enums::TaskStatus;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, Postgres},
    ConnectOptions, PgPool, QueryBuilder, Row,
};

use crate::config::DatabaseConfig;
use crate::error::IngesterError;

#[derive(Clone)]
pub struct DBClient {
    pub pool: PgPool,
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

#[derive(Debug, Clone)]
pub struct JsonDownloadTask {
    pub metadata_url: String,
    pub status: TaskStatus,
    pub attempts: i16,
    pub max_attempts: i16,
}

pub struct UpdatedTask {
    pub status: TaskStatus,
    pub metadata_url: String,
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
            b.push_bind(key.metadata_url);
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
                                    RETURNING t.tsk_metadata_url, t.tsk_status, t.tsk_attempts, t.tsk_max_attempts;");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Get pending tasks: {}", err)))?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("tsk_metadata_url");
            let status: TaskStatus = row.get("tsk_status");
            let attempts: i16 = row.get("tsk_attempts");
            let max_attempts: i16 = row.get("tsk_max_attempts");

            tasks.push(JsonDownloadTask {
                metadata_url,
                status,
                attempts,
                max_attempts,
            });
        }

        Ok(tasks)
    }

    pub async fn insert_tasks(&self, data: &mut Vec<Task>) -> Result<(), IngesterError> {
        data.sort_by(|a, b| a.ofd_metadata_url.cmp(&b.ofd_metadata_url));

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

        query_builder.push_values(data, |mut b, off_d| {
            b.push_bind(off_d.ofd_metadata_url.clone());
            b.push_bind(off_d.ofd_locked_until);
            b.push_bind(off_d.ofd_attempts);
            b.push_bind(off_d.ofd_max_attempts);
            b.push_bind(off_d.ofd_error.clone());
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
