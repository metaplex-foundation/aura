use std::{collections::VecDeque, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use entities::{
    enums::TaskStatus,
    models::{MetadataDownloadTask, Task, UrlWithStatus},
};
use interface::json_metadata::CacheControlResponse;
use metrics_utils::IngesterMetricsConfig;
use sqlx::{Postgres, QueryBuilder, Row};
use tokio::sync::Mutex;
use usecase::save_metrics::result_to_metrics;

use crate::{error::IndexDbError, PgClient};

// arbitrary number, should be enough to not overflow batch insert command at Postgre
pub const MAX_BUFFERED_TASKS_TO_TAKE: usize = 5000;

pub struct UpdatedTask {
    status: TaskStatus,
    mutability: String,
    metadata_url: String,
    etag: Option<String>,
    last_modified_at: Option<DateTime<Utc>>,
    cache_control: Option<CacheControlResponse>,
    error_message: Option<String>,
}

impl UpdatedTask {
    pub fn builder() -> UpdatedTaskBuilder {
        UpdatedTaskBuilder::default()
    }
}

#[derive(Default)]
pub struct UpdatedTaskBuilder {
    status: Option<TaskStatus>,
    mutability: Option<String>,
    metadata_url: Option<String>,
    etag: Option<String>,
    last_modified_at: Option<DateTime<Utc>>,
    cache_control: Option<CacheControlResponse>,
    error_message: Option<String>,
}

impl UpdatedTaskBuilder {
    pub fn status(mut self, status: TaskStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn mutability(mut self, mutability: &str) -> Self {
        self.mutability = Some(mutability.into());
        self
    }

    pub fn metadata_url(mut self, metadata_url: &str) -> Self {
        self.metadata_url = Some(metadata_url.into());
        self
    }

    pub fn etag(mut self, etag: Option<&str>) -> Self {
        self.etag = etag.map(|s| s.to_string());
        self
    }

    pub fn last_modified_at(mut self, last_modified_at: Option<DateTime<Utc>>) -> Self {
        self.last_modified_at = last_modified_at;
        self
    }

    pub fn cache_control(mut self, cache_control: Option<CacheControlResponse>) -> Self {
        self.cache_control = cache_control;
        self
    }

    pub fn error_message<T: Into<String>>(mut self, error_message: Option<T>) -> Self {
        self.error_message = error_message.map(|s| s.into());
        self
    }

    pub fn build(self) -> Result<UpdatedTask, &'static str> {
        Ok(UpdatedTask {
            status: self.status.ok_or("status is required")?,
            mutability: self.mutability.ok_or("mutability is required")?,
            metadata_url: self.metadata_url.ok_or("metadata_url is required")?,
            etag: self.etag,
            last_modified_at: self.last_modified_at,
            cache_control: self.cache_control,
            error_message: self.error_message,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JsonTask {
    pub metadata_hash: Vec<u8>,
    pub metadata_url: String,
    pub status: TaskStatus,
}

impl PgClient {
    pub async fn insert_new_tasks(&self, data: &mut Vec<Task>) -> Result<(), IndexDbError> {
        data.sort_by(|a, b| a.metadata_url.cmp(&b.metadata_url));

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO tasks (
                tasks_metadata_hash,
                tasks_metadata_url,
                tasks_etag,
                tasks_last_modified_at,
                tasks_mutability,
                tasks_next_try_at,
                tasks_task_status
            ) ",
        );

        query_builder.push_values(
            data,
            |mut b: sqlx::query_builder::Separated<'_, '_, Postgres, &str>, task| {
                let url_with_status = UrlWithStatus::new(task.metadata_url.as_str(), false);
                b.push_bind(url_with_status.get_metadata_id());
                b.push_bind(url_with_status.metadata_url);
                b.push_bind(task.etag.clone());
                b.push_bind(task.last_modified_at);
                b.push_bind(task.mutability);
                b.push_bind(task.next_try_at);
                b.push_bind(task.status);
            },
        );

        query_builder.push("ON CONFLICT DO NOTHING;");

        let query = query_builder.build();

        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn store_tasks(
        &self,
        tasks_buffer: Arc<Mutex<VecDeque<Task>>>,
        tasks: &[Task],
        metrics: Arc<IngesterMetricsConfig>,
    ) {
        let mut tasks_to_insert = tasks.to_owned();

        // scope crated to unlock mutex before insert_tasks func, which can be time consuming
        let tasks = {
            let mut tasks_buffer = tasks_buffer.lock().await;

            let number_of_tasks = {
                if tasks_buffer.len() + tasks.len() > MAX_BUFFERED_TASKS_TO_TAKE {
                    MAX_BUFFERED_TASKS_TO_TAKE.saturating_sub(tasks.len())
                } else {
                    tasks_buffer.len()
                }
            };

            tasks_buffer.drain(0..number_of_tasks).collect::<Vec<Task>>()
        };

        tasks_to_insert.extend(tasks);

        if !tasks_to_insert.is_empty() {
            let res = self.insert_new_tasks(&mut tasks_to_insert).await;
            result_to_metrics(metrics, &res, "accounts_saving_tasks");
        }
    }

    pub async fn update_tasks(&self, data: Vec<UpdatedTask>) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "
            UPDATE tasks
            SET
                tasks_task_status = tmp.tasks_task_status,
                tasks_etag = tmp.etag,
                tasks_last_modified_at = tmp.last_modified_at,
                tasks_mutability = tmp.mutability,
                tasks_next_try_at = tmp.tasks_next_try_at,
                tasks_error = tmp.tasks_error
                FROM (
        ",
        );

        query_builder.push_values(data, |mut b, task| {
            let url = UrlWithStatus::new(task.metadata_url.as_str(), false); // status is ignored here
            b.push_bind(url.get_metadata_id());
            b.push_bind(url.metadata_url);
            b.push_bind(task.status);
            b.push_bind(task.etag.clone());
            b.push_bind(task.last_modified_at);
            b.push_bind(task.mutability.clone());

            let tasks_next_try_at = match &task.mutability[..] {
                "mutable" => task
                    .cache_control
                    .as_ref()
                    .and_then(|cc| {
                        cc.max_age.map(|max_age| {
                            Utc::now() + Duration::seconds(max_age.try_into().unwrap())
                        })
                    })
                    .or_else(|| Some(Utc::now() + Duration::days(1))),
                _ => None,
            };
            b.push_bind(tasks_next_try_at);
            b.push_bind(task.error_message);
        });

        query_builder.push(") as tmp (tasks_metadata_hash, tasks_metadata_url, task_status, etag, last_modified_at, mutability, tasks_next_try_at, tasks_error) WHERE tasks.tasks_metadata_hash = tmp.tasks_metadata_hash;");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn update_tasks_attempt_time(&self, data: Vec<String>) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "
            UPDATE tasks
            SET
                tasks_next_try_at = NOW() + INTERVAL '1 day'
                WHERE tasks.tasks_metadata_url IN (
        ",
        );

        query_builder.push_values(data, |mut b, task| {
            let url = UrlWithStatus::new(task.as_str(), false); // status is ignored here
            b.push_bind(url.metadata_url);
        });

        query_builder.push(");");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_pending_metadata_tasks(
        &self,
        tasks_count: i32,
    ) -> Result<Vec<MetadataDownloadTask>, IndexDbError> {
        // skip locked not to intersect with synchronizer work
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "WITH selected_tasks AS (
                SELECT t.tasks_metadata_hash FROM tasks AS t
                WHERE t.tasks_task_status = 'pending' AND NOW() > t.tasks_next_try_at
                ORDER BY t.tasks_last_modified_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT ",
        );
        query_builder.push_bind(tasks_count);

        query_builder.push(
            ")
            UPDATE tasks t
            SET
                tasks_next_try_at = NOW() + INTERVAL '1 day'
            FROM
                selected_tasks
            WHERE
                t.tasks_metadata_hash = selected_tasks.tasks_metadata_hash
            RETURNING
                t.tasks_metadata_url,
                t.tasks_task_status,
                t.tasks_etag,
                t.tasks_last_modified_at;",
        );

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("tasks_metadata_url");
            let status: TaskStatus = row.get("tasks_task_status");
            let etag: Option<String> = row.get("tasks_etag");
            let last_modified_at: Option<DateTime<Utc>> = row.get("tasks_last_modified_at");

            tasks.push(MetadataDownloadTask { metadata_url, status, etag, last_modified_at });
        }

        Ok(tasks)
    }

    pub async fn get_refresh_metadata_tasks(
        &self,
        tasks_count: i32,
    ) -> Result<Vec<MetadataDownloadTask>, IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "WITH selected_tasks AS (
                                    SELECT t.tasks_metadata_hash FROM tasks AS t
                                    WHERE t.tasks_task_status = 'success' AND NOW() > t.tasks_next_try_at AND t.tasks_mutability = 'mutable'
                                    ORDER BY t.tasks_last_modified_at ASC
                                    FOR UPDATE SKIP LOCKED
                                    LIMIT ",
        );
        query_builder.push_bind(tasks_count);

        query_builder.push(
            ")
            UPDATE tasks t
            SET tasks_next_try_at = NOW() + INTERVAL '1 day'
            FROM selected_tasks
            WHERE t.tasks_metadata_hash = selected_tasks.tasks_metadata_hash
            RETURNING t.tasks_metadata_url, t.tasks_task_status, t.tasks_etag, t.tasks_last_modified_at;",
        );

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("tasks_metadata_url");
            let status: TaskStatus = row.get("tasks_task_status");
            let etag: Option<String> = row.get("tasks_etag");
            let last_modified_at: Option<DateTime<Utc>> = row.get("tasks_last_modified_at");

            tasks.push(MetadataDownloadTask { metadata_url, status, etag, last_modified_at });
        }

        Ok(tasks)
    }

    pub async fn get_tasks_count(&self) -> Result<i64, IndexDbError> {
        let resp = sqlx::query("SELECT COUNT(*) FROM tasks").fetch_one(&self.pool).await?;
        let count: i64 = resp.get(0);

        Ok(count)
    }

    pub async fn get_tasks(
        &self,
        limit: i64,
        after: Option<Vec<u8>>,
    ) -> Result<Vec<JsonTask>, IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "
            SELECT
                tasks_metadata_hash,
                tasks_metadata_url,
                tasks_task_status
            FROM tasks",
        );

        if let Some(after) = after {
            query_builder.push(" WHERE tasks_metadata_hash > ");
            query_builder.push_bind(after);
        }

        query_builder.push(" ORDER BY tasks_metadata_hash");

        query_builder.push(" limit ");

        query_builder.push_bind(limit);

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_hash: Vec<u8> = row.get("tasks_metadata_hash");
            let metadata_url: String = row.get("tasks_metadata_url");
            let status: TaskStatus = row.get("tasks_task_status");

            tasks.push(JsonTask { metadata_hash, metadata_url, status });
        }

        Ok(tasks)
    }

    pub async fn change_task_status(
        &self,
        tasks_urls: Vec<String>,
        status_to_set: TaskStatus,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("update tasks SET tasks_task_status = ");
        query_builder.push_bind(status_to_set);
        query_builder.push(" WHERE tasks_metadata_url IN (");

        let mut qb = query_builder.separated(", ");
        for url in tasks_urls {
            qb.push_bind(url);
        }
        query_builder.push(")");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }
}
