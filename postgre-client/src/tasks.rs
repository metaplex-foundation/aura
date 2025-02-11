use std::{collections::VecDeque, sync::Arc};

use entities::{
    enums::TaskStatus,
    models::{MetadataDownloadTask, Task, UrlWithStatus},
};
use metrics_utils::IngesterMetricsConfig;
use sqlx::{Postgres, QueryBuilder, Row};
use tokio::sync::Mutex;
use usecase::save_metrics::result_to_metrics;

use crate::{error::IndexDbError, PgClient};

// arbitrary number, should be enough to not overflow batch insert command at Postgre
pub const MAX_BUFFERED_TASKS_TO_TAKE: usize = 5000;

pub struct UpdatedTask {
    pub status: TaskStatus,
    pub mutability: String,
    pub metadata_url: String,
    pub etag: Option<String>,
    pub last_modified_at: Option<String>,
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
                metadata_hash,
                metadata_url,
                etag,
                last_modified,
                mutability,
                next_try_at,
                status,
            ) ",
        );

        query_builder.push_values(data, |mut b, offchain_data| {
            let task = UrlWithStatus::new(offchain_data.metadata_url.as_str(), false);
            b.push_bind(task.get_metadata_id());
            b.push_bind(task.metadata_url);
            b.push_bind(offchain_data.etag.clone());
            b.push_bind(offchain_data.last_modified_at.clone());
            b.push_bind(offchain_data.mutability);
            b.push_bind(offchain_data.next_try_at);
            b.push_bind(offchain_data.status);
        });

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
                status = tmp.status, 
                etag = tmp.etag, 
                last_modified = tmp.last_modified, 
                mutability = tmp.mutability, 
                next_refresh_at = NOW + INTERVAL '1 day' 
                FROM (
        ",
        );

        query_builder.push_values(data, |mut b, task| {
            let url = UrlWithStatus::new(task.metadata_url.as_str(), false); // status is ignored here
            b.push_bind(url.metadata_url);
            b.push_bind(task.status);
            b.push_bind(task.etag.clone());
            b.push_bind(task.last_modified_at.clone());
            b.push_bind(task.mutability);
        });

        query_builder.push(") as tmp (status, etag, last_modified, mutability) WHERE tasks.metadata_hash = tmp.metadata_hash;");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn update_tasks_attempt_time(&self, data: Vec<String>) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "
            UPDATE tasks 
            SET
                next_refresh_at = NOW + INTERVAL '1 day' 
                WHERE tasks.metadata_url IN (
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
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "WITH selected_tasks AS (
                                    SELECT t.metadata_hash FROM tasks AS t
                                    WHERE t.status = 'pending' AND NOW() > t.next_try_at
                                    FOR UPDATE
                                    LIMIT ",
        );
        query_builder.push_bind(tasks_count);

        query_builder.push(
            ")
            UPDATE tasks t
            SET next_try_at = NOW() + INTERVAL '1 day'
            FROM selected_tasks
            WHERE t.metadata_hash = selected_tasks.metadata_hash
            RETURNING t.metadata_hash, t.metadata_url, t.status, t.attempts, t.max_attempts, t.error;");

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("metadata_url");
            let status: TaskStatus = row.get("task_status");
            let etag: Option<String> = row.get("etag");
            let last_modified_at: Option<String> = row.get("last_modified_at");

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
                                    SELECT t.metadata_hash FROM tasks AS t
                                    WHERE t.status = 'refresh' AND NOW() > t.next_try_at AND t.mutability = 'mutable'
                                    FOR UPDATE
                                    LIMIT ",
        );
        query_builder.push_bind(tasks_count);

        query_builder.push(
            ")
            UPDATE tasks t
            SET next_try_at = NOW() + INTERVAL '1 day'
            FROM selected_tasks
            WHERE t.metadata_hash = selected_tasks.metadata_hash
            RETURNING t.metadata_hash, t.metadata_url, t.status, t.attempts, t.max_attempts, t.error;");

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("metadata_url");
            let status: TaskStatus = row.get("task_status");
            let etag: Option<String> = row.get("etag");
            let last_modified_at: Option<String> = row.get("last_modified_at");

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
                metadata_hash,
                metadata_url,
                status
            FROM tasks",
        );

        if let Some(after) = after {
            query_builder.push(" WHERE metadata_hash > ");
            query_builder.push_bind(after);
        }

        query_builder.push(" ORDER BY metadata_hash");

        query_builder.push(" limit ");

        query_builder.push_bind(limit);

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_hash: Vec<u8> = row.get("metadata_hash");
            let metadata_url: String = row.get("metadata_url");
            let status: TaskStatus = row.get("status");

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
            QueryBuilder::new("update tasks SET task_status = ");
        query_builder.push_bind(status_to_set);
        query_builder.push(" WHERE metadata_url IN (");

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
