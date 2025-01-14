use crate::error::IndexDbError;
use crate::PgClient;
use entities::enums::TaskStatus;
use entities::models::{JsonDownloadTask, Task, UrlWithStatus};
use metrics_utils::IngesterMetricsConfig;
use sqlx::{Postgres, QueryBuilder, Row};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use usecase::save_metrics::result_to_metrics;

// arbitrary number, should be enough to not overflow batch insert command at Postgre
pub const MAX_BUFFERED_TASKS_TO_TAKE: usize = 5000;

pub struct UpdatedTask {
    pub status: TaskStatus,
    pub metadata_url: String,
    pub error: String,
}

impl PgClient {
    async fn insert_new_tasks(&self, data: &mut Vec<Task>) -> Result<(), IndexDbError> {
        data.sort_by(|a, b| a.ofd_metadata_url.cmp(&b.ofd_metadata_url));

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO tasks (
                tsk_id,
                tsk_metadata_url,
                tsk_locked_until,
                tsk_attempts,
                tsk_max_attempts,
                tsk_error,
                tsk_status
            ) ",
        );

        query_builder.push_values(data, |mut b, off_d| {
            let tsk = UrlWithStatus::new(off_d.ofd_metadata_url.as_str(), false);
            b.push_bind(tsk.get_metadata_id());
            b.push_bind(tsk.metadata_url);
            b.push_bind(off_d.ofd_locked_until);
            b.push_bind(off_d.ofd_attempts);
            b.push_bind(off_d.ofd_max_attempts);
            b.push_bind(off_d.ofd_error.clone());
            b.push_bind(off_d.ofd_status);
        });

        query_builder.push("ON CONFLICT (tsk_id) DO NOTHING;");

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

            tasks_buffer
                .drain(0..number_of_tasks)
                .collect::<Vec<Task>>()
        };

        tasks_to_insert.extend(tasks);

        if !tasks_to_insert.is_empty() {
            let res = self.insert_new_tasks(&mut tasks_to_insert).await;
            result_to_metrics(metrics, &res, "accounts_saving_tasks");
        }
    }

    pub async fn update_tasks_and_attempts(
        &self,
        data: Vec<UpdatedTask>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE tasks SET tsk_status = tmp.tsk_status, tsk_attempts = tsk_attempts+1, tsk_error = tmp.tsk_error FROM (");

        query_builder.push_values(data, |mut b, key| {
            let tsk = UrlWithStatus::new(key.metadata_url.as_str(), false); // status is ignoring here
            b.push_bind(tsk.metadata_url);
            b.push_bind(key.status);
            b.push_bind(key.error);
        });

        query_builder.push(") as tmp (tsk_metadata_url, tsk_status, tsk_error) WHERE tasks.tsk_metadata_url = tmp.tsk_metadata_url;");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_pending_tasks(
        &self,
        tasks_count: i32,
    ) -> Result<Vec<JsonDownloadTask>, IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("WITH cte AS (
                                        SELECT tsk_id
                                        FROM tasks
                                        WHERE (tsk_status = 'running' OR tsk_status = 'pending') AND tsk_locked_until < NOW() AND tsk_attempts < tsk_max_attempts
                                        LIMIT ");

        query_builder.push_bind(tasks_count);

        // skip locked not to intersect with synchronizer work
        query_builder.push(
            " FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
            SET tsk_status = 'running',
            tsk_locked_until = NOW() + INTERVAL '90 seconds'
            FROM cte
            WHERE t.tsk_id = cte.tsk_id
            RETURNING t.tsk_metadata_url, t.tsk_status, t.tsk_attempts, t.tsk_max_attempts;",
        );

        let query = query_builder.build();
        let rows = query.fetch_all(&self.pool).await?;

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

    pub async fn insert_json_download_tasks(
        &self,
        data: &mut Vec<Task>,
    ) -> Result<(), IndexDbError> {
        data.sort_by(|a, b| a.ofd_metadata_url.cmp(&b.ofd_metadata_url));

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO tasks (
                tsk_id,
                tsk_metadata_url,
                tsk_locked_until,
                tsk_attempts,
                tsk_max_attempts,
                tsk_error,
                tsk_status
            ) ",
        );

        query_builder.push_values(data, |mut b, off_d| {
            let tsk = UrlWithStatus::new(off_d.ofd_metadata_url.as_str(), false);
            b.push_bind(tsk.get_metadata_id());
            b.push_bind(tsk.metadata_url);
            b.push_bind(off_d.ofd_locked_until);
            b.push_bind(off_d.ofd_attempts);
            b.push_bind(off_d.ofd_max_attempts);
            b.push_bind(off_d.ofd_error.clone());
            b.push_bind(off_d.ofd_status);
        });

        query_builder.push("ON CONFLICT (tsk_id) DO NOTHING;");

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }
}
