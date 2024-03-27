use crate::PgClient;
use entities::models::{Task, UrlWithStatus};
use metrics_utils::IngesterMetricsConfig;
use sqlx::QueryBuilder;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use usecase::save_metrics::result_to_metrics;

// arbitrary number, should be enough to not overflow batch insert command at Postgre
pub const MAX_BUFFERED_TASKS_TO_TAKE: usize = 5000;

impl PgClient {
    async fn insert_new_tasks(&self, data: &mut Vec<Task>) -> Result<(), String> {
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
        query
            .execute(&self.pool)
            .await
            .map_err(|err| format!("Insert tasks: {}", err))?;

        Ok(())
    }

    pub async fn store_tasks(
        &self,
        tasks_buffer: Arc<Mutex<VecDeque<Task>>>,
        tasks: &Vec<Task>,
        metrics: Arc<IngesterMetricsConfig>,
    ) {
        let mut tasks_to_insert = tasks.clone();

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
}