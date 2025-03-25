use std::sync::Arc;

use entities::models::MetadataDownloadTask;
use postgre_client::PgClient;
use tokio::{
    sync::mpsc::Sender,
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::metadata_workers::TaskType;

pub const SLEEP_TIME_SECS: u64 = 1;

pub struct TasksStreamer {
    pub db_conn: Arc<PgClient>,
    pub cancellation_token: CancellationToken,
    pub pending_tasks_sender: Sender<MetadataDownloadTask>,
    pub refresh_tasks_sender: Sender<MetadataDownloadTask>,
}

impl TasksStreamer {
    pub fn new(
        db_conn: Arc<PgClient>,
        cancellation_token: CancellationToken,
        pending_tasks_sender: Sender<MetadataDownloadTask>,
        refresh_tasks_sender: Sender<MetadataDownloadTask>,
    ) -> Self {
        Self { db_conn, cancellation_token, pending_tasks_sender, refresh_tasks_sender }
    }

    pub async fn run(self, num_of_tasks: i32) {
        tokio::join!(
            self.stream_tasks(TaskType::Pending, num_of_tasks),
            self.stream_tasks(TaskType::Refresh, num_of_tasks)
        );
    }

    async fn stream_tasks(&self, task_type: TaskType, num_of_tasks: i32) {
        let db_conn = self.db_conn.clone();
        let tasks_sender = match task_type {
            TaskType::Pending => self.pending_tasks_sender.clone(),
            TaskType::Refresh => self.refresh_tasks_sender.clone(),
        };

        tokio::select! {
            _ = async move {
                loop {
                    let tasks = match task_type {
                        TaskType::Pending => {
                            db_conn.get_pending_metadata_tasks(num_of_tasks).await
                        },
                        TaskType::Refresh => {
                            db_conn.get_refresh_metadata_tasks(num_of_tasks).await
                        },
                    }.map_err(|err| {
                        error!("Error while selecting tasks for JsonDownloader: {}", err);
                    });

                    if let Ok(tasks) = tasks {
                        for task in tasks.iter() {
                            if let Err(err) = tasks_sender.send(task.clone()).await {
                                error!(
                                    "Error during sending task to the tasks channel: {}",
                                    err.to_string()
                                );
                            }
                        }
                    }

                sleep(Duration::from_secs(SLEEP_TIME_SECS)).await;
                }
            } => {},
            _ = self.cancellation_token.cancelled() => {},
        }
    }
}
