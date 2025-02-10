use std::sync::Arc;

use entities::models::MetadataDownloadTask;
use postgre_client::PgClient;
use tokio::{
    sync::{broadcast::Receiver, mpsc::Sender, Mutex},
    task::JoinSet,
    time::{sleep, Duration},
};
use tracing::error;

use crate::metadata_workers::TaskType;

pub const SLEEP_TIME: u64 = 1;

pub struct TasksStreamer {
    pub db_conn: Arc<PgClient>,
    pub shutdown_rx: Receiver<()>,
    pub pending_tasks_sender: Sender<MetadataDownloadTask>,
    pub refresh_tasks_sender: Sender<MetadataDownloadTask>,
    // to check if it's empty and populate it
    pub tasks_receiver: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
}

impl TasksStreamer {
    pub fn new(
        db_conn: Arc<PgClient>,
        shutdown_rx: Receiver<()>,
        pending_tasks_sender: Sender<MetadataDownloadTask>,
        refresh_tasks_sender: Sender<MetadataDownloadTask>,
        tasks_receiver: Arc<Mutex<Receiver<MetadataDownloadTask>>>,
    ) -> Self {
        Self { db_conn, shutdown_rx, pending_tasks_sender, refresh_tasks_sender, tasks_receiver }
    }

    pub async fn run(self, num_of_tasks: i32, tasks: &mut JoinSet<()>) {
        self.stream_tasks(TaskType::Pending, num_of_tasks, tasks).await;
        self.stream_tasks(TaskType::Refresh, num_of_tasks, tasks).await;
    }

    async fn stream_tasks(&self, task_type: TaskType, num_of_tasks: i32, tasks: &mut JoinSet<()>) {
        let mut shutdown_rx = self.shutdown_rx.resubscribe();
        let tasks_receiver = self.tasks_receiver.clone();
        let db_conn = self.db_conn.clone();
        let tasks_sender = match task_type {
            TaskType::Pending => self.pending_tasks_sender.clone(),
            TaskType::Refresh => self.refresh_tasks_sender.clone(),
        };

        tasks.spawn(async move {
            tokio::select! {
                _ = async move {
                    loop {
                        let is_empty = tasks_receiver.lock().await.is_empty();

                        if is_empty {
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
                        }
                        sleep(Duration::from_secs(SLEEP_TIME)).await;
                    }
                } => {},
                _ = shutdown_rx.recv() => {},
            }
        });
    }
}
