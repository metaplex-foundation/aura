use std::sync::Arc;

use entities::models::JsonDownloadTask;
use postgre_client::PgClient;
use tokio::{
    sync::{broadcast::Receiver, mpsc::Sender, Mutex},
    task::JoinSet,
    time::{sleep, Duration},
};
use tracing::error;

pub const SLEEP_TIME: u64 = 1;

pub struct TasksStreamer {
    pub db_conn: Arc<PgClient>,
    pub tasks_sender: Sender<JsonDownloadTask>,
    // to check if it's empty and populate it
    pub tasks_receiver: Arc<Mutex<Receiver<JsonDownloadTask>>>,
}

impl TasksStreamer {
    pub fn new(
        db_conn: Arc<PgClient>,
        tasks_sender: Sender<JsonDownloadTask>,
        tasks_receiver: Arc<Mutex<Receiver<JsonDownloadTask>>>,
    ) -> Self {
        Self { db_conn, tasks_sender, tasks_receiver }
    }

    pub async fn run(self, mut shutdown_rx: Receiver<()>, num_of_tasks: i32, tasks: &mut JoinSet<()>) {
        tasks.spawn(async move {
            tokio::select! {
                _ = async move {
                    loop {
                        let is_empty = self.tasks_receiver.lock().await.is_empty().clone();
        
                        if is_empty {
                            let tasks = self.db_conn.get_pending_tasks(num_of_tasks).await;
        
                            match tasks {
                                Ok(tasks) => {
                                    for task in tasks.iter() {
                                        if let Err(err) = self.tasks_sender.send(task.clone()).await {
                                            error!(
                                                "Error during sending task to the tasks channel: {}",
                                                err.to_string()
                                            );
                                        }
                                    }
                                },
                                Err(err) => {
                                    error!("Error while selecting tasks for JsonDownloader: {}", err);
                                },
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
