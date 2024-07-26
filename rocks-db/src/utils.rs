use tokio::task::JoinSet;
use log::{info, error};

pub async fn wait_for_all_tasks_to_finish<T: 'static>(tasks: &mut JoinSet<T>, task_name: String) {
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the {:?}s was finished", task_name)
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Task {:?} panic: {:?}", task_name, err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Task {:?} error: {}", task_name, err);
            }
        }
    }
}