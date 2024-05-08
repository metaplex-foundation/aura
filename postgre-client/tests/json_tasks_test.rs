#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use chrono::{DateTime, Days, Utc};
    use setup::pg::*;

    use entities::{enums::TaskStatus, models::Task};
    use testcontainers::clients::Cli;
    use tokio;

    #[tokio::test]
    async fn test_json_task_select() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        // make sure we select pending task
        let pending_task = Task {
            ofd_metadata_url: "https://url1.com".to_string(),
            ofd_locked_until: Some(Utc::now().checked_sub_days(Days::new(1)).unwrap()),
            ofd_attempts: 0,
            ofd_max_attempts: 10,
            ofd_error: None,
            ofd_status: TaskStatus::Pending,
        };
        asset_index_storage
            .insert_json_download_tasks(&mut vec![pending_task.clone()])
            .await
            .unwrap();

        let selected = asset_index_storage.get_pending_tasks(100).await.unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected.get(0).unwrap().metadata_url,
            pending_task.ofd_metadata_url
        );

        // make sure we do not select locked task
        let locked_until: DateTime<Utc> = Utc::now().checked_add_days(Days::new(1)).unwrap();
        let locked_task = Task {
            ofd_metadata_url: "https://url2.com".to_string(),
            ofd_locked_until: Some(locked_until),
            ofd_attempts: 0,
            ofd_max_attempts: 10,
            ofd_error: None,
            ofd_status: TaskStatus::Running,
        };
        asset_index_storage
            .insert_json_download_tasks(&mut vec![locked_task.clone()])
            .await
            .unwrap();

        let selected = asset_index_storage.get_pending_tasks(100).await.unwrap();

        // previous task was selected and locked until row updated so here we receive 0
        assert_eq!(selected.len(), 0);

        // make sure we do not select tasks with max_attempts reached
        let locked_until: DateTime<Utc> = Utc::now().checked_sub_days(Days::new(1)).unwrap();
        let attempts_reached_task = Task {
            ofd_metadata_url: "https://url3.com".to_string(),
            ofd_locked_until: Some(locked_until),
            ofd_attempts: 10,
            ofd_max_attempts: 10,
            ofd_error: None,
            ofd_status: TaskStatus::Pending,
        };
        asset_index_storage
            .insert_json_download_tasks(&mut vec![attempts_reached_task.clone()])
            .await
            .unwrap();

        let selected = asset_index_storage.get_pending_tasks(100).await.unwrap();

        assert_eq!(selected.len(), 0);

        // make sure we do not select failed tasks
        let locked_until: DateTime<Utc> = Utc::now().checked_sub_days(Days::new(1)).unwrap();
        let failed_task = Task {
            ofd_metadata_url: "https://url4.com".to_string(),
            ofd_locked_until: Some(locked_until),
            ofd_attempts: 1,
            ofd_max_attempts: 10,
            ofd_error: None,
            ofd_status: TaskStatus::Failed,
        };
        asset_index_storage
            .insert_json_download_tasks(&mut vec![failed_task.clone()])
            .await
            .unwrap();

        let selected = asset_index_storage.get_pending_tasks(100).await.unwrap();

        assert_eq!(selected.len(), 0);

        // make sure we do not select success tasks
        let locked_until: DateTime<Utc> = Utc::now().checked_sub_days(Days::new(1)).unwrap();
        let success_task = Task {
            ofd_metadata_url: "https://url5.com".to_string(),
            ofd_locked_until: Some(locked_until),
            ofd_attempts: 1,
            ofd_max_attempts: 10,
            ofd_error: None,
            ofd_status: TaskStatus::Success,
        };
        asset_index_storage
            .insert_json_download_tasks(&mut vec![success_task.clone()])
            .await
            .unwrap();

        let selected = asset_index_storage.get_pending_tasks(100).await.unwrap();

        assert_eq!(selected.len(), 0);
    }
}
