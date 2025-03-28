#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use chrono::{DateTime, Days, Utc};
    use entities::{
        enums::{OffchainDataMutability, TaskStatus},
        models::Task,
    };
    use setup::pg::*;
    use testcontainers::clients::Cli;

    #[tokio::test]
    async fn test_select_pending_tasks() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        let pending_mutable_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Pending,
        };
        let pending_immutable_task = Task {
            metadata_url: "https://url2.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Pending,
        };
        let mut tasks = vec![pending_mutable_task.clone(), pending_immutable_task.clone()];

        asset_index_storage.insert_new_tasks(&mut tasks).await.unwrap();
        let selected = asset_index_storage.get_pending_metadata_tasks(2).await.unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected.get(0).unwrap().metadata_url, pending_mutable_task.metadata_url);
        assert_eq!(selected.get(1).unwrap().metadata_url, pending_immutable_task.metadata_url);
    }

    #[tokio::test]
    async fn test_select_pending_tasks_that_shouldnt_be_processed() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;
        let next_try_at = Utc::now().checked_add_days(Days::new(10)).unwrap();

        let pending_mutable_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at,
            status: TaskStatus::Pending,
        };
        let pending_immutable_task = Task {
            metadata_url: "https://url2.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at,
            status: TaskStatus::Pending,
        };
        let mut tasks = vec![pending_mutable_task.clone(), pending_immutable_task.clone()];

        asset_index_storage.insert_new_tasks(&mut tasks).await.unwrap();
        let selected = asset_index_storage.get_pending_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 0);
    }

    #[tokio::test]
    async fn test_select_successful_tasks() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;
        let next_try_at = Utc::now().checked_sub_days(Days::new(10)).unwrap();

        let pending_mutable_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at,
            status: TaskStatus::Success,
        };

        // will not be selected because it's immutable so refresh makes no sense
        let pending_immutable_task = Task {
            metadata_url: "https://url2.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at,
            status: TaskStatus::Success,
        };

        let mut tasks = vec![pending_mutable_task.clone(), pending_immutable_task.clone()];

        asset_index_storage.insert_new_tasks(&mut tasks).await.unwrap();
        let selected = asset_index_storage.get_refresh_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected.get(0).unwrap().metadata_url, pending_mutable_task.metadata_url);
    }

    #[tokio::test]
    async fn test_select_successful_tasks_that_shouldnt_be_processed() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;
        let next_try_at = Utc::now().checked_add_days(Days::new(10)).unwrap();

        let pending_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at,
            status: TaskStatus::Success,
        };

        asset_index_storage.insert_new_tasks(&mut vec![pending_task.clone()]).await.unwrap();
        let selected = asset_index_storage.get_refresh_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 0);
    }

    #[tokio::test]
    async fn test_immutable_task_is_unselectable_for_refresh() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        let immutable_and_pending_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Pending,
        };
        let immutable_and_successful_task = Task {
            metadata_url: "https://url2.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Success,
        };
        let immutable_and_failed_task = Task {
            metadata_url: "https://url3.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Failed,
        };
        let mut tasks: Vec<Task> = vec![
            immutable_and_pending_task.clone(),
            immutable_and_successful_task.clone(),
            immutable_and_failed_task.clone(),
        ];

        asset_index_storage.insert_new_tasks(&mut tasks).await.unwrap();
        let selected = asset_index_storage.get_refresh_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 0);
    }

    #[tokio::test]
    async fn test_failed_task_is_unselectable() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        let failed_and_mutable_task = Task {
            metadata_url: "https://url1.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Mutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Failed,
        };
        let failed_and_immutable_task = Task {
            metadata_url: "https://url2.com".to_string(),
            etag: None,
            last_modified_at: None,
            mutability: OffchainDataMutability::Immutable,
            next_try_at: DateTime::<Utc>::from(Utc::now()),
            status: TaskStatus::Failed,
        };
        let mut tasks = vec![failed_and_mutable_task.clone(), failed_and_immutable_task.clone()];

        asset_index_storage.insert_new_tasks(&mut tasks).await.unwrap();
        let selected = asset_index_storage.get_pending_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 0);
        let selected = asset_index_storage.get_refresh_metadata_tasks(1).await.unwrap();
        assert_eq!(selected.len(), 0);
    }
}
