#[cfg(test)]
mod tests {
    use interface::migration_version_manager::MigrationVersionManager;
    use setup::TestEnvironment;
    use std::collections::HashSet;
    use testcontainers::clients::Cli;

    #[tokio::test]
    async fn test_migration_management() {
        let cli = Cli::default();
        let (env, _) = TestEnvironment::create(&cli, 0, 0).await;
        env.pg_env.client.apply_migration(150).await.unwrap();
        env.pg_env.client.apply_migration(200).await.unwrap();
        let all_migrations = env
            .pg_env
            .client
            .get_all_applied_migrations()
            .await
            .unwrap();

        let mut testing_response = HashSet::new();
        testing_response.insert(150);
        testing_response.insert(200);
        assert_eq!(testing_response, all_migrations)
    }
}
