#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::sync::Arc;

    use metrics_utils::SynchronizerMetricsConfig;
    use nft_ingester::index_syncronizer::Synchronizer;
    use postgre_client::storage_traits::AssetIndexStorage;
    use setup::rocks::*;
    use tempfile::TempDir;
    use testcontainers::clients::Cli;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_csv_export_from_rocks_import_into_pg() {
        let env = RocksTestEnvironment::new(&[]);
        let number_of_assets = 1000;
        let _generated_assets = env.generate_assets(number_of_assets, 25);
        let storage = env.storage;
        let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let temp_dir_path = temp_dir.path();

        let temp_dir_path = temp_dir_path.to_str().unwrap();
        let cli: Cli = Cli::default();
        let pg_env = setup::pg::TestEnvironment::new_with_mount(&cli, temp_dir_path).await;
        let client = pg_env.client.clone();
        let syncronizer = Synchronizer::new(
            storage,
            client.clone(),
            1,
            2000,
            temp_dir_path.to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
        );
        syncronizer.full_syncronize(rx).await.unwrap();
        assert_eq!(pg_env.count_rows_in_metadata().await.unwrap(), 1);
        assert_eq!(
            pg_env.count_rows_in_creators().await.unwrap(),
            number_of_assets as i64
        );
        assert_eq!(
            pg_env.count_rows_in_assets().await.unwrap(),
            number_of_assets as i64
        );
        let metadata_key_set = client.get_existing_metadata_keys().await.unwrap();
        assert_eq!(metadata_key_set.len(), 1);
        pg_env.teardown().await;
        temp_dir.close().unwrap();
    }
}
