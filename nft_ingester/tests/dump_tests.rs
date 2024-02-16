#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::fs::File;

    use csv::WriterBuilder;
    use setup::rocks::*;
    use tempfile::TempDir;
    use testcontainers::clients::Cli;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_scv_export_from_rocks_import_into_pg() {
        let env = RocksTestEnvironment::new(&[]);
        let number_of_assets = 1000;
        let _generated_assets = env.generate_assets(number_of_assets, 25);
        let storage = env.storage;
        let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let temp_dir_path = temp_dir.path().to_str().unwrap();
        let tasks_path = format!("{}/tasks.csv", temp_dir_path);
        let assets_path = format!("{}/assets.csv", temp_dir_path);
        let creators_path = format!("{}/creators.csv", temp_dir_path);
        let tasks_file = File::create(tasks_path.to_string()).unwrap();
        let assets_file = File::create(assets_path.to_string()).unwrap();
        let creators_file = File::create(creators_path.to_string()).unwrap();
        let mut tasks_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(tasks_file);
        let mut assets_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(assets_file);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(creators_file);

        storage
            .dump_csv(
                &mut tasks_writer,
                &mut creators_writer,
                &mut assets_writer,
                2000,
                rx,
            )
            .await
            .unwrap();

        let cli: Cli = Cli::default();
        let pg_env = setup::pg::TestEnvironment::new_with_mount(&cli, temp_dir_path).await;
        let client = pg_env.client.clone();
        client
            .copy_all(
                tasks_path.to_string(),
                creators_path.to_string(),
                assets_path.to_string(),
            )
            .await
            .unwrap();
        assert_eq!(pg_env.count_rows_in_metadata().await.unwrap(), 1);
        assert_eq!(
            pg_env.count_rows_in_creators().await.unwrap(),
            number_of_assets as i64
        );
        assert_eq!(
            pg_env.count_rows_in_assets().await.unwrap(),
            number_of_assets as i64
        );
        pg_env.teardown().await;
    }
}
