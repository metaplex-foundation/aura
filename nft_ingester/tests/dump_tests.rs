#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::sync::Arc;

    use entities::{api_req_params::Options, models::UrlWithStatus};
    use metrics_utils::SynchronizerMetricsConfig;
    use nft_ingester::index_syncronizer::Synchronizer;
    use postgre_client::{
        model::{AssetSortBy, AssetSortDirection, AssetSorting, SearchAssetsFilter},
        storage_traits::{AssetIndexStorage, AssetPubkeyFilteredFetcher},
    };
    use setup::rocks::*;
    use solana_program::pubkey::Pubkey;
    use tempfile::TempDir;
    use testcontainers::clients::Cli;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_csv_export_from_rocks_import_into_pg() {
        let env = RocksTestEnvironment::new(&[]);
        let number_of_assets = 1000;
        let generated_assets = env.generate_assets(number_of_assets, 25);
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
            2000,
            temp_dir_path.to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
        );
        syncronizer.full_syncronize(&rx).await.unwrap();
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
        let key = metadata_key_set.iter().next().unwrap();
        let url = generated_assets.dynamic_details[0].url.value.to_string();
        let t = UrlWithStatus::new(&url, false);
        assert_eq!(*key, t.get_metadata_id());

        let keys = client
            .get_asset_pubkeys_filtered(
                &SearchAssetsFilter::default(),
                &AssetSorting {
                    sort_by: AssetSortBy::SlotCreated,
                    sort_direction: AssetSortDirection::Asc,
                },
                100,
                None,
                None,
                None,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        keys.iter().for_each(|k| {
            let key = Pubkey::try_from(k.pubkey.clone()).unwrap();
            assert!(generated_assets.pubkeys.contains(&key));
        });
        pg_env.teardown().await;
        temp_dir.close().unwrap();
    }
}
