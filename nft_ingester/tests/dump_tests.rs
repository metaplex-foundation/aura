#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::sync::Arc;

    use entities::{
        api_req_params::GetByMethodsOptions,
        enums::ASSET_TYPES,
        models::{TokenAccount, UrlWithStatus},
    };
    use metrics_utils::{IngesterMetricsConfig, SynchronizerMetricsConfig};
    use nft_ingester::{
        index_syncronizer::Synchronizer,
        processors::account_based::token_updates_processor::TokenAccountsProcessor,
    };
    use postgre_client::{
        model::{AssetSortBy, AssetSortDirection, AssetSorting, SearchAssetsFilter},
        storage_traits::AssetPubkeyFilteredFetcher,
    };
    use rocks_db::batch_savers::BatchSaveStorage;
    use setup::rocks::*;
    use solana_program::pubkey::Pubkey;
    use tempfile::TempDir;
    use testcontainers::clients::Cli;

    // corresponds to So11111111111111111111111111111111111111112
    pub const NATIVE_MINT_PUBKEY: Pubkey = Pubkey::new_from_array([
        6, 155, 136, 87, 254, 171, 129, 132, 251, 104, 127, 99, 70, 24, 192, 53, 218, 196, 57, 220,
        26, 235, 59, 85, 152, 160, 240, 0, 0, 0, 0, 1,
    ]);

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    #[tracing_test::traced_test]
    #[ignore = "FIXME: on macos, the generated CSV files cannot be found"]
    async fn test_csv_export_from_rocks_import_into_pg() {
        let env = RocksTestEnvironment::new(&[]);
        let number_of_assets = 1000;
        let generated_assets = env.generate_assets(number_of_assets, 25).await;
        let storage = env.storage;

        let mut batch_storage =
            BatchSaveStorage::new(storage.clone(), 10, Arc::new(IngesterMetricsConfig::new()));
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        for i in 0..number_of_assets {
            let key = Pubkey::new_unique();
            let token_account = TokenAccount {
                pubkey: key,
                mint: generated_assets.pubkeys[i],
                delegate: None,
                owner: generated_assets.owners[i].owner.value.unwrap(),
                extensions: None,
                frozen: false,
                delegated_amount: 0,
                slot_updated: 10,
                amount: 1000,
                write_version: 10,
            };
            token_accounts_processor
                .transform_and_save_token_account(&mut batch_storage, key, &token_account)
                .unwrap();
        }
        batch_storage.flush().unwrap();

        let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let temp_dir_path = temp_dir.path();

        let temp_dir_path = temp_dir_path.to_str().unwrap();
        let cli: Cli = Cli::default();
        let pg_env = setup::pg::TestEnvironment::new_with_mount(&cli, temp_dir_path).await;
        let client = pg_env.client.clone();
        let syncronizer = Arc::new(Synchronizer::new(
            storage,
            client.clone(),
            2000,
            temp_dir_path.to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
            1,
        ));
        for asset_type in ASSET_TYPES {
            syncronizer.full_syncronize(&rx, asset_type).await.unwrap();
        }

        assert_eq!(pg_env.count_rows_in_metadata().await.unwrap(), 1);
        assert_eq!(pg_env.count_rows_in_creators().await.unwrap(), number_of_assets as i64);
        assert_eq!(pg_env.count_rows_in_assets().await.unwrap(), number_of_assets as i64);
        assert_eq!(pg_env.count_rows_in_authorities().await.unwrap(), number_of_assets as i64);
        assert_eq!(pg_env.count_rows_in_fungible_tokens().await.unwrap(), 1000 as i64);
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
                &GetByMethodsOptions { show_unverified_collections: true, ..Default::default() },
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

#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod mtg_441_tests {
    use std::sync::Arc;

    use entities::{
        api_req_params::{GetAsset, Options},
        enums::SpecificationAssetClass,
    };
    use interface::account_balance::MockAccountBalanceGetter;
    use metrics_utils::ApiMetricsConfig;
    use nft_ingester::{
        api::{dapi::rpc_asset_models::Asset, DasApi},
        config::JsonMiddlewareConfig,
        json_worker::JsonWorker,
        raydium_price_fetcher::RaydiumTokenPriceFetcher,
    };
    use rocks_db::Storage;
    use serde_json::Value;
    use setup::{rocks::RocksTestEnvironmentSetup, TestEnvironment};
    use testcontainers::clients::Cli;
    use tokio::{sync::Mutex, task::JoinSet};
    use usecase::proofs::MaybeProofChecker;

    use crate::tests::NATIVE_MINT_PUBKEY;

    const SLOT_UPDATED: u64 = 100;

    fn get_das_api(
        env: &TestEnvironment,
    ) -> DasApi<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        MockAccountBalanceGetter,
        RaydiumTokenPriceFetcher,
        Storage,
    > {
        DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        )
    }

    fn parse_asset(json: Value) -> Asset {
        serde_json::from_value::<Asset>(json).expect("Cannot parse 'Asset'.")
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn authority_none_collection_authority_some() {
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create_and_setup_from_closures(
            &cli,
            20,
            SLOT_UPDATED,
            &[SpecificationAssetClass::MplCoreAsset],
            RocksTestEnvironmentSetup::without_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_with_authority,
        )
        .await;

        let first_pubkey =
            generated_assets.static_details.first().expect("Cannot get first pubkey.").pubkey;

        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
        let api_res = get_das_api(&env)
            .get_asset(
                GetAsset {
                    id: first_pubkey.to_string(),
                    options: Options { show_unverified_collections: true, ..Default::default() },
                },
                mutexed_tasks,
            )
            .await;

        assert!(api_res.is_ok());
        let api_res = api_res.expect("Cannot run api call.");
        let res = parse_asset(api_res);
        assert!(res.id.eq(&first_pubkey.to_string()));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn authority_some_collection_authority_none() {
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create_and_setup_from_closures(
            &cli,
            20,
            SLOT_UPDATED,
            &[SpecificationAssetClass::MplCoreAsset],
            RocksTestEnvironmentSetup::with_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_without_authority,
        )
        .await;

        let first_pubkey =
            generated_assets.static_details.first().expect("Cannot get first pubkey.").pubkey;

        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
        let api_res = get_das_api(&env)
            .get_asset(
                GetAsset {
                    id: first_pubkey.to_string(),
                    options: Options { show_unverified_collections: true, ..Default::default() },
                },
                mutexed_tasks,
            )
            .await;
        assert!(api_res.is_ok());
        let api_res = api_res.expect("Cannot run api call.");
        let res = parse_asset(api_res);
        assert!(res.id.eq(&first_pubkey.to_string()));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn authority_some_collection_authority_some() {
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create_and_setup_from_closures(
            &cli,
            20,
            SLOT_UPDATED,
            &[SpecificationAssetClass::MplCoreAsset],
            RocksTestEnvironmentSetup::with_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_with_authority,
        )
        .await;

        let first_pubkey =
            generated_assets.static_details.first().expect("Cannot get first pubkey.").pubkey;

        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
        let api_res = get_das_api(&env)
            .get_asset(
                GetAsset {
                    id: first_pubkey.to_string(),
                    options: Options { show_unverified_collections: true, ..Default::default() },
                },
                mutexed_tasks,
            )
            .await;
        assert!(api_res.is_ok());
        let api_res = api_res.expect("Cannot run api call.");
        let res = parse_asset(api_res);
        assert!(res.id.eq(&first_pubkey.to_string()));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn authority_none_collection_authority_none() {
        let cli = Cli::default();
        let (env, generated_assets) = TestEnvironment::create_and_setup_from_closures(
            &cli,
            20,
            SLOT_UPDATED,
            &[SpecificationAssetClass::MplCoreAsset],
            RocksTestEnvironmentSetup::without_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_without_authority,
        )
        .await;

        let first_pubkey =
            generated_assets.static_details.first().expect("Cannot get first pubkey.").pubkey;

        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
        let api_res = get_das_api(&env)
            .get_asset(
                GetAsset {
                    id: first_pubkey.to_string(),
                    options: Options { show_unverified_collections: true, ..Default::default() },
                },
                mutexed_tasks,
            )
            .await;
        assert!(api_res.is_ok());
        let api_res = api_res.expect("Cannot run api call.");
        let res = parse_asset(api_res);
        assert!(res.id.eq(&first_pubkey.to_string()));
    }
}
