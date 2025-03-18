#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::{
        fs::File,
        io::{self, Read},
        sync::Arc,
    };

    use entities::api_req_params::{DisplayOptions, GetAsset, GetAssetProof};
    use interface::account_balance::MockAccountBalanceGetter;
    use metrics_utils::{
        red::RequestErrorDurationMetrics, ApiMetricsConfig, BackfillerMetricsConfig,
        IngesterMetricsConfig,
    };
    use nft_ingester::{
        backfiller::DirectBlockParser,
        buffer::Buffer,
        config::{HealthCheckInfo, JsonMiddlewareConfig},
        json_worker::JsonWorker,
        processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
        raydium_price_fetcher::RaydiumTokenPriceFetcher,
        transaction_ingester,
    };
    use rocks_db::{columns::offchain_data::OffChainData, migrator::MigrationState, Storage};
    use solana_program::pubkey::Pubkey;
    use testcontainers::clients::Cli;
    use usecase::proofs::MaybeProofChecker;

    // corresponds to So11111111111111111111111111111111111111112
    pub const NATIVE_MINT_PUBKEY: Pubkey = Pubkey::new_from_array([
        6, 155, 136, 87, 254, 171, 129, 132, 251, 104, 127, 99, 70, 24, 192, 53, 218, 196, 57, 220,
        26, 235, 59, 85, 152, 160, 240, 0, 0, 0, 0, 1,
    ]);

    #[tokio::test(flavor = "multi_thread")]
    #[tracing_test::traced_test]
    #[ignore = "FIXME: column families not opened error (probably outdated)"]
    async fn test_bubblegum_proofs() {
        // write slots we need to parse because backfiller dropped it during raw transactions saving
        let _slots_to_parse = &[
            242049108, 242049247, 242049255, 242050728, 242050746, 242143893, 242143906, 242239091,
            242239108, 242248687, 242560746, 242847845, 242848373, 242853752, 242856151, 242943141,
            242943774, 242947970, 242948187, 242949333, 242949940, 242951695, 242952638,
        ];

        let tx_storage_dir = tempfile::TempDir::new().unwrap();

        let storage_archieve = File::open("./tests/artifacts/test_rocks.zip").unwrap();

        zip_extract::extract(storage_archieve, tx_storage_dir.path(), false).unwrap();

        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let transactions_storage = Storage::open(
            &format!("{}{}", tx_storage_dir.path().to_str().unwrap(), "/test_rocks"),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap();

        let rocks_storage = Arc::new(transactions_storage);

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            HealthCheckInfo {
                node_name: Some("test".to_string()),
                app_version: "1.0".to_string(),
                image_info: None,
            },
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
        );

        let _buffer = Arc::new(Buffer::new());

        let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        ));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let _consumer = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
        ));
        let _producer = rocks_storage.clone();

        let file = File::open("./tests/artifacts/expected_proofs.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let assets_to_test_proof_for = vec![
            "HZPrFymDBjKcYnUGFtQR6uZEoqN9MQCiEvYa75u3xTeX",
            "F8XkxardSug8FSViEGF5XMSXQ93wBYmqmiGuWiYt2uV8",
            "8roA736sYuZccUMGpT67LqtX86yZ4fb99Ga2Up6adZ9y",
            "Df2fyPsqeDhCkyteWVwsL1tTfhFygchuDBDy8YUhHJPk",
            "Ahsn3GcwiwuXCavbet26YCMGifG8XAEEZAkH6A8KM8sa",
            "9ZEFH8WVtfRqAbLdzkfYXANWVtrafrQjrUkZcswsZgcG",
            "HWxE2EReU9VEixVNSw9e1QGUAGQJ32VACAPSH6dPSXbU",
            "7aKVZtBGW37kR3usNCUTdGHbUtwiLwVwkbc1DT7ikPaw",
        ];

        for asset in assets_to_test_proof_for.iter() {
            let payload = GetAssetProof { id: asset.to_string() };
            let proof_result = api.get_asset_proof(payload).await.unwrap();

            assert_eq!(proof_result, expected_results[*asset]);
        }

        env.teardown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tracing_test::traced_test]
    #[ignore = "FIXME: column families not opened error (probably outdated)"]
    async fn test_asset_compression_info() {
        // write slots we need to parse because backfiller dropped it during raw transactions saving
        let _slots_to_parse = &[
            242049108, 242049247, 242049255, 242050728, 242050746, 242143893, 242143906, 242239091,
            242239108, 242248687, 242560746, 242847845, 242848373, 242853752, 242856151, 242943141,
            242943774, 242947970, 242948187, 242949333, 242949940, 242951695, 242952638,
        ];

        let tx_storage_dir = tempfile::TempDir::new().unwrap();

        let storage_archieve = File::open("./tests/artifacts/test_rocks.zip").unwrap();

        zip_extract::extract(storage_archieve, tx_storage_dir.path(), false).unwrap();

        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let transactions_storage = Storage::open(
            &format!("{}{}", tx_storage_dir.path().to_str().unwrap(), "/test_rocks"),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap();

        let rocks_storage = Arc::new(transactions_storage);

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            HealthCheckInfo {
                node_name: Some("test".to_string()),
                app_version: "1.0".to_string(),
                image_info: None,
            },
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
        );

        let _buffer = Arc::new(Buffer::new());

        let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        ));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let _consumer = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
        ));
        let _producer = rocks_storage.clone();

        let metadata = OffChainData {
            url: Some("https://supersweetcollection.notarealurl/token.json".to_string()),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            ..Default::default()
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone().unwrap(), metadata)
            .unwrap();

        let metadata = OffChainData {
            url: Some(
                "https://arweave.net/nbCWy-OEu7MG5ORuJMurP5A-65qO811R-vL_8l_JHQM".to_string(),
            ),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            ..Default::default()
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone().unwrap(), metadata)
            .unwrap();

        let file = File::open("./tests/artifacts/expected_compression.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let assets_to_test_proof_for = vec![
            "HZPrFymDBjKcYnUGFtQR6uZEoqN9MQCiEvYa75u3xTeX",
            "F8XkxardSug8FSViEGF5XMSXQ93wBYmqmiGuWiYt2uV8",
            "8roA736sYuZccUMGpT67LqtX86yZ4fb99Ga2Up6adZ9y",
            "Df2fyPsqeDhCkyteWVwsL1tTfhFygchuDBDy8YUhHJPk",
            "Ahsn3GcwiwuXCavbet26YCMGifG8XAEEZAkH6A8KM8sa",
            "9ZEFH8WVtfRqAbLdzkfYXANWVtrafrQjrUkZcswsZgcG",
            "HWxE2EReU9VEixVNSw9e1QGUAGQJ32VACAPSH6dPSXbU",
            "7aKVZtBGW37kR3usNCUTdGHbUtwiLwVwkbc1DT7ikPaw",
        ];

        for asset in assets_to_test_proof_for.iter() {
            let payload = GetAsset {
                id: asset.to_string(),
                options: Some(DisplayOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                }),
            };
            let asset_info = api.get_asset(payload).await.unwrap();

            assert_eq!(asset_info["compression"], expected_results[*asset]);
        }

        env.teardown().await;
    }
}
