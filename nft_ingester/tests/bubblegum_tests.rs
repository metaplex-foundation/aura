#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use entities::api_req_params::{GetAsset, GetAssetProof, Options};
    use entities::models::OffChainData;
    use interface::account_balance::MockAccountBalanceGetter;
    use metrics_utils::red::RequestErrorDurationMetrics;
    use metrics_utils::{ApiMetricsConfig, BackfillerMetricsConfig, IngesterMetricsConfig};
    use nft_ingester::config::JsonMiddlewareConfig;
    use nft_ingester::json_worker::JsonWorker;
    use nft_ingester::raydium_price_fetcher::RaydiumTokenPriceFetcher;
    use nft_ingester::rocks_db::RocksDbManager;
    use nft_ingester::{
        backfiller::{DirectBlockParser, TransactionsParser},
        bubblegum_updates_processor::BubblegumTxProcessor,
        buffer::Buffer,
        transaction_ingester::{self, BackfillTransactionIngester},
    };
    use rocks_db::migrator::MigrationState;
    use rocks_db::{bubblegum_slots::BubblegumSlotGetter, Storage};
    use std::fs::File;
    use std::io::{self, Read};
    use std::sync::Arc;
    use testcontainers::clients::Cli;
    use tokio::sync::broadcast;
    use tokio::sync::Mutex;
    use tokio::task::JoinSet;
    use usecase::proofs::MaybeProofChecker;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_bubblegum_proofs() {
        // write slots we need to parse because backfiller dropped it during raw transactions saving
        let slots_to_parse = &[
            242049108, 242049247, 242049255, 242050728, 242050746, 242143893, 242143906, 242239091,
            242239108, 242248687, 242560746, 242847845, 242848373, 242853752, 242856151, 242943141,
            242943774, 242947970, 242948187, 242949333, 242949940, 242951695, 242952638,
        ];

        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let tx_storage_dir = tempfile::TempDir::new().unwrap();

        let storage_archieve = File::open("./tests/artifacts/test_rocks.zip").unwrap();

        zip_extract::extract(storage_archieve, tx_storage_dir.path(), false).unwrap();

        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let transactions_storage = Storage::open(
            &format!(
                "{}{}",
                tx_storage_dir.path().to_str().unwrap(),
                "/test_rocks"
            ),
            mutexed_tasks.clone(),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap();

        let rocks_storage = Arc::new(transactions_storage);

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let rocks_db = Arc::new(RocksDbManager::new_primary(env.rocks_env.storage.clone()));
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            rocks_db.clone(),
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
        );

        let buffer = Arc::new(Buffer::new());

        let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            buffer.json_tasks.clone(),
        ));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let consumer = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
        ));
        let producer = rocks_storage.clone();

        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let none: Option<Arc<Storage>> = None;
        TransactionsParser::<
            DirectBlockParser<BackfillTransactionIngester, Storage>,
            Storage,
            BubblegumSlotGetter,
        >::parse_slots(
            consumer.clone(),
            producer.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
            1,
            slots_to_parse,
            shutdown_rx,
            none,
        )
        .await
        .unwrap();

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
            let payload = GetAssetProof {
                id: asset.to_string(),
            };
            let proof_result = api.get_asset_proof(payload).await.unwrap();

            assert_eq!(proof_result, expected_results[*asset]);
        }

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_asset_compression_info() {
        // write slots we need to parse because backfiller dropped it during raw transactions saving
        let slots_to_parse = &[
            242049108, 242049247, 242049255, 242050728, 242050746, 242143893, 242143906, 242239091,
            242239108, 242248687, 242560746, 242847845, 242848373, 242853752, 242856151, 242943141,
            242943774, 242947970, 242948187, 242949333, 242949940, 242951695, 242952638,
        ];

        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let tx_storage_dir = tempfile::TempDir::new().unwrap();

        let storage_archieve = File::open("./tests/artifacts/test_rocks.zip").unwrap();

        zip_extract::extract(storage_archieve, tx_storage_dir.path(), false).unwrap();

        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let transactions_storage = Storage::open(
            &format!(
                "{}{}",
                tx_storage_dir.path().to_str().unwrap(),
                "/test_rocks"
            ),
            mutexed_tasks.clone(),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap();

        let rocks_storage = Arc::new(transactions_storage);

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let rocks_db = Arc::new(RocksDbManager::new_primary(env.rocks_env.storage.clone()));
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            rocks_db.clone(),
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
        );

        let buffer = Arc::new(Buffer::new());

        let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            buffer.json_tasks.clone(),
        ));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let consumer = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
        ));
        let producer = rocks_storage.clone();

        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let none: Option<Arc<Storage>> = None;
        TransactionsParser::<
            DirectBlockParser<BackfillTransactionIngester, Storage>,
            Storage,
            BubblegumSlotGetter,
        >::parse_slots(
            consumer.clone(),
            producer.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
            1,
            slots_to_parse,
            shutdown_rx,
            none,
        )
        .await
        .unwrap();

        let metadata = OffChainData {
            url: "https://supersweetcollection.notarealurl/token.json".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone(), metadata)
            .unwrap();

        let metadata = OffChainData {
            url: "https://arweave.net/nbCWy-OEu7MG5ORuJMurP5A-65qO811R-vL_8l_JHQM".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone(), metadata)
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
                options: Some(Options {
                    show_unverified_collections: true,
                    ..Default::default()
                }),
            };
            let asset_info = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

            assert_eq!(asset_info["compression"], expected_results[*asset]);
        }

        env.teardown().await;
    }
}
