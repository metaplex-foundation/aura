#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::{
        fs::File,
        io::{self, Read},
        str::FromStr,
        sync::Arc,
    };

    use blockbuster::token_metadata::{
        accounts::Metadata,
        types::{Collection, Creator, Key},
    };
    use entities::{
        api_req_params::{GetAsset, Options},
        models::{MetadataInfo, Mint, TokenAccount},
    };
    use interface::account_balance::MockAccountBalanceGetter;
    use metrics_utils::{
        red::RequestErrorDurationMetrics, ApiMetricsConfig, BackfillerMetricsConfig,
        IngesterMetricsConfig,
    };
    use nft_ingester::{
        backfiller::DirectBlockParser,
        buffer::Buffer,
        config::JsonMiddlewareConfig,
        json_worker::JsonWorker,
        processors::{
            account_based::{
                mplx_updates_processor::MplxAccountsProcessor,
                token_updates_processor::TokenAccountsProcessor,
            },
            transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
        },
        raydium_price_fetcher::RaydiumTokenPriceFetcher,
        transaction_ingester,
    };
    use rocks_db::{batch_savers::BatchSaveStorage, columns::offchain_data::OffChainData, Storage};
    use solana_sdk::pubkey::Pubkey;
    use testcontainers::clients::Cli;
    use tokio::{
        sync::{broadcast, Mutex},
        task::JoinSet,
    };
    use usecase::proofs::MaybeProofChecker;

    // corresponds to So11111111111111111111111111111111111111112
    pub const NATIVE_MINT_PUBKEY: Pubkey = Pubkey::new_from_array([
        6, 155, 136, 87, 254, 171, 129, 132, 251, 104, 127, 99, 70, 24, 192, 53, 218, 196, 57, 220,
        26, 235, 59, 85, 152, 160, 240, 0, 0, 0, 0, 1,
    ]);

    // 242856151 slot when decompress happened

    async fn process_bubblegum_transactions(
        mutexed_tasks: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
        env_rocks: Arc<rocks_db::Storage>,
        _buffer: Arc<Buffer>,
    ) {
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
        let transactions_storage = Storage::open_cfs(
            &format!("{}{}", tx_storage_dir.path().to_str().unwrap(), "/test_rocks"),
            vec![
                "BUBBLEGUM_SLOTS",
                "ASSET_OWNER",
                "ASSET_AUTHORITY",
                "RAW_BLOCK_CBOR_ENCODED",
                "ASSET_DYNAMIC",
                "ASSET_COLLECTION",
                "ASSET_STATIC",
                "SIGNATURE_IDX",
                "CL_LEAF",
                "CL_ITEMS",
                "ASSETS_UPDATED_IN_SLOT_IDX",
                "ASSET_LEAF",
                "SLOT_ASSET_IDX",
                "OFFCHAIN_DATA",
            ],
            mutexed_tasks.clone(),
            red_metrics.clone(),
        )
        .unwrap();

        let rocks_storage = Arc::new(transactions_storage);

        let bubblegum_updates_processor =
            Arc::new(BubblegumTxProcessor::new(env_rocks, Arc::new(IngesterMetricsConfig::new())));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let _consumer = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            rocks_storage.clone(),
            Arc::new(BackfillerMetricsConfig::new()),
        ));
        let _producer = rocks_storage.clone();

        let (_shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);
    }

    async fn process_accounts(
        storage: &mut BatchSaveStorage,
        nft_created_slot: i64,
        mint: &Pubkey,
    ) {
        let mplx_accs_parser = MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let spl_token_accs_parser =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let owner = Pubkey::from_str("3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM").unwrap();

        let token_acc = TokenAccount {
            pubkey: Pubkey::from_str("DrWX2HdxYcvy5o34YYbpgnbHrqeaiXoMejqCDRLKNqX").unwrap(),
            mint: *mint,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: nft_created_slot,
            amount: 1,
            write_version: 1,
        };

        let mint_acc = Mint {
            pubkey: *mint,
            slot_updated: nft_created_slot,
            supply: 1,
            decimals: 0,
            mint_authority: Some(
                Pubkey::from_str("ywx1vh2bG1brfX8SqWMxGiivNTZjMHf9vuKrXKt4pNT").unwrap(),
            ),
            freeze_authority: None,
            token_program: Default::default(),
            extensions: None,
            write_version: 1,
        };

        spl_token_accs_parser
            .transform_and_save_token_account(storage, token_acc.pubkey, &token_acc)
            .unwrap();

        spl_token_accs_parser.transform_and_save_mint_account(storage, &mint_acc).unwrap();

        let decompressed_token_data = MetadataInfo {
            metadata: Metadata {
                key: Key::MetadataV1,
                update_authority: Pubkey::from_str("ywx1vh2bG1brfX8SqWMxGiivNTZjMHf9vuKrXKt4pNT")
                    .unwrap(),
                mint: *mint,
                name: "Mufacka name".to_string(),
                symbol: "SSNC".to_string(),
                uri: "https://arweave.net/nbCWy-OEu7MG5ORuJMurP5A-65qO811R-vL_8l_JHQM".to_string(),
                seller_fee_basis_points: 100,
                primary_sale_happened: false,
                is_mutable: true,
                edition_nonce: Some(255),
                token_standard: Some(
                    blockbuster::token_metadata::types::TokenStandard::NonFungible,
                ),
                collection: Some(Collection {
                    verified: false,
                    key: Pubkey::from_str("3yMfqHsajYFw2Yw6C4kwrvHRESMg9U7isNVJuzNETJKG").unwrap(),
                }),
                uses: None,
                collection_details: None,
                programmable_config: None,
                creators: Some(vec![
                    Creator {
                        address: Pubkey::from_str("3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM")
                            .unwrap(),
                        verified: true,
                        share: 99,
                    },
                    Creator {
                        address: Pubkey::from_str("5zgWmEx4ppdh6LfaPUmfJG2gBAK8bC2gBv7zshD6N1hG")
                            .unwrap(),
                        verified: false,
                        share: 1,
                    },
                ]),
            },
            slot_updated: nft_created_slot as u64,
            lamports: 1,
            executable: false,
            metadata_owner: None,
            write_version: 1,
            rent_epoch: 0,
        };

        mplx_accs_parser
            .transform_and_store_metadata_account(storage, *mint, &decompressed_token_data)
            .unwrap();
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_decompress_ideal_flow() {
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

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

        let buffer = Arc::new(Buffer::new());

        let mint = Pubkey::from_str("7DvMvi5iw8a4ESsd3bArGgduhvUgfD95iQmgucajgMPQ").unwrap();

        process_bubblegum_transactions(
            mutexed_tasks.clone(),
            env.rocks_env.storage.clone(),
            buffer.clone(),
        )
        .await;

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        process_accounts(&mut batch_storage, 242856151, &mint).await;
        batch_storage.flush().unwrap();

        let file = File::open("./tests/artifacts/expected_decompress_result.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let payload = GetAsset {
            id: mint.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let asset_info = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(asset_info["compression"], expected_results["compression"]);
        assert_eq!(asset_info["grouping"], expected_results["grouping"]);
        assert_eq!(asset_info["royalty"], expected_results["royalty"]);
        assert_eq!(asset_info["creators"], expected_results["creators"]);
        assert_eq!(asset_info["ownership"], expected_results["ownership"]);
        assert_eq!(asset_info["supply"], expected_results["supply"]);
        assert_eq!(asset_info["mutable"], expected_results["mutable"]);
        assert_eq!(asset_info["burnt"], expected_results["burnt"]);

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_decompress_first_mint_then_decompress_same_slot() {
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

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

        let buffer = Arc::new(Buffer::new());

        let mint = Pubkey::from_str("7DvMvi5iw8a4ESsd3bArGgduhvUgfD95iQmgucajgMPQ").unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        process_accounts(&mut batch_storage, 242856151, &mint).await;
        batch_storage.flush().unwrap();

        process_bubblegum_transactions(
            mutexed_tasks.clone(),
            env.rocks_env.storage.clone(),
            buffer.clone(),
        )
        .await;

        let file = File::open("./tests/artifacts/expected_decompress_result.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let payload = GetAsset {
            id: mint.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let asset_info = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(asset_info["compression"], expected_results["compression"]);
        assert_eq!(asset_info["grouping"], expected_results["grouping"]);
        assert_eq!(asset_info["royalty"], expected_results["royalty"]);
        assert_eq!(asset_info["creators"], expected_results["creators"]);
        assert_eq!(asset_info["ownership"], expected_results["ownership"]);
        assert_eq!(asset_info["supply"], expected_results["supply"]);
        assert_eq!(asset_info["mutable"], expected_results["mutable"]);
        assert_eq!(asset_info["burnt"], expected_results["burnt"]);

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_decompress_first_mint_then_decompress_diff_slots() {
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

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

        let buffer = Arc::new(Buffer::new());

        let mint = Pubkey::from_str("7DvMvi5iw8a4ESsd3bArGgduhvUgfD95iQmgucajgMPQ").unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        process_accounts(&mut batch_storage, 252856151, &mint).await;
        batch_storage.flush().unwrap();

        process_bubblegum_transactions(
            mutexed_tasks.clone(),
            env.rocks_env.storage.clone(),
            buffer.clone(),
        )
        .await;

        let file = File::open("./tests/artifacts/expected_decompress_result.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let payload = GetAsset {
            id: mint.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let asset_info = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(asset_info["compression"], expected_results["compression"]);
        assert_eq!(asset_info["grouping"], expected_results["grouping"]);
        assert_eq!(asset_info["royalty"], expected_results["royalty"]);
        assert_eq!(asset_info["creators"], expected_results["creators"]);
        assert_eq!(asset_info["ownership"], expected_results["ownership"]);
        assert_eq!(asset_info["supply"], expected_results["supply"]);
        assert_eq!(asset_info["mutable"], expected_results["mutable"]);
        assert_eq!(asset_info["burnt"], expected_results["burnt"]);

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_decompress_first_decompress_then_mint_diff_slots() {
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

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

        let buffer = Arc::new(Buffer::new());

        let mint = Pubkey::from_str("7DvMvi5iw8a4ESsd3bArGgduhvUgfD95iQmgucajgMPQ").unwrap();

        process_bubblegum_transactions(
            mutexed_tasks.clone(),
            env.rocks_env.storage.clone(),
            buffer.clone(),
        )
        .await;

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        process_accounts(&mut batch_storage, 252856151, &mint).await;
        batch_storage.flush().unwrap();

        let file = File::open("./tests/artifacts/expected_decompress_result.json").unwrap();
        let mut reader = io::BufReader::new(file);

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        let expected_results: serde_json::Value = serde_json::from_str(&contents).unwrap();

        let payload = GetAsset {
            id: mint.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let asset_info = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(asset_info["compression"], expected_results["compression"]);
        assert_eq!(asset_info["grouping"], expected_results["grouping"]);
        assert_eq!(asset_info["royalty"], expected_results["royalty"]);
        assert_eq!(asset_info["creators"], expected_results["creators"]);
        assert_eq!(asset_info["ownership"], expected_results["ownership"]);
        assert_eq!(asset_info["supply"], expected_results["supply"]);
        assert_eq!(asset_info["mutable"], expected_results["mutable"]);
        assert_eq!(asset_info["burnt"], expected_results["burnt"]);

        env.teardown().await;
    }
}
