#[cfg(test)]
mod tests {
    use entities::api_req_params::{SearchAssets, SearchAssetsOptions};
    use metrics_utils::ApiMetricsConfig;
    use nft_ingester::api::dapi::response::AssetList;
    use nft_ingester::config::JsonMiddlewareConfig;
    use nft_ingester::json_worker::JsonWorker;
    use nft_ingester::price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater, SOLANA_CURRENCY};
    use solana_client::nonblocking::rpc_client::RpcClient;
    use std::sync::Arc;
    use testcontainers::clients::Cli;
    use tokio::sync::Mutex;
    use tokio::task::JoinSet;
    use usecase::proofs::MaybeProofChecker;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_price_fetch() {
        let cnt = 0;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let solana_price_updater =
            SolanaPriceUpdater::new(env.rocks_env.storage.clone(), CoinGeckoPriceFetcher::new());
        solana_price_updater.update_price().await.unwrap();

        let price = env
            .rocks_env
            .storage
            .token_prices
            .get(SOLANA_CURRENCY.to_string())
            .unwrap()
            .unwrap();
        assert!(price.price > 0.0)
    }

    #[cfg(feature = "rpc_tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_balance() {
        let cnt = 0;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let solana_price_updater =
            SolanaPriceUpdater::new(env.rocks_env.storage.clone(), CoinGeckoPriceFetcher::new());
        solana_price_updater.update_price().await.unwrap();
        let api =
            nft_ingester::api::api_impl::DasApi::<MaybeProofChecker, JsonWorker, JsonWorker>::new(
                env.pg_env.client.clone(),
                env.rocks_env.storage.clone(),
                Arc::new(ApiMetricsConfig::new()),
                None,
                50,
                None,
                None,
                JsonMiddlewareConfig::default(),
                Arc::new(RpcClient::new(
                    "https://api.mainnet-beta.solana.com".to_string(),
                )),
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some("6GmTFg5SCs4zGfDEidUAJjS5pSrXEPwW8Rpfs3RHrbc5".to_string()),
            options: Some(SearchAssetsOptions {
                show_unverified_collections: true,
                show_native_balance: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let res = api
            .search_assets(payload, mutexed_tasks.clone())
            .await
            .unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();
        assert!(res.native_balance.unwrap().total_price > 0.0);
    }
}
