#[cfg(test)]
mod tests {
    use nft_ingester::price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater, SOLANA_CURRENCY};
    use testcontainers::clients::Cli;

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
}
