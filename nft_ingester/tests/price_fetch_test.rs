#[cfg(test)]
mod tests {
    use interface::price_fetcher::TokenPriceFetcher;
    use nft_ingester::price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater, SOLANA_CURRENCY};
    use nft_ingester::raydium_price_fetcher::RaydiumTokenPriceFetcher;
    use solana_program::pubkey::Pubkey;
    use std::str::FromStr;
    use testcontainers::clients::Cli;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_price_fetch() {
        let cnt = 0;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let solana_price_updater = SolanaPriceUpdater::new(
            env.rocks_env.storage.clone(),
            CoinGeckoPriceFetcher::new(),
            30,
        );
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_token_price_fetch() {
        let token_price_fetcher = RaydiumTokenPriceFetcher::default();
        let token_rocket =
            Pubkey::from_str("J4YFAQRJg9Us4wiGBewHuHxHG7bjDgZ2mRG7z4HT84Nm").unwrap();
        let token_z = Pubkey::from_str("Hqc9MLy4ebx2z6PNgS1w7PiU3umYdciQZfno6wR33iqy").unwrap();
        let token_pie = Pubkey::from_str("HmwZ7aL6GtQZBAd7w9mSJMVy5fRU9ra57c6bnuPCinvD").unwrap();
        let non_existed_token = Pubkey::new_unique();
        let tokens = &[token_rocket, token_z, token_pie, non_existed_token];
        let prices = token_price_fetcher
            .fetch_token_prices(tokens)
            .await
            .unwrap();
        let symbols = token_price_fetcher
            .fetch_token_symbols(tokens)
            .await
            .unwrap();

        assert_eq!(symbols.get(&token_rocket.to_string()).unwrap(), "ROCKET");
        assert_eq!(symbols.get(&token_z.to_string()).unwrap(), "Z");
        assert_eq!(symbols.get(&token_pie.to_string()).unwrap(), "$PIE");
        assert!(symbols.get(&non_existed_token.to_string()).is_none());

        assert!(prices.get(&token_rocket.to_string()).unwrap().clone() > 0.0);
        assert!(prices.get(&token_z.to_string()).unwrap().clone() > 0.0);
        assert!(prices.get(&token_pie.to_string()).unwrap().clone() > 0.0);
        assert!(prices.get(&non_existed_token.to_string()).is_none());
    }
}
