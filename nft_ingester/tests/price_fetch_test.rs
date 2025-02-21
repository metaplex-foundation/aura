#[cfg(test)]
mod tests {
    use interface::price_fetcher::TokenPriceFetcher;
    use nft_ingester::{
        price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater, SOLANA_CURRENCY},
        raydium_price_fetcher::RaydiumTokenPriceFetcher,
    };
    use solana_program::pubkey::Pubkey;
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

        let price =
            env.rocks_env.storage.token_prices.get(SOLANA_CURRENCY.to_string()).unwrap().unwrap();
        assert!(price.price > 0.0)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_token_price_fetch() {
        let token_price_fetcher = RaydiumTokenPriceFetcher::default();
        let token_rocket = "J4YFAQRJg9Us4wiGBewHuHxHG7bjDgZ2mRG7z4HT84Nm".to_owned();
        let token_z = "Hqc9MLy4ebx2z6PNgS1w7PiU3umYdciQZfno6wR33iqy".to_owned();
        let token_pie = "HmwZ7aL6GtQZBAd7w9mSJMVy5fRU9ra57c6bnuPCinvD".to_owned();
        let non_existed_token = Pubkey::new_unique().to_string();
        let tokens =
            &[token_rocket.clone(), token_z.clone(), token_pie.clone(), non_existed_token.clone()];
        let prices = token_price_fetcher.fetch_token_prices(tokens).await.unwrap();
        let symbols = token_price_fetcher.fetch_token_symbols(tokens).await.unwrap();

        assert_eq!(symbols.get(&token_rocket).unwrap(), "ROCKET");
        assert_eq!(symbols.get(&token_z).unwrap(), "Z");
        assert_eq!(symbols.get(&token_pie).unwrap(), "$PIE");
        assert!(symbols.get(&non_existed_token).is_none());

        assert!(prices.get(&token_rocket).unwrap().clone() > 0.0);
        assert!(prices.get(&token_z).unwrap().clone() > 0.0);
        assert!(prices.get(&token_pie).unwrap().clone() > 0.0);
        assert!(prices.get(&non_existed_token).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_token_price_fetcher_warmup() {
        let token_price_fetcher = RaydiumTokenPriceFetcher::default();
        token_price_fetcher.warmup().await.expect("warmup must succeed");

        // check that the cache was pre-filled by some token symbols
        assert!(token_price_fetcher.get_cache_sizes().0 > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_token_price_get_list_of_tokens() {
        let token_price_fetcher = RaydiumTokenPriceFetcher::default();
        token_price_fetcher.warmup().await.expect("warmup must succeed");

        pub const USDC_MINT_BYTES: [u8; 32] = [
            198, 250, 122, 243, 190, 219, 173, 58, 61, 101, 243, 106, 171, 201, 116, 49, 177, 187,
            228, 194, 210, 246, 224, 228, 124, 166, 2, 3, 69, 47, 93, 97,
        ];
        let usdc_pk = Pubkey::new_from_array(USDC_MINT_BYTES);

        assert!(token_price_fetcher.get_all_token_symbols().await.unwrap().len() > 0);
        assert!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".eq(&usdc_pk.to_string()));
        assert!(token_price_fetcher
            .get_all_token_symbols()
            .await
            .unwrap()
            .contains_key("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"));
        assert!(token_price_fetcher
            .get_all_token_symbols()
            .await
            .unwrap()
            .contains_key(&usdc_pk.to_string()));
    }
}
