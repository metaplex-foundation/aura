#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use blockbuster::token_metadata::state::{Data, Key, Metadata, TokenStandard};
    use entities::models::{EditionV1, MasterEdition};
    use metrics_utils::IngesterMetricsConfig;
    use nft_ingester::buffer::Buffer;
    use nft_ingester::db_v2::DBClient;
    use nft_ingester::mplx_updates_processor::{MetadataInfo, MplxAccsProcessor, TokenMetadata};
    use nft_ingester::token_updates_processor::TokenAccsProcessor;
    use rocks_db::columns::{Mint, TokenAccount};
    use rocks_db::editions::TokenMetadataEdition;
    use solana_program::pubkey::Pubkey;
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;
    use testcontainers::clients::Cli;

    pub fn generate_metadata(mint_key: Pubkey) -> MetadataInfo {
        MetadataInfo {
            metadata: Metadata {
                key: Key::MetadataV1,
                update_authority: Pubkey::new_unique(),
                mint: mint_key,
                data: Data {
                    name: "name".to_string(),
                    symbol: "symbol".to_string(),
                    uri: "https://ping-pong".to_string(),
                    seller_fee_basis_points: 10,
                    creators: None,
                },
                primary_sale_happened: false,
                is_mutable: true,
                edition_nonce: None,
                token_standard: Some(TokenStandard::Fungible),
                collection: None,
                uses: None,
                collection_details: None,
                programmable_config: None,
            },
            slot_updated: 1,
            write_version: 1,
            lamports: 1,
            executable: false,
            metadata_owner: None,
        }
    }

    #[cfg(test)]
    #[tokio::test]
    async fn token_update_process() {
        let first_mint = Pubkey::from_str("5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61").unwrap();
        let second_mint = Pubkey::from_str("94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe").unwrap();
        let first_token_account =
            Pubkey::from_str("4X8qeyubc6tgd2SLb4bZ6SXoNE1wqUGw48r4mFY9qHq1").unwrap();
        let second_token_account =
            Pubkey::from_str("286WMWHqCa2Nbgn5Yw1fwfXrX9cMFWMJqQYQRDyCAJwh").unwrap();
        let first_owner = Pubkey::from_str("3JFC4cB56Er45nWVe29Bhnn5GnwQzSmHVf6eUq9ac91h").unwrap();
        let second_owner =
            Pubkey::from_str("C2rQTDPik9byUGt4ALM1c1hVrRzJwQGxvrsAWfi3v2yY").unwrap();

        let first_mint_to_save = Mint {
            pubkey: first_mint,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
        };
        let second_mint_to_save = Mint {
            pubkey: second_mint,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
        };
        let first_token_account_to_save = TokenAccount {
            pubkey: first_token_account,
            mint: first_mint,
            delegate: None,
            owner: first_owner,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 0,
        };
        let second_token_account_to_save = TokenAccount {
            pubkey: second_token_account,
            mint: second_mint,
            delegate: None,
            owner: second_owner,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 0,
        };

        let buffer = Arc::new(Buffer::new());
        buffer
            .mints
            .lock()
            .await
            .insert(first_mint.to_bytes().to_vec(), first_mint_to_save);
        buffer
            .mints
            .lock()
            .await
            .insert(second_mint.to_bytes().to_vec(), second_mint_to_save);
        buffer.token_accs.lock().await.insert(
            first_token_account.to_bytes().to_vec(),
            first_token_account_to_save,
        );
        buffer.token_accs.lock().await.insert(
            second_token_account.to_bytes().to_vec(),
            second_token_account_to_save,
        );
        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let spl_token_accs_parser = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mut spl_token_accs_parser_clone = spl_token_accs_parser.clone();
        let keep_running = Arc::new(AtomicBool::new(true));
        let keep_running_clone = keep_running.clone();
        tokio::spawn(async move {
            spl_token_accs_parser_clone
                .process_token_accs(keep_running_clone)
                .await
        });

        let mut spl_token_accs_parser_clone = spl_token_accs_parser.clone();
        let keep_running_clone = keep_running.clone();
        tokio::spawn(async move {
            spl_token_accs_parser_clone
                .process_mint_accs(keep_running_clone)
                .await
        });
        tokio::time::sleep(Duration::from_secs(10)).await;

        let first_owner_from_db = env
            .rocks_env
            .storage
            .asset_owner_data
            .get(first_mint)
            .unwrap()
            .unwrap();
        let second_owner_from_db = env
            .rocks_env
            .storage
            .asset_owner_data
            .get(second_mint)
            .unwrap()
            .unwrap();
        assert_eq!(first_owner_from_db.owner.value, first_owner);
        assert_eq!(second_owner_from_db.owner.value, second_owner);

        let first_dynamic_from_db = env
            .rocks_env
            .storage
            .asset_dynamic_data
            .get(first_mint)
            .unwrap()
            .unwrap();
        let second_dynamic_from_db = env
            .rocks_env
            .storage
            .asset_dynamic_data
            .get(second_mint)
            .unwrap()
            .unwrap();
        assert_eq!(first_dynamic_from_db.supply.unwrap().value, 1);
        assert_eq!(second_dynamic_from_db.supply.unwrap().value, 1);
    }

    #[cfg(test)]
    #[tokio::test]
    async fn mplx_update_process() {
        let first_mint = Pubkey::from_str("5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61").unwrap();
        let second_mint = Pubkey::from_str("94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe").unwrap();
        let first_edition =
            Pubkey::from_str("4X8qeyubc6tgd2SLb4bZ6SXoNE1wqUGw48r4mFY9qHq1").unwrap();
        let second_edition =
            Pubkey::from_str("286WMWHqCa2Nbgn5Yw1fwfXrX9cMFWMJqQYQRDyCAJwh").unwrap();

        let first_metadata_to_save = generate_metadata(first_mint);
        let second_metadata_to_save = generate_metadata(second_mint);
        let first_edition_to_save = TokenMetadata {
            edition: TokenMetadataEdition::EditionV1 {
                0: EditionV1 {
                    key: Default::default(),
                    parent: Default::default(),
                    edition: 0,
                    write_version: 1,
                },
            },
            write_version: 1,
            slot_updated: 1,
        };
        let second_edition_to_save = TokenMetadata {
            edition: TokenMetadataEdition::MasterEdition {
                0: MasterEdition {
                    key: Default::default(),
                    supply: 0,
                    max_supply: None,
                    write_version: 1,
                },
            },
            write_version: 1,
            slot_updated: 1,
        };

        let buffer = Arc::new(Buffer::new());
        buffer
            .mplx_metadata_info
            .lock()
            .await
            .insert(first_mint.to_bytes().to_vec(), first_metadata_to_save);
        buffer
            .mplx_metadata_info
            .lock()
            .await
            .insert(second_mint.to_bytes().to_vec(), second_metadata_to_save);
        buffer
            .token_metadata_editions
            .lock()
            .await
            .insert(first_edition, first_edition_to_save);
        buffer
            .token_metadata_editions
            .lock()
            .await
            .insert(second_edition, second_edition_to_save);
        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let db_client = Arc::new(DBClient {
            pool: env.pg_env.pool.clone(),
        });
        let mplx_accs_parser = MplxAccsProcessor::new(
            1,
            buffer.clone(),
            db_client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        );

        let mut mplx_accs_parser_clone = mplx_accs_parser.clone();
        let keep_running = Arc::new(AtomicBool::new(true));
        let keep_running_clone = keep_running.clone();
        tokio::spawn(async move {
            mplx_accs_parser_clone
                .process_metadata_accs(keep_running_clone)
                .await
        });

        let mut mplx_accs_parser_clone = mplx_accs_parser.clone();
        let keep_running_clone = keep_running.clone();
        tokio::spawn(async move {
            mplx_accs_parser_clone
                .process_edition_accs(keep_running_clone)
                .await
        });
        tokio::time::sleep(Duration::from_secs(10)).await;

        let first_static_from_db = env
            .rocks_env
            .storage
            .asset_static_data
            .get(first_mint)
            .unwrap()
            .unwrap();
        let second_static_from_db = env
            .rocks_env
            .storage
            .asset_static_data
            .get(second_mint)
            .unwrap()
            .unwrap();
        assert_eq!(first_static_from_db.pubkey, first_mint);
        assert_eq!(second_static_from_db.pubkey, second_mint);

        let first_edition_from_db = env
            .rocks_env
            .storage
            .token_metadata_edition_cbor
            .get_cbor_encoded(first_edition)
            .await
            .unwrap()
            .unwrap();
        let second_edition_from_db = env
            .rocks_env
            .storage
            .token_metadata_edition_cbor
            .get_cbor_encoded(second_edition)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first_edition_from_db,
            TokenMetadataEdition::EditionV1 {
                0: EditionV1 {
                    key: Default::default(),
                    parent: Default::default(),
                    edition: 0,
                    write_version: 1,
                },
            }
        );
        assert_eq!(
            second_edition_from_db,
            TokenMetadataEdition::MasterEdition {
                0: MasterEdition {
                    key: Default::default(),
                    supply: 0,
                    max_supply: None,
                    write_version: 1,
                },
            }
        );
    }
}
