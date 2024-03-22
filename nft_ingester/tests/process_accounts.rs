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
        use std::collections::HashMap;

        use solana_sdk::blake3::Hash;

        let first_mint = Pubkey::new_unique();
        let second_mint = Pubkey::new_unique();
        let first_token_account = Pubkey::new_unique();
        let second_token_account = Pubkey::new_unique();
        let first_owner = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();

        let first_mint_to_save = Mint {
            pubkey: first_mint,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
            write_version: 1,
        };
        let second_mint_to_save = Mint {
            pubkey: second_mint,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
            write_version: 1,
        };
        let first_token_account_to_save = TokenAccount {
            pubkey: first_token_account,
            mint: first_mint,
            delegate: None,
            owner: first_owner,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 1,
            write_version: 1,
        };
        let second_token_account_to_save = TokenAccount {
            pubkey: second_token_account,
            mint: second_mint,
            delegate: None,
            owner: second_owner,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 1,
            write_version: 1,
        };

        let buffer = Arc::new(Buffer::new());
        let mut token_accs = HashMap::new();
        let mut mints = HashMap::new();
        mints.insert(first_mint.to_bytes().to_vec(), first_mint_to_save);
        mints.insert(second_mint.to_bytes().to_vec(), second_mint_to_save);
        token_accs.insert(first_token_account, first_token_account_to_save);
        token_accs.insert(second_token_account, second_token_account_to_save);
        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let spl_token_accs_parser = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        spl_token_accs_parser
            .transform_and_save_token_accs(&token_accs)
            .await;
        spl_token_accs_parser
            .transform_and_save_mint_accs(&mints)
            .await;

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
        use std::collections::HashMap;

        let first_mint = Pubkey::new_unique();
        let second_mint = Pubkey::new_unique();
        let first_edition = Pubkey::new_unique();
        let second_edition = Pubkey::new_unique();
        let parent = Pubkey::new_unique();
        let supply = 12345;

        let first_metadata_to_save = generate_metadata(first_mint);
        let second_metadata_to_save = generate_metadata(second_mint);
        let first_edition_to_save = TokenMetadata {
            edition: TokenMetadataEdition::EditionV1 {
                0: EditionV1 {
                    key: Default::default(),
                    parent,
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
                    supply,
                    max_supply: None,
                    write_version: 1,
                },
            },
            write_version: 1,
            slot_updated: 1,
        };

        let buffer = Arc::new(Buffer::new());
        let mut mplx_metadata_info = HashMap::new();
        let mut token_metadata_editions = HashMap::new();
        mplx_metadata_info.insert(first_mint.to_bytes().to_vec(), first_metadata_to_save);
        mplx_metadata_info.insert(second_mint.to_bytes().to_vec(), second_metadata_to_save);
        token_metadata_editions.insert(first_edition, first_edition_to_save.edition);
        token_metadata_editions.insert(second_edition, second_edition_to_save.edition);
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

        mplx_accs_parser
            .transform_and_store_metadata_accs(&mplx_metadata_info)
            .await;
        mplx_accs_parser
            .transform_and_store_edition_accs(&token_metadata_editions)
            .await;

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
        if let TokenMetadataEdition::EditionV1(edition) = first_edition_from_db {
            assert_eq!(edition.parent, parent);
        } else {
            panic!("expected EditionV1 enum variant");
        };
        if let TokenMetadataEdition::MasterEdition(edition) = second_edition_from_db {
            assert_eq!(edition.supply, supply);
        } else {
            panic!("expected MasterEdition enum variant");
        };
    }
}
