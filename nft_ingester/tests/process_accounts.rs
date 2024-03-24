#[cfg(test)]
//#[cfg(feature = "integration_tests")]
mod tests {
    use blockbuster::mpl_core::types::{
        CompressionProof, HashablePluginSchema, Plugin, UpdateAuthority,
    };
    use blockbuster::programs::mpl_core_program::MplCoreAccountData;
    use blockbuster::token_metadata::accounts::Metadata;
    use blockbuster::token_metadata::types::{Key, TokenStandard};
    use entities::models::{EditionV1, MasterEdition};
    use metrics_utils::IngesterMetricsConfig;
    use nft_ingester::buffer::Buffer;
    use nft_ingester::mpl_core_processor::MplCoreProcessor;
    use nft_ingester::mplx_updates_processor::{
        IndexableAssetWithWriteVersion, MetadataInfo, MplxAccsProcessor, TokenMetadata,
    };
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
                name: "".to_string(),
                symbol: "".to_string(),
                uri: "".to_string(),
                seller_fee_basis_points: 0,
                primary_sale_happened: false,
                is_mutable: true,
                edition_nonce: None,
                token_standard: Some(TokenStandard::Fungible),
                collection: None,
                uses: None,
                collection_details: None,
                programmable_config: None,
                creators: None,
            },
            slot_updated: 1,
            write_version: 1,
            lamports: 1,
            executable: false,
            metadata_owner: None,
        }
    }

    #[tokio::test]
    async fn token_update_process() {
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
        buffer
            .token_accs
            .lock()
            .await
            .insert(first_token_account, first_token_account_to_save);
        buffer
            .token_accs
            .lock()
            .await
            .insert(second_token_account, second_token_account_to_save);
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
        assert_eq!(first_owner_from_db.owner.value.unwrap(), first_owner);
        assert_eq!(second_owner_from_db.owner.value.unwrap(), second_owner);

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

    #[tokio::test]
    async fn mplx_update_process() {
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
        let mplx_accs_parser = MplxAccsProcessor::new(
            1,
            buffer.clone(),
            env.pg_env.client.clone(),
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

    // #[tokio::test]
    // async fn mpl_core_update_process() {
    //     let first_mpl_core = Pubkey::new_unique();
    //     let first_owner = Pubkey::new_unique();
    //     let first_authority = Pubkey::new_unique();
    //     let first_core_name = "first_core_name";
    //     let first_uri = "first_uri";
    //     let second_mpl_core = Pubkey::new_unique();
    //     let second_owner = Pubkey::new_unique();
    //     let second_authority = Pubkey::new_unique();
    //     let second_core_name = "second_core_name";
    //     let second_uri = "second_uri";
    //
    //     let first_mpl_core_to_save = IndexableAssetWithWriteVersion {
    //         indexable_asset: MplCoreAccountData::FullAsset(CompressionProof {
    //             owner: first_owner,
    //             update_authority: UpdateAuthority::Address(first_authority),
    //             name: first_core_name.to_string(),
    //             uri: first_uri.to_string(),
    //             seq: 0,
    //             plugins: vec![HashablePluginSchema {
    //                 index: 0,
    //                 authority: Authority::UpdateAuthority,
    //                 plugin: Plugin::Freeze(Freeze { frozen: true }),
    //             }],
    //         }),
    //         lamports: 1,
    //         executable: false,
    //         slot_updated: 1,
    //         write_version: 1,
    //     };
    //     let second_mpl_core_to_save = IndexableAssetWithWriteVersion {
    //         indexable_asset: MplCoreAccountData::FullAsset(CompressionProof {
    //             owner: second_owner,
    //             update_authority: UpdateAuthority::Address(second_authority),
    //             name: second_core_name.to_string(),
    //             uri: second_uri.to_string(),
    //             seq: 0,
    //             plugins: vec![HashablePluginSchema {
    //                 index: 0,
    //                 authority: Authority::UpdateAuthority,
    //                 plugin: Plugin::Transfer(Transfer {}),
    //             }],
    //         }),
    //         lamports: 1,
    //         executable: false,
    //         slot_updated: 1,
    //         write_version: 1,
    //     };
    //
    //     let buffer = Arc::new(Buffer::new());
    //     buffer
    //         .mpl_core_compressed_proofs
    //         .lock()
    //         .await
    //         .insert(first_mpl_core, first_mpl_core_to_save);
    //     buffer
    //         .mpl_core_compressed_proofs
    //         .lock()
    //         .await
    //         .insert(second_mpl_core, second_mpl_core_to_save);
    //     let cnt = 20;
    //     let cli = Cli::default();
    //     let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
    //     let db_client = Arc::new(DBClient {
    //         pool: env.pg_env.pool.clone(),
    //     });
    //     let mpl_core_parser = MplCoreProcessor::new(
    //         env.rocks_env.storage.clone(),
    //         db_client.clone(),
    //         buffer.clone(),
    //         Arc::new(IngesterMetricsConfig::new()),
    //         1,
    //     );
    //
    //     let mut mpl_core_parser_clone = mpl_core_parser.clone();
    //     let keep_running = Arc::new(AtomicBool::new(true));
    //     let keep_running_clone = keep_running.clone();
    //     tokio::spawn(async move {
    //         mpl_core_parser_clone
    //             .process_mpl_assets(keep_running_clone)
    //             .await
    //     });
    //     tokio::time::sleep(Duration::from_secs(5)).await;
    //
    //     let first_dynamic_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_dynamic_data
    //         .get(first_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     let first_owner_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_owner_data
    //         .get(first_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     let first_authority_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_authority_data
    //         .get(first_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     assert_eq!(first_dynamic_from_db.pubkey, first_mpl_core);
    //     assert_eq!(
    //         first_dynamic_from_db.freeze_delegate.unwrap().value,
    //         first_authority
    //     );
    //     assert_eq!(first_dynamic_from_db.is_frozen.value, true);
    //     assert_eq!(first_dynamic_from_db.url.value, first_uri.to_string());
    //     assert_eq!(
    //         first_dynamic_from_db.raw_name.unwrap().value,
    //         first_core_name.to_string()
    //     );
    //     assert_eq!(first_owner_from_db.owner.value.unwrap(), first_owner);
    //     assert_eq!(first_authority_from_db.authority, first_authority);
    //
    //     let second_dynamic_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_dynamic_data
    //         .get(second_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     let second_owner_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_owner_data
    //         .get(second_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     let second_authority_from_db = env
    //         .rocks_env
    //         .storage
    //         .asset_authority_data
    //         .get(second_mpl_core)
    //         .unwrap()
    //         .unwrap();
    //     assert_eq!(second_dynamic_from_db.pubkey, second_mpl_core);
    //     assert_eq!(second_dynamic_from_db.freeze_delegate, None);
    //     assert_eq!(second_dynamic_from_db.is_frozen.value, false);
    //     assert_eq!(second_dynamic_from_db.url.value, second_uri.to_string());
    //     assert_eq!(
    //         second_dynamic_from_db.raw_name.unwrap().value,
    //         second_core_name.to_string()
    //     );
    //     assert_eq!(second_owner_from_db.owner.value.unwrap(), second_owner);
    //     assert_eq!(
    //         second_owner_from_db.delegate.value.unwrap(),
    //         second_authority
    //     );
    //     assert_eq!(second_authority_from_db.authority, second_authority);
    // }
}
