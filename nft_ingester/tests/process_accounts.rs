#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use blockbuster::mpl_core::types::{
        AppData, ExternalPluginAdapter, ExternalPluginAdapterSchema, ExternalPluginAdapterType,
        FreezeDelegate, Plugin, PluginAuthority, PluginType, TransferDelegate, UpdateAuthority,
    };
    use blockbuster::mpl_core::{
        IndexableAsset, IndexableCheckResult, IndexableExternalPluginSchemaV1,
        IndexablePluginSchemaV1, LifecycleChecks,
    };
    use blockbuster::programs::mpl_core_program::MplCoreAccountData;
    use blockbuster::token_metadata::accounts::Metadata;
    use blockbuster::token_metadata::types::{Key, TokenStandard};
    use entities::models::{EditionV1, MasterEdition};
    use metrics_utils::IngesterMetricsConfig;
    use nft_ingester::buffer::Buffer;
    use nft_ingester::mpl_core_processor::MplCoreProcessor;
    use nft_ingester::mplx_updates_processor::{
        IndexableAssetWithAccountInfo, MetadataInfo, MplxAccsProcessor, TokenMetadata,
    };
    use nft_ingester::token_updates_processor::TokenAccsProcessor;
    use rocks_db::columns::{Mint, TokenAccount};
    use rocks_db::editions::TokenMetadataEdition;
    use rocks_db::AssetAuthority;
    use solana_program::pubkey::Pubkey;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use base64::Engine;
    use base64::engine::general_purpose;
    use testcontainers::clients::Cli;
    use nft_ingester::inscriptions_processor::InscriptionsProcessor;
    use nft_ingester::message_handler::MessageHandlerIngester;
    use nft_ingester::plerkle;

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
            rent_epoch: 0,
            executable: false,
            metadata_owner: None,
        }
    }

    #[tokio::test]
    async fn token_update_process() {
        use std::collections::HashMap;

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
        let mplx_accs_parser = MplxAccsProcessor::new(
            1,
            buffer.clone(),
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

    #[tokio::test]
    async fn mpl_core_update_process() {
        let first_mpl_core = Pubkey::new_unique();
        let first_owner = Pubkey::new_unique();
        let first_authority = Pubkey::new_unique();
        let first_core_name = "first_core_name";
        let first_uri = "first_uri";
        let second_mpl_core = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();
        let second_authority = Pubkey::new_unique();
        let second_core_name = "second_core_name";
        let second_uri = "second_uri";
        let mut first_plugins = HashMap::new();
        first_plugins.insert(
            PluginType::FreezeDelegate,
            IndexablePluginSchemaV1 {
                index: 0,
                offset: 165,
                authority: PluginAuthority::UpdateAuthority,
                data: Plugin::FreezeDelegate(FreezeDelegate { frozen: true }),
            },
        );
        let mut second_plugins = HashMap::new();
        second_plugins.insert(
            PluginType::TransferDelegate,
            IndexablePluginSchemaV1 {
                index: 0,
                offset: 165,
                authority: PluginAuthority::UpdateAuthority,
                data: Plugin::TransferDelegate(TransferDelegate {}),
            },
        );

        let first_mpl_core_to_save = IndexableAssetWithAccountInfo {
            indexable_asset: MplCoreAccountData::Asset(IndexableAsset {
                owner: Some(first_owner),
                update_authority: UpdateAuthority::Address(first_authority),
                name: first_core_name.to_string(),
                uri: first_uri.to_string(),
                seq: 0,
                num_minted: None,
                current_size: None,
                plugins: first_plugins,
                unknown_plugins: vec![],
                external_plugins: vec![],
                unknown_external_plugins: vec![],
            }),
            lamports: 1,
            executable: false,
            slot_updated: 1,
            write_version: 1,
            rent_epoch: 0,
        };
        let second_mpl_core_to_save = IndexableAssetWithAccountInfo {
            indexable_asset: MplCoreAccountData::Collection(IndexableAsset {
                owner: Some(second_owner),
                update_authority: UpdateAuthority::Collection(second_authority),
                name: second_core_name.to_string(),
                uri: second_uri.to_string(),
                seq: 0,
                num_minted: None,
                current_size: None,
                plugins: second_plugins,
                unknown_plugins: vec![],
                external_plugins: vec![IndexableExternalPluginSchemaV1 {
                    index: 0,
                    offset: 0,
                    authority: PluginAuthority::UpdateAuthority,
                    lifecycle_checks: Some(LifecycleChecks {
                        create: vec![IndexableCheckResult::CanApprove],
                        update: vec![IndexableCheckResult::CanListen],
                        transfer: vec![IndexableCheckResult::CanReject],
                        burn: vec![
                            IndexableCheckResult::CanReject,
                            IndexableCheckResult::CanApprove,
                        ],
                    }),
                    unknown_lifecycle_checks: None,
                    r#type: ExternalPluginAdapterType::LifecycleHook,
                    adapter_config: ExternalPluginAdapter::AppData(AppData {
                        data_authority: PluginAuthority::Owner,
                        schema: ExternalPluginAdapterSchema::Binary,
                    }),
                    data_offset: Some(254),
                    data_len: Some(500),
                    data: Some("asdfasdf".to_string()),
                }],
                unknown_external_plugins: vec![],
            }),
            lamports: 1,
            executable: false,
            slot_updated: 1,
            write_version: 1,
            rent_epoch: 0,
        };

        let buffer = Arc::new(Buffer::new());
        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let mpl_core_parser = MplCoreProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        env.rocks_env
            .storage
            .asset_authority_data
            .put(
                second_authority,
                AssetAuthority {
                    pubkey: Default::default(),
                    authority: second_owner,
                    slot_updated: 0,
                    write_version: None,
                },
            )
            .unwrap();
        let mut indexable_assets = HashMap::new();
        indexable_assets.insert(first_mpl_core, first_mpl_core_to_save);
        indexable_assets.insert(second_mpl_core, second_mpl_core_to_save);
        mpl_core_parser
            .transform_and_store_mpl_assets(&indexable_assets)
            .await;

        let first_dynamic_from_db = env
            .rocks_env
            .storage
            .asset_dynamic_data
            .get(first_mpl_core)
            .unwrap()
            .unwrap();
        let first_owner_from_db = env
            .rocks_env
            .storage
            .asset_owner_data
            .get(first_mpl_core)
            .unwrap()
            .unwrap();
        let first_authority_from_db = env
            .rocks_env
            .storage
            .asset_authority_data
            .get(first_mpl_core)
            .unwrap()
            .unwrap();
        assert_eq!(first_dynamic_from_db.pubkey, first_mpl_core);
        assert_eq!(first_dynamic_from_db.is_frozen.value, true);
        assert_eq!(first_dynamic_from_db.url.value, first_uri.to_string());
        assert_eq!(
            first_dynamic_from_db.raw_name.unwrap().value,
            first_core_name.to_string()
        );
        assert_eq!(first_owner_from_db.owner.value.unwrap(), first_owner);
        assert_eq!(first_authority_from_db.authority, first_authority);

        let second_dynamic_from_db = env
            .rocks_env
            .storage
            .asset_dynamic_data
            .get(second_mpl_core)
            .unwrap()
            .unwrap();
        let second_owner_from_db = env
            .rocks_env
            .storage
            .asset_owner_data
            .get(second_mpl_core)
            .unwrap()
            .unwrap();
        let second_authority_from_db = env
            .rocks_env
            .storage
            .asset_authority_data
            .get(second_mpl_core)
            .unwrap()
            .unwrap();
        assert_eq!(second_dynamic_from_db.pubkey, second_mpl_core);
        assert_eq!(second_dynamic_from_db.is_frozen.value, false);
        assert_eq!(second_dynamic_from_db.url.value, second_uri.to_string());
        assert_eq!(
            second_dynamic_from_db.raw_name.unwrap().value,
            second_core_name.to_string()
        );
        assert_eq!(second_owner_from_db.owner.value.unwrap(), second_owner);
        assert_eq!(second_owner_from_db.delegate.value.unwrap(), second_owner);
        assert_eq!(second_authority_from_db.authority, second_owner);
        assert_eq!(
            second_dynamic_from_db
                .mpl_core_external_plugins
                .unwrap()
                .value,
            "[{\"data_len\":500,\"index\":0,\"offset\":0,\"authority\":{\"type\":\"UpdateAuthority\",\"address\":null},\"lifecycle_checks\":{\"transfer\":[\"CanReject\"],\"create\":[\"CanApprove\"],\"update\":[\"CanListen\"],\"burn\":[\"CanReject\",\"CanApprove\"]},\"type\":\"LifecycleHook\",\"adapter_config\":{\"data_authority\":{\"type\":\"Owner\",\"address\":null},\"schema\":\"Binary\"},\"data_offset\":254,\"data\":\"asdfasdf\"}]".to_string()
        );
    }

    #[tokio::test]
    async fn inscription_process() {
        let inscription_account_data = general_purpose::STANDARD_NO_PAD.decode("ZAuXKuQmRbsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAF00JG/taM5xDErn+0mQMBbbBdJuhYeh30FuRLrqWSbfBhAAAABhcHBsaWNhdGlvbi90ZXh0AeOkcaHjppsua2rgJHv2TUkEEClH4Y96jMvvKr1caFZzE7QEAAAAAABDAAAAAUAAAABmNTMyMGVmMjhkNTM3NWQ3YjFhNmFlNzBlYzQzZWRkMTE1ZmQxMmVhOTMzZTAxNjUzMDZhNzg4ZGNiZWVjYTMxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap() ;
        let inscription_data_account_data = general_purpose::STANDARD_NO_PAD.decode("eyJwIjoic3BsLTIwIiwib3AiOiJkZXBsb3kiLCJ0aWNrIjoiaGVsaXVzIiwibWF4IjoiMTAwMCIsImxpbSI6IjEifQ==").unwrap() ;
        let inscription_pk = Pubkey::from_str("F5PvEHfUiSVuNKFdJbDo7iMHWp9x1aDBbvkt1DTGVQcJ").unwrap();
        let buffer = Arc::new(Buffer::new());
        let message_handler = MessageHandlerIngester::new(buffer.clone());
        message_handler.handle_inscription_account(&plerkle::AccountInfo {
            slot: 100,
            pubkey: Pubkey::from_str("F5PvEHfUiSVuNKFdJbDo7iMHWp9x1aDBbvkt1DTGVQcJ").unwrap(),
            owner: libreplex_inscriptions::id(),
            lamports: 1000,
            rent_epoch: 100,
            executable: false,
            write_version: 100,
            data: inscription_account_data,
        }).await;
        message_handler.handle_inscription_account(&plerkle::AccountInfo {
            slot: 100,
            pubkey: Pubkey::from_str("GKcyym3BLXizQJWfH8H3YA5wLJTYVHrNMoGzbCsHtogn").unwrap(),
            owner: libreplex_inscriptions::id(),
            lamports: 1000,
            rent_epoch: 100,
            executable: false,
            write_version: 100,
            data: inscription_data_account_data,
        }).await;
        let asset_pk = buffer.inscriptions.lock().await.get()

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let mpl_core_parser = InscriptionsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        env.rocks_env
            .storage
            .asset_authority_data
            .put(
                second_authority,
                AssetAuthority {
                    pubkey: Default::default(),
                    authority: second_owner,
                    slot_updated: 0,
                    write_version: None,
                },
            )
            .unwrap();
        let mut indexable_assets = HashMap::new();
        indexable_assets.insert(first_mpl_core, first_mpl_core_to_save);
        indexable_assets.insert(second_mpl_core, second_mpl_core_to_save);
        mpl_core_parser
            .transform_and_store_mpl_assets(&indexable_assets)
            .await;

    }
}
