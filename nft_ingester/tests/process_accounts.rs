#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use base64::engine::general_purpose;
    use base64::Engine;
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
    use entities::enums::{TokenMetadataEdition, UnprocessedAccount};
    use entities::models::{
        EditionMetadata, EditionV1, IndexableAssetWithAccountInfo, MasterEdition, MetadataInfo,
        Mint, TokenAccount,
    };
    use metrics_utils::IngesterMetricsConfig;
    use nft_ingester::buffer::Buffer;
    use nft_ingester::inscriptions_processor::InscriptionsProcessor;
    use nft_ingester::message_parser::MessageParser;
    use nft_ingester::mpl_core_processor::MplCoreProcessor;
    use nft_ingester::mplx_updates_processor::MplxAccountsProcessor;
    use nft_ingester::plerkle;
    use nft_ingester::token_updates_processor::TokenAccountsProcessor;
    use rocks_db::batch_savers::BatchSaveStorage;
    use rocks_db::AssetAuthority;
    use solana_program::pubkey::Pubkey;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
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
            rent_epoch: 0,
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
            token_program: Default::default(),
            extensions: None,
            write_version: 1,
        };
        let second_mint_to_save = Mint {
            pubkey: second_mint,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            extensions: None,
            write_version: 1,
        };
        let first_token_account_to_save = TokenAccount {
            pubkey: first_token_account,
            mint: first_mint,
            delegate: None,
            owner: first_owner,
            extensions: None,
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
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 1,
            write_version: 1,
        };

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let spl_token_accs_parser =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        spl_token_accs_parser
            .transform_and_save_token_account(
                &mut batch_storage,
                first_token_account,
                &first_token_account_to_save,
            )
            .unwrap();
        spl_token_accs_parser
            .transform_and_save_token_account(
                &mut batch_storage,
                second_token_account,
                &second_token_account_to_save,
            )
            .unwrap();
        spl_token_accs_parser
            .transform_and_save_mint_account(&mut batch_storage, &first_mint_to_save)
            .unwrap();
        spl_token_accs_parser
            .transform_and_save_mint_account(&mut batch_storage, &second_mint_to_save)
            .unwrap();
        batch_storage.flush().unwrap();

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
        let first_edition_to_save = EditionMetadata {
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
        let second_edition_to_save = EditionMetadata {
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

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let mplx_accs_parser = MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        mplx_accs_parser
            .transform_and_store_metadata_account(
                &mut batch_storage,
                first_mint,
                &first_metadata_to_save,
            )
            .unwrap();
        mplx_accs_parser
            .transform_and_store_metadata_account(
                &mut batch_storage,
                second_mint,
                &second_metadata_to_save,
            )
            .unwrap();
        mplx_accs_parser
            .transform_and_store_edition_account(
                &mut batch_storage,
                first_edition,
                &first_edition_to_save.edition,
            )
            .unwrap();
        mplx_accs_parser
            .transform_and_store_edition_account(
                &mut batch_storage,
                second_edition,
                &second_edition_to_save.edition,
            )
            .unwrap();
        batch_storage.flush().unwrap();

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

        let cnt = 20;
        let cli = Cli::default();
        let (env, _generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let mpl_core_parser = MplCoreProcessor::new(Arc::new(IngesterMetricsConfig::new()));
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

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        mpl_core_parser
            .transform_and_store_mpl_asset(
                &mut batch_storage,
                first_mpl_core,
                &first_mpl_core_to_save,
            )
            .unwrap();
        mpl_core_parser
            .transform_and_store_mpl_asset(
                &mut batch_storage,
                second_mpl_core,
                &second_mpl_core_to_save,
            )
            .unwrap();
        batch_storage.flush().unwrap();

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
        // real world accounts
        let inscription_account_data = general_purpose::STANDARD_NO_PAD.decode("ZAuXKuQmRbsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAF00JG/taM5xDErn+0mQMBbbBdJuhYeh30FuRLrqWSbfBhAAAABhcHBsaWNhdGlvbi90ZXh0AeOkcaHjppsua2rgJHv2TUkEEClH4Y96jMvvKr1caFZzE7QEAAAAAABDAAAAAUAAAABmNTMyMGVmMjhkNTM3NWQ3YjFhNmFlNzBlYzQzZWRkMTE1ZmQxMmVhOTMzZTAxNjUzMDZhNzg4ZGNiZWVjYTMxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap() ;
        let inscription_data_account_data = general_purpose::STANDARD.decode("eyJwIjoic3BsLTIwIiwib3AiOiJkZXBsb3kiLCJ0aWNrIjoiaGVsaXVzIiwibWF4IjoiMTAwMCIsImxpbSI6IjEifQ==").unwrap();
        let inscription_pk =
            Pubkey::from_str("F5PvEHfUiSVuNKFdJbDo7iMHWp9x1aDBbvkt1DTGVQcJ").unwrap();
        let inscription_data_pk =
            Pubkey::from_str("GKcyym3BLXizQJWfH8H3YA5wLJTYVHrNMoGzbCsHtogn").unwrap();
        let buffer = Arc::new(Buffer::new());
        let message_parser = MessageParser::new();

        let parsed_inscription_account = message_parser
            .handle_inscription_account(&plerkle::AccountInfo {
                slot: 100,
                pubkey: inscription_pk,
                owner: libreplex_inscriptions::id(),
                lamports: 1000,
                rent_epoch: 100,
                executable: false,
                write_version: 100,
                data: inscription_account_data,
            })
            .unwrap();
        let parsed_inscription_account_data = message_parser
            .handle_inscription_account(&plerkle::AccountInfo {
                slot: 100,
                pubkey: inscription_data_pk,
                owner: libreplex_inscriptions::id(),
                lamports: 1000,
                rent_epoch: 100,
                executable: false,
                write_version: 100,
                data: inscription_data_account_data,
            })
            .unwrap();
        buffer
            .accounts
            .lock()
            .await
            .insert(inscription_pk, parsed_inscription_account);
        buffer
            .accounts
            .lock()
            .await
            .insert(inscription_data_pk, parsed_inscription_account_data);

        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let asset_pk = generated_assets.static_details.first().unwrap().pubkey;

        match buffer
            .accounts
            .lock()
            .await
            .get_mut(&inscription_pk)
            .unwrap()
        {
            UnprocessedAccount::Inscription(i) => {
                i.inscription.root = asset_pk;
            }
            _ => panic!("Invalid account type"),
        };

        let mpl_core_parser = InscriptionsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        match buffer.accounts.lock().await.get(&inscription_pk).unwrap() {
            UnprocessedAccount::Inscription(inscription) => {
                mpl_core_parser
                    .store_inscription(&mut batch_storage, inscription)
                    .unwrap();
            }
            _ => panic!("Invalid account type"),
        };
        match buffer
            .accounts
            .lock()
            .await
            .get(&inscription_data_pk)
            .unwrap()
        {
            UnprocessedAccount::InscriptionData(inscription_data) => {
                mpl_core_parser
                    .store_inscription_data(
                        &mut batch_storage,
                        inscription_data_pk,
                        inscription_data,
                    )
                    .unwrap();
            }
            _ => panic!("Invalid account type"),
        };
        batch_storage.flush().unwrap();

        let inscription = env
            .rocks_env
            .storage
            .inscriptions
            .get(asset_pk)
            .unwrap()
            .unwrap();
        assert_eq!(inscription.inscription_data_account, inscription_data_pk);
        assert_eq!(inscription.root, asset_pk);
        assert_eq!(inscription.order, 308243);
        assert_eq!(inscription.size, 67);
        assert_eq!(inscription.content_type, "application/text");
        assert_eq!(inscription.encoding, "base64");
        assert_eq!(
            inscription.validation_hash.unwrap(),
            "f5320ef28d5375d7b1a6ae70ec43edd115fd12ea933e0165306a788dcbeeca31"
        );
        assert_eq!(inscription.authority, Pubkey::default());

        let inscription_data = env
            .rocks_env
            .storage
            .inscription_data
            .get(inscription.inscription_data_account)
            .unwrap()
            .unwrap();
        let inscription_data_json =
            serde_json::from_slice::<serde_json::Value>(inscription_data.data.as_slice()).unwrap();
        assert_eq!(
            inscription_data_json.get("p").unwrap().as_str().unwrap(),
            "spl-20"
        );
        assert_eq!(
            inscription_data_json.get("op").unwrap().as_str().unwrap(),
            "deploy"
        );
        assert_eq!(
            inscription_data_json.get("tick").unwrap().as_str().unwrap(),
            "helius"
        );
        assert_eq!(
            inscription_data_json.get("max").unwrap().as_str().unwrap(),
            "1000"
        );
        assert_eq!(
            inscription_data_json.get("lim").unwrap().as_str().unwrap(),
            "1"
        );
    }
}
