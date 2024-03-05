#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use blockbuster::token_metadata::state::{
        Data, Key, Metadata, TokenStandard as BLKTokenStandard,
    };
    use digital_asset_types::rpc::response::{AssetList, TokenAccountsList};
    use entities::api_req_params::{DisplayOptions, GetTokenAccounts};
    use entities::{
        api_req_params::{AssetSortBy, AssetSortDirection, AssetSorting, GetAsset, SearchAssets},
        enums::{
            ChainMutability, Interface, OwnerType, OwnershipModel, RoyaltyModel, RoyaltyTargetType,
            SpecificationAssetClass, TokenStandard,
        },
        models::{ChainDataV1, UpdateVersion, Updated},
    };
    use metrics_utils::{ApiMetricsConfig, IngesterMetricsConfig};
    use mpl_token_metadata::accounts::MasterEdition;
    use nft_ingester::{
        buffer::Buffer,
        db_v2::DBClient,
        mplx_updates_processor::{BurntMetadataSlot, MetadataInfo, MplxAccsProcessor},
        token_updates_processor::TokenAccsProcessor,
    };
    use rocks_db::{
        columns::{Mint, TokenAccount},
        offchain_data::OffChainData,
        AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
    };
    use serde_json::{json, Value};
    use solana_program::pubkey::Pubkey;
    use testcontainers::clients::Cli;
    use usecase::proofs::MaybeProofChecker;

    const SLOT_UPDATED: u64 = 100;
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_assets() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );
        let limit = 10;
        let before: Option<String>;
        let after: Option<String>;
        // test base case with only a limit and default (0) page
        {
            let payload = SearchAssets {
                limit: Some(limit),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            assert!(res.is_object());
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            before = res_obj.before;
            after = res_obj.after;
            assert_eq!(res_obj.total, limit, "total should match the limit");
            assert_eq!(
                res_obj.limit, limit,
                "limit should match the requested limit"
            );
            assert_eq!(
                res_obj.items.len(),
                limit as usize,
                "assets length should match the limit"
            );

            for i in 0..limit {
                assert_eq!(
                    res_obj.items[i as usize].id,
                    generated_assets.pubkeys[cnt - 1 - i as usize].to_string(),
                    "asset should match the pubkey"
                );
            }
        }
        // test limit and 1st page being the same as the 0
        {
            let payload = SearchAssets {
                limit: Some(limit),
                page: Some(1),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            for i in 0..limit {
                assert_eq!(
                    res_obj.items[i as usize].id,
                    generated_assets.pubkeys[cnt - 1 - i as usize].to_string(),
                    "asset should match the pubkey"
                );
            }
        }
        // test the 2nd page
        {
            let payload = SearchAssets {
                limit: Some(limit),
                page: Some(2),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            for i in 0..limit {
                assert_eq!(
                    res_obj.items[i as usize].id,
                    generated_assets.pubkeys[cnt - 1 - limit as usize - i as usize].to_string(),
                    "asset should match the pubkey"
                );
            }
        }
        // test the 3rd page - no more assets
        {
            let payload = SearchAssets {
                limit: Some(limit),
                page: Some(3),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert!(res_obj.items.is_empty(), "items should be empty");
        }
        // test the 2nd page using after
        {
            let payload = SearchAssets {
                limit: Some(limit),
                after: after,
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            for i in 0..limit {
                assert_eq!(
                    res_obj.items[i as usize].id,
                    generated_assets.pubkeys[cnt - 1 - limit as usize - i as usize].to_string(),
                    "asset should match the pubkey"
                );
            }
        }
        // test a request with before the first page returns an empty result
        {
            let payload = SearchAssets {
                limit: Some(limit),
                before: before,
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert!(res_obj.items.is_empty(), "items should be empty");
        }
        // test a request with all "common" fields filled in in the filter
        {
            let payload = SearchAssets {
                limit: Some(limit),
                interface: Some(Interface::V1NFT),
                owner_type: Some(OwnershipModel::Single),
                frozen: Some(false),
                supply: Some(1),
                compressed: Some(false),
                compressible: Some(false),
                royalty_target_type: Some(RoyaltyModel::Creators),
                royalty_amount: Some(0),
                burnt: Some(false),
                json_uri: Some("http://example.com".to_string()),
                sort_by: Some(AssetSorting {
                    sort_by: AssetSortBy::Created,
                    sort_direction: Some(AssetSortDirection::Desc),
                }),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            for i in 0..limit {
                assert_eq!(
                    res_obj.items[i as usize].id,
                    generated_assets.pubkeys[cnt - 1 - i as usize].to_string(),
                    "asset should match the pubkey"
                );
            }
        }
        // test a request with owner
        {
            let ref_value = generated_assets.owners[8].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                owner_address: Some(ref_value.owner.value.to_string()),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a creator
        {
            let ref_value = generated_assets.dynamic_details[5].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                creator_address: Some(ref_value.creators.value[0].creator.to_string()),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a creator verified field, considering the value was randomly generated in the dynamic data
        {
            let limit = 20;
            let payload = SearchAssets {
                limit: Some(limit),
                creator_verified: Some(true),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            // calculate the number of assets with creator verified true
            let mut cnt = 0;
            for dynamic in generated_assets.dynamic_details.iter() {
                if dynamic.creators.value[0].creator_verified {
                    cnt += 1;
                }
            }
            assert_eq!(res_obj.total, cnt, "total should be {}", cnt);
        }
        // test a request with an authority_address field
        {
            let ref_value = generated_assets.authorities[9].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                authority_address: Some(ref_value.authority.to_string()),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a collection set as grouping
        {
            let ref_value = generated_assets.collections[12].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                grouping: Some(("collection".to_string(), ref_value.collection.to_string())),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a delegate field
        {
            let ref_value = generated_assets.owners[13].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                delegate: Some(ref_value.delegate.value.unwrap().to_string()),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a supply mint field
        {
            let ref_value = generated_assets.static_details[14].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                supply_mint: Some(ref_value.pubkey.to_string()),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        // test a request with a royalty target
        {
            let ref_value = generated_assets.dynamic_details[15].clone();
            let payload = SearchAssets {
                limit: Some(limit),
                royalty_target: Some(ref_value.creators.value[0].creator.to_string()),
                royalty_target_type: Some(RoyaltyModel::Creators),
                royalty_amount: Some(ref_value.royalty_amount.value as u32),
                ..Default::default()
            };
            let res = api.search_assets(payload).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();

            assert_eq!(res_obj.total, 1, "total should be 1");
            assert_eq!(res_obj.items.len(), 1, "items length should be 1");
            assert_eq!(
                res_obj.items[0].id,
                ref_value.pubkey.to_string(),
                "asset should match the pubkey"
            );
        }
        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_asset_none_grouping_with_token_standard() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let pb = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let mut chain_data = ChainDataV1 {
            name: "name".to_string(),
            symbol: "symbol".to_string(),
            edition_nonce: Some(1),
            primary_sale_happened: false,
            token_standard: Some(TokenStandard::NonFungible),
            uses: None,
        };
        chain_data.sanitize();

        let chain_data = json!(chain_data);
        let asset_static_details = AssetStaticDetails {
            pubkey: pb,
            specification_asset_class: SpecificationAssetClass::Nft,
            royalty_target_type: RoyaltyTargetType::Creators,
            created_at: 12 as i64,
            edition_address: Some(MasterEdition::find_pda(&pb).0),
        };

        let dynamic_details = AssetDynamicDetails {
            pubkey: pb,
            is_compressed: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
            is_compressible: Updated::new(12, Some(UpdateVersion::Sequence(12)), false),
            supply: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), 1)),
            seq: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), 12)),
            onchain_data: Some(Updated::new(
                12,
                Some(UpdateVersion::Sequence(12)),
                chain_data.to_string(),
            )),
            creators: Updated::new(12, Some(UpdateVersion::Sequence(12)), vec![]),
            royalty_amount: Updated::new(12, Some(UpdateVersion::Sequence(12)), 5),
            url: Updated::new(
                12,
                Some(UpdateVersion::Sequence(12)),
                "https://ping-pong".to_string(),
            ),
            chain_mutability: Some(Updated::new(
                12,
                Some(UpdateVersion::Sequence(12)),
                ChainMutability::Mutable,
            )),
            lamports: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), 1)),
            executable: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), false)),
            metadata_owner: Some(Updated::new(
                12,
                Some(UpdateVersion::Sequence(12)),
                "ff".to_string(),
            )),
            ..Default::default()
        };

        let asset_authority = AssetAuthority {
            pubkey: pb,
            authority,
            slot_updated: 12,
            write_version: Some(1),
        };

        let owner = AssetOwner {
            pubkey: pb,
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), authority),
            delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
            owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
            owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
        };

        let metadata = OffChainData {
            url: "https://ping-pong".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone(), metadata)
            .unwrap();

        env.rocks_env
            .storage
            .asset_static_data
            .put(pb, asset_static_details)
            .unwrap();
        env.rocks_env
            .storage
            .asset_dynamic_data
            .put(pb, dynamic_details)
            .unwrap();
        env.rocks_env
            .storage
            .asset_authority_data
            .put(pb, asset_authority)
            .unwrap();
        env.rocks_env
            .storage
            .asset_owner_data
            .put(pb, owner)
            .unwrap();

        let payload = GetAsset { id: pb.to_string() };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["grouping"], Value::Array(vec![]));
        assert_eq!(
            response["content"]["metadata"]["token_standard"],
            "NonFungible"
        );

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_asset_without_offchain_data() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let pb = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let mut chain_data = ChainDataV1 {
            name: "name".to_string(),
            symbol: "symbol".to_string(),
            edition_nonce: Some(1),
            primary_sale_happened: false,
            token_standard: Some(TokenStandard::NonFungible),
            uses: None,
        };
        chain_data.sanitize();

        let chain_data = json!(chain_data);
        let asset_static_details = AssetStaticDetails {
            pubkey: pb,
            specification_asset_class: SpecificationAssetClass::Nft,
            royalty_target_type: RoyaltyTargetType::Creators,
            created_at: 12 as i64,
            edition_address: Some(MasterEdition::find_pda(&pb).0),
        };

        let json_uri = "http://someUrl.com".to_string();

        let dynamic_details = AssetDynamicDetails {
            pubkey: pb,
            is_compressed: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
            is_compressible: Updated::new(12, Some(UpdateVersion::Sequence(12)), false),
            supply: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), 1)),
            seq: Some(Updated::new(12, Some(UpdateVersion::Sequence(12)), 12)),
            onchain_data: Some(Updated::new(
                12,
                Some(UpdateVersion::Sequence(12)),
                chain_data.to_string(),
            )),
            creators: Updated::new(12, Some(UpdateVersion::Sequence(12)), vec![]),
            royalty_amount: Updated::new(12, Some(UpdateVersion::Sequence(12)), 5),
            url: Updated::new(12, Some(UpdateVersion::Sequence(12)), json_uri.clone()),
            ..Default::default()
        };

        let asset_authority = AssetAuthority {
            pubkey: pb,
            authority,
            slot_updated: 12,
            write_version: Some(1),
        };

        let owner = AssetOwner {
            pubkey: pb,
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), authority),
            delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
            owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
            owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
        };

        env.rocks_env
            .storage
            .asset_static_data
            .put(pb, asset_static_details)
            .unwrap();
        env.rocks_env
            .storage
            .asset_dynamic_data
            .put(pb, dynamic_details)
            .unwrap();
        env.rocks_env
            .storage
            .asset_authority_data
            .put(pb, asset_authority)
            .unwrap();
        env.rocks_env
            .storage
            .asset_owner_data
            .put(pb, owner)
            .unwrap();

        let payload = GetAsset { id: pb.to_string() };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["id"], pb.to_string());
        assert_eq!(response["grouping"], Value::Array(vec![]));
        assert_eq!(
            response["content"]["metadata"]["token_standard"],
            "NonFungible"
        );
        assert_eq!(response["content"]["json_uri"], json_uri);

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_fungible_asset() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let buffer = Arc::new(Buffer::new());

        let db_client = Arc::new(DBClient {
            pool: env.pg_env.pool.clone(),
        });

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
            db_client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        );

        let token_key = Pubkey::new_unique();
        let mint_key = Pubkey::new_unique();
        let owner_key = Pubkey::new_unique();

        let mint_auth_key = Pubkey::new_unique();

        let token_acc = TokenAccount {
            pubkey: token_key,
            mint: mint_key,
            delegate: None,
            owner: owner_key,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 1,
            write_version: 1,
        };

        let mint_acc = Mint {
            pubkey: mint_key,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: Some(mint_auth_key),
            freeze_authority: None,
            write_version: 1,
        };

        let metadata = MetadataInfo {
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
                token_standard: Some(BLKTokenStandard::Fungible),
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
        };
        let mut metadata_info = HashMap::new();
        metadata_info.insert(mint_key.to_bytes().to_vec(), metadata);

        let metadata = OffChainData {
            url: "https://ping-pong".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };

        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone(), metadata)
            .unwrap();

        token_updates_processor
            .transform_and_save_mint_accs(&[(Vec::<u8>::new(), mint_acc)].into_iter().collect())
            .await;
        token_updates_processor
            .transform_and_save_token_accs(&[(token_acc.pubkey, token_acc)].into_iter().collect())
            .await;

        mplx_updates_processor
            .transform_and_store_metadata_accs(&metadata_info)
            .await;

        let payload = GetAsset {
            id: mint_key.to_string(),
        };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["ownership"]["ownership_model"], "single");
        assert_eq!(response["ownership"]["owner"], "");
        assert_eq!(response["interface"], "FungibleToken".to_string());

        let mint_acc = Mint {
            pubkey: mint_key,
            slot_updated: 2,
            supply: 2,
            decimals: 0,
            mint_authority: Some(mint_auth_key),
            freeze_authority: None,
            write_version: 2,
        };

        token_updates_processor
            .transform_and_save_mint_accs(&[(Vec::<u8>::new(), mint_acc)].into_iter().collect())
            .await;

        let payload = GetAsset {
            id: mint_key.to_string(),
        };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["ownership"]["ownership_model"], "token");
        assert_eq!(response["ownership"]["owner"], "");
        assert_eq!(response["interface"], "FungibleToken".to_string());

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_asset_programable_interface() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let buffer = Arc::new(Buffer::new());

        let db_client = Arc::new(DBClient {
            pool: env.pg_env.pool.clone(),
        });

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
            db_client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        );

        let mut metadata_info = HashMap::new();
        let mut mint_accs = Vec::new();
        let mut token_accs = Vec::new();

        let token_standards = vec![
            BLKTokenStandard::ProgrammableNonFungible,
            BLKTokenStandard::ProgrammableNonFungibleEdition,
        ];

        for standard in token_standards.iter() {
            let token_key = Pubkey::new_unique();
            let mint_key = Pubkey::new_unique();
            let owner_key = Pubkey::new_unique();

            let mint_auth_key = Pubkey::new_unique();

            let token_acc = TokenAccount {
                pubkey: token_key,
                mint: mint_key,
                delegate: None,
                owner: owner_key,
                frozen: false,
                delegated_amount: 0,
                slot_updated: 1,
                amount: 1,
                write_version: 1,
            };

            let mint_acc = Mint {
                pubkey: mint_key,
                slot_updated: 1,
                supply: 1,
                decimals: 0,
                mint_authority: Some(mint_auth_key),
                freeze_authority: None,
                write_version: 1,
            };

            let metadata = MetadataInfo {
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
                    token_standard: Some(*standard),
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
            };

            metadata_info.insert(mint_key.to_bytes().to_vec(), metadata);
            mint_accs.push(mint_acc);
            token_accs.push(token_acc);
        }

        let metadata = OffChainData {
            url: "https://ping-pong".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };

        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone(), metadata)
            .unwrap();

        token_updates_processor
            .transform_and_save_mint_accs(
                &mint_accs
                    .clone()
                    .into_iter()
                    .map(|mint| (mint.pubkey.to_bytes().to_vec(), mint))
                    .collect(),
            )
            .await;
        token_updates_processor
            .transform_and_save_token_accs(
                &token_accs
                    .into_iter()
                    .map(|token_acc| (token_acc.pubkey, token_acc))
                    .collect(),
            )
            .await;

        mplx_updates_processor
            .transform_and_store_metadata_accs(&metadata_info)
            .await;

        let payload = GetAsset {
            id: mint_accs[0].pubkey.to_string(),
        };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["id"], mint_accs[0].pubkey.to_string());
        assert_eq!(response["interface"], "ProgrammableNFT".to_string());

        let payload = GetAsset {
            id: mint_accs[1].pubkey.to_string(),
        };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["id"], mint_accs[1].pubkey.to_string());
        assert_eq!(response["interface"], "ProgrammableNFT".to_string());

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_burnt() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let keep_running = Arc::new(AtomicBool::new(true));

        let buffer = Arc::new(Buffer::new());

        let db_client = Arc::new(DBClient {
            pool: env.pg_env.pool.clone(),
        });

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
            db_client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
        );

        let token_key = Pubkey::new_unique();
        let mint_key = Pubkey::new_unique();
        let owner_key = Pubkey::new_unique();

        let mint_auth_key = Pubkey::new_unique();

        let token_acc = TokenAccount {
            pubkey: token_key,
            mint: mint_key,
            delegate: None,
            owner: owner_key,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 1,
            amount: 1,
            write_version: 1,
        };

        let mint_acc = Mint {
            pubkey: mint_key,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: Some(mint_auth_key),
            freeze_authority: None,
            write_version: 1,
        };

        let metadata = MetadataInfo {
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
                token_standard: Some(BLKTokenStandard::Fungible),
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
        };

        let metadata_ofch = OffChainData {
            url: "https://ping-pong".to_string(),
            metadata: "{\"msg\": \"hallo\"}".to_string(),
        };

        let (metadata_key, _) = Pubkey::find_program_address(
            &[
                "metadata".as_ref(),
                blockbuster::programs::token_metadata::token_metadata_id().as_ref(),
                mint_key.as_ref(),
            ],
            &blockbuster::programs::token_metadata::token_metadata_id(),
        );

        let mut mtd_buff = buffer.mplx_metadata_info.lock().await;

        mtd_buff.insert(metadata_key.to_bytes().to_vec(), metadata);
        drop(mtd_buff);

        let mut burnt_buff = buffer.burnt_metadata_at_slot.lock().await;

        burnt_buff.insert(metadata_key, BurntMetadataSlot { slot_updated: 2 });
        drop(burnt_buff);

        let mut mplx_updates_processor_clone = mplx_updates_processor.clone();
        let cloned_keep_running = keep_running.clone();
        tokio::spawn(async move {
            mplx_updates_processor_clone
                .process_metadata_accs(cloned_keep_running)
                .await;
        });
        let mut mplx_updates_processor_clone = mplx_updates_processor.clone();
        let cloned_keep_running = keep_running.clone();
        tokio::spawn(async move {
            mplx_updates_processor_clone
                .process_burnt_accs(cloned_keep_running)
                .await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata_ofch.url.clone(), metadata_ofch)
            .unwrap();

        token_updates_processor
            .transform_and_save_mint_accs(&[(Vec::<u8>::new(), mint_acc)].into_iter().collect())
            .await;
        token_updates_processor
            .transform_and_save_token_accs(&[(token_acc.pubkey, token_acc)].into_iter().collect())
            .await;

        let payload = GetAsset {
            id: mint_key.to_string(),
        };
        let response = api.get_asset(payload).await.unwrap();

        assert_eq!(response["ownership"]["ownership_model"], "single");
        assert_eq!(response["ownership"]["owner"], "");
        assert_eq!(response["interface"], "FungibleToken".to_string());
        assert_eq!(response["burnt"], true);

        keep_running.store(false, Ordering::SeqCst);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_token_accounts() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
        );

        let buffer = Arc::new(Buffer::new());
        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );

        let mut token_accounts = HashMap::new();
        let first_owner = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();
        let first_mint = Pubkey::new_unique();
        let second_mint = Pubkey::new_unique();
        for _ in 0..1000 {
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint: Pubkey::new_unique(),
                    delegate: None,
                    owner: first_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 1050,
                    write_version: 10,
                },
            );
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint: Pubkey::new_unique(),
                    delegate: None,
                    owner: second_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 1050,
                    write_version: 10,
                },
            );
        }
        // Insert zero amounts
        for _ in 0..10 {
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint: Pubkey::new_unique(),
                    delegate: None,
                    owner: first_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 0,
                    write_version: 10,
                },
            );
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint: Pubkey::new_unique(),
                    delegate: None,
                    owner: second_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 0,
                    write_version: 10,
                },
            );
        }
        let first_owner_with_mint_count = 143;
        for _ in 0..first_owner_with_mint_count {
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint: second_mint,
                    delegate: None,
                    owner: second_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 140,
                    write_version: 10,
                },
            );
        }

        token_updates_processor
            .transform_and_save_token_accs(&token_accounts)
            .await;

        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: Some(first_owner.to_string()),
            mint: None,
            options: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), 10);

        let payload = GetTokenAccounts {
            limit: Some(1000),
            page: Some(2),
            owner: Some(first_owner.to_string()),
            mint: None,
            options: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), 0);

        let payload = GetTokenAccounts {
            limit: Some(1000),
            page: Some(2),
            owner: Some(first_owner.to_string()),
            mint: None,
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), 10);

        let payload = GetTokenAccounts {
            limit: Some(1000),
            page: Some(1),
            owner: Some(second_owner.to_string()),
            mint: Some(second_mint.to_string()),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(
            parsed_response.token_accounts.len(),
            first_owner_with_mint_count
        );

        let payload = GetTokenAccounts {
            limit: None,
            page: None,
            owner: Some(second_owner.to_string()),
            mint: Some(first_mint.to_string()),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), 0);

        env.teardown().await;
    }
}
