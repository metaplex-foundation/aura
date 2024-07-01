#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use blockbuster::token_metadata::accounts::Metadata;
    use blockbuster::token_metadata::types::{Key, TokenStandard as BLKTokenStandard};
    use digital_asset_types::rpc::response::{
        AssetList, TokenAccountsList, TransactionSignatureList,
    };
    use entities::api_req_params::{
        DisplayOptions, GetAssetProof, GetAssetSignatures, GetTokenAccounts, Options,
    };
    use entities::models::{AssetSignature, AssetSignatureKey, OffChainData};
    use entities::{
        api_req_params::{
            AssetSortBy, AssetSortDirection, AssetSorting, GetAsset, GetAssetsByAuthority,
            GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner, SearchAssets,
        },
        enums::{
            ChainMutability, Interface, OwnerType, OwnershipModel, RoyaltyModel, RoyaltyTargetType,
            SpecificationAssetClass, TokenStandard,
        },
        models::{ChainDataV1, UpdateVersion, Updated},
    };
    use interface::json::{MockJsonDownloader, MockJsonPersister};
    use metrics_utils::{ApiMetricsConfig, IngesterMetricsConfig};
    use mockall::predicate;
    use mpl_token_metadata::accounts::MasterEdition;
    use nft_ingester::api::error::DasApiError;
    use nft_ingester::{
        buffer::Buffer,
        config::JsonMiddlewareConfig,
        json_worker::JsonWorker,
        mplx_updates_processor::{BurntMetadataSlot, MetadataInfo, MplxAccsProcessor},
        token_updates_processor::TokenAccsProcessor,
    };
    use rocks_db::asset::AssetLeaf;
    use rocks_db::tree_seq::TreesGaps;
    use rocks_db::{
        columns::{Mint, TokenAccount},
        AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
    };
    use serde_json::{json, Value};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use testcontainers::clients::Cli;
    use tokio::{sync::Mutex, task::JoinSet};
    use usecase::proofs::MaybeProofChecker;

    const SLOT_UPDATED: u64 = 100;
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_assets() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let limit = 10;
        let before: Option<String>;
        let after: Option<String>;
        // test base case with only a limit and default (0) page
        {
            let payload = SearchAssets {
                limit: Some(limit),
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
            assert!(res.is_object());
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            after = res_obj.cursor.clone();
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

            // by default API uses cursor so the first returned element in this request is 'before'
            // note: sorting wasn't pointed so cursor is just a pubkey
            before = Some(res_obj.items[0].id.as_str().to_string());
        }
        // test limit and 1st page being the same as the 0
        {
            let payload = SearchAssets {
                limit: Some(limit),
                page: Some(1),
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert!(res_obj.items.is_empty(), "items should be empty");
        }
        // test the 2nd page using after
        {
            let payload = SearchAssets {
                limit: Some(limit),
                after: after,
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                owner_address: ref_value.owner.value.map(|owner| owner.to_string()),
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                grouping: Some((
                    "collection".to_string(),
                    ref_value.collection.value.to_string(),
                )),
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
                options: Some(Options {
                    show_unverified_collections: true,
                }),
                ..Default::default()
            };
            let res = api
                .search_assets(payload, mutexed_tasks.clone())
                .await
                .unwrap();
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

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
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
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

        let payload = GetAsset {
            id: pb.to_string(),
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

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
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
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

        let payload = GetAsset {
            id: pb.to_string(),
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let buffer = Arc::new(Buffer::new());

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
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
                name: "".to_string(),
                symbol: "".to_string(),
                uri: "".to_string(),
                seller_fee_basis_points: 0,
                creators: None,
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
            rent_epoch: 0,
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
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

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
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let buffer = Arc::new(Buffer::new());

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
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
                    creators: None,
                    name: "".to_string(),
                    symbol: "".to_string(),
                    uri: "".to_string(),
                    seller_fee_basis_points: 0,
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
                rent_epoch: 0,
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
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["id"], mint_accs[0].pubkey.to_string());
        assert_eq!(response["interface"], "ProgrammableNFT".to_string());

        let payload = GetAsset {
            id: mint_accs[1].pubkey.to_string(),
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let buffer = Arc::new(Buffer::new());

        let token_updates_processor = TokenAccsProcessor::new(
            env.rocks_env.storage.clone(),
            buffer.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            1,
        );
        let mplx_updates_processor = MplxAccsProcessor::new(
            1,
            buffer.clone(),
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
                creators: None,
                name: "".to_string(),
                symbol: "".to_string(),
                uri: "".to_string(),
                seller_fee_basis_points: 0,
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
            rent_epoch: 0,
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
        let mut mplx_metadata_info: HashMap<Vec<u8>, MetadataInfo> = HashMap::new();
        mplx_metadata_info.insert(metadata_key.to_bytes().to_vec(), metadata);

        let mut burnt_buff: HashMap<Pubkey, BurntMetadataSlot> = HashMap::new();
        burnt_buff.insert(metadata_key, BurntMetadataSlot { slot_updated: 2 });

        mplx_updates_processor
            .transform_and_store_metadata_accs(&mplx_metadata_info)
            .await;
        mplx_updates_processor
            .transform_and_store_burnt_metadata(&burnt_buff)
            .await;
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
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["ownership"]["ownership_model"], "single");
        assert_eq!(response["ownership"]["owner"], "");
        assert_eq!(response["interface"], "FungibleToken".to_string());
        assert_eq!(response["burnt"], true);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_asset_signatures() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );

        let first_tree = Pubkey::new_unique();
        let second_tree = Pubkey::new_unique();
        let first_leaf_idx = 100;
        let first_tree_other_leaf_idx = 101;
        let second_leaf_idx = 200;

        for seq in 0..100 {
            let signature = Signature::new_unique();
            env.rocks_env
                .storage
                .asset_signature
                .put(
                    AssetSignatureKey {
                        tree: first_tree,
                        leaf_idx: first_leaf_idx,
                        seq,
                    },
                    AssetSignature {
                        tx: signature.to_string(),
                        instruction: "TestInstruction".to_string(),
                        slot: seq * 2,
                    },
                )
                .unwrap();
        }
        for seq in 100..200 {
            let signature = Signature::new_unique();
            env.rocks_env
                .storage
                .asset_signature
                .put(
                    AssetSignatureKey {
                        tree: first_tree,
                        leaf_idx: first_tree_other_leaf_idx,
                        seq,
                    },
                    AssetSignature {
                        tx: signature.to_string(),
                        instruction: "TestInstruction".to_string(),
                        slot: seq * 2,
                    },
                )
                .unwrap();
        }
        for seq in 0..100 {
            let signature = Signature::new_unique();
            env.rocks_env
                .storage
                .asset_signature
                .put(
                    AssetSignatureKey {
                        tree: second_tree,
                        leaf_idx: second_leaf_idx,
                        seq,
                    },
                    AssetSignature {
                        tx: signature.to_string(),
                        instruction: "TestInstruction".to_string(),
                        slot: seq * 2,
                    },
                )
                .unwrap();
        }

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(50),
            page: Some(1),
            before: None,
            after: None,
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: None,
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, Some("50".to_string()));
        assert_eq!(parsed_response.before, Some("99".to_string()));
        assert_eq!(parsed_response.items.len(), 50);

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(50),
            page: None,
            before: None,
            after: parsed_response.after,
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: None,
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, Some("0".to_string()));
        assert_eq!(parsed_response.before, Some("49".to_string()));
        assert_eq!(parsed_response.items.len(), 50);

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(30),
            page: Some(4),
            before: None,
            after: None,
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: Some(AssetSortDirection::Asc),
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, Some("99".to_string()));
        assert_eq!(parsed_response.before, Some("90".to_string()));
        assert_eq!(parsed_response.items.len(), 10);

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(30),
            page: Some(2),
            before: None,
            after: None,
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: Some(AssetSortDirection::Desc),
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, Some("40".to_string()));
        assert_eq!(parsed_response.before, Some("69".to_string()));
        assert_eq!(parsed_response.items.len(), 30);

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(30),
            page: None,
            before: Some("49".to_string()),
            after: Some("51".to_string()),
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: Some(AssetSortDirection::Asc),
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, None);
        assert_eq!(parsed_response.before, None);
        assert_eq!(parsed_response.items.len(), 0);

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(30),
            page: None,
            before: Some("49".to_string()),
            after: Some("51".to_string()),
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: Some(AssetSortDirection::Desc),
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.after, Some("50".to_string()));
        assert_eq!(parsed_response.before, Some("50".to_string()));
        assert_eq!(parsed_response.items.len(), 1);

        // ensure there are no extra signatures returned for an asset

        let payload = GetAssetSignatures {
            id: None,
            limit: Some(500),
            page: Some(1),
            before: None,
            after: None,
            tree: Some(first_tree.to_string()),
            leaf_index: Some(first_leaf_idx),
            sort_direction: None,
            cursor: None,
        };
        let response = api.get_asset_signatures(payload, false).await.unwrap();
        let parsed_response: TransactionSignatureList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.items.len(), 100);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_token_accounts() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            after: None,
            before: None,
            cursor: None,
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
            after: None,
            before: None,
            cursor: None,
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
            after: None,
            before: None,
            cursor: None,
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
            after: None,
            before: None,
            cursor: None,
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
            after: None,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), 0);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_token_accounts_pagination() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
        let third_owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
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
            let pk = Pubkey::new_unique();
            token_accounts.insert(
                pk,
                TokenAccount {
                    pubkey: pk,
                    mint,
                    delegate: None,
                    owner: third_owner,
                    frozen: false,
                    delegated_amount: 0,
                    slot_updated: 10,
                    amount: 1050,
                    write_version: 10,
                },
            );
        }

        token_updates_processor
            .transform_and_save_token_accs(&token_accounts)
            .await;

        check_pagination(&api, Some(first_owner.to_string()), None).await;

        check_pagination(&api, Some(third_owner.to_string()), Some(mint.to_string())).await;

        check_pagination(&api, None, Some(mint.to_string())).await;
    }

    async fn check_pagination(
        api: &nft_ingester::api::api_impl::DasApi<MaybeProofChecker, JsonWorker, JsonWorker>,
        owner: Option<String>,
        mint: Option<String>,
    ) {
        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: None,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_10: TokenAccountsList = serde_json::from_value(response).unwrap();

        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: None,
            before: None,
            cursor: first_10.cursor.clone(),
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let second_10: TokenAccountsList = serde_json::from_value(response).unwrap();

        let payload = GetTokenAccounts {
            limit: Some(20),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: None,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_20: TokenAccountsList = serde_json::from_value(response).unwrap();

        let mut first_two_resp = first_10.token_accounts;
        first_two_resp.extend(second_10.token_accounts.clone());

        assert_eq!(first_20.token_accounts, first_two_resp);

        let payload = GetTokenAccounts {
            limit: Some(9),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: None,
            // it's safe to do it in test because we want to check how reverse work
            before: first_20.cursor.clone(),
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_10_reverse: TokenAccountsList = serde_json::from_value(response).unwrap();

        let reversed = first_10_reverse.token_accounts;
        let mut second_10_resp = second_10.token_accounts.clone();
        // pop because we select 9 assets
        // select 9 because request with before do not return asset which is in before parameter
        second_10_resp.pop();
        assert_eq!(reversed, second_10_resp);

        let payload = GetTokenAccounts {
            limit: None,
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            // it's safe to do it in test because we want to check how reverse work
            after: first_10.cursor.clone(),
            before: first_20.cursor,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_10_before_after: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(
            first_10_before_after.token_accounts,
            second_10.token_accounts
        );

        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: first_10.cursor,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let after_first_10: TokenAccountsList = serde_json::from_value(response).unwrap();

        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: after_first_10.after,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let after_first_20: TokenAccountsList = serde_json::from_value(response).unwrap();

        let payload = GetTokenAccounts {
            limit: Some(30),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions {
                show_zero_balance: true,
            }),
            after: None,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_30: TokenAccountsList = serde_json::from_value(response).unwrap();

        let mut combined_10_30 = after_first_10.token_accounts;
        combined_10_30.extend(after_first_20.token_accounts.clone());

        assert_eq!(combined_10_30, first_30.token_accounts[10..]);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_assets_by_owner() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let ref_value = generated_assets.owners[8].clone();
        let payload = GetAssetsByOwner {
            owner_address: ref_value
                .owner
                .value
                .map(|owner| owner.to_string())
                .unwrap_or_default(),
            sort_by: None,
            limit: None,
            page: None,
            before: None,
            after: None,
            cursor: None,
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let res = api
            .get_assets_by_owner(payload, mutexed_tasks.clone())
            .await
            .unwrap();
        let res_obj: AssetList = serde_json::from_value(res).unwrap();

        assert_eq!(res_obj.total, 1, "total should be 1");
        assert_eq!(res_obj.items.len(), 1, "items length should be 1");
        assert_eq!(
            res_obj.items[0].id,
            ref_value.pubkey.to_string(),
            "asset should match the pubkey"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_assets_by_group() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let ref_value = generated_assets.collections[12].clone();
        let payload = GetAssetsByGroup {
            group_key: "collection".to_string(),
            group_value: ref_value.collection.value.to_string(),
            sort_by: None,
            limit: None,
            page: None,
            before: None,
            after: None,
            cursor: None,
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let res = api
            .get_assets_by_group(payload, mutexed_tasks.clone())
            .await
            .unwrap();
        let res_obj: AssetList = serde_json::from_value(res).unwrap();

        assert_eq!(res_obj.total, 1, "total should be 1");
        assert_eq!(res_obj.items.len(), 1, "items length should be 1");
        assert_eq!(
            res_obj.items[0].id,
            ref_value.pubkey.to_string(),
            "asset should match the pubkey"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_assets_by_creator() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let ref_value = generated_assets.dynamic_details[5].clone();
        let payload = GetAssetsByCreator {
            creator_address: ref_value.creators.value[0].creator.to_string(),
            only_verified: None, // as the test env randomly generates verified and unverified creators we're not passing in this filter
            sort_by: None,
            limit: None,
            page: None,
            before: None,
            after: None,
            cursor: None,
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let res = api
            .get_assets_by_creator(payload, mutexed_tasks.clone())
            .await
            .unwrap();
        let res_obj: AssetList = serde_json::from_value(res).unwrap();

        assert_eq!(res_obj.total, 1, "total should be 1");
        assert_eq!(res_obj.items.len(), 1, "items length should be 1");
        assert_eq!(
            res_obj.items[0].id,
            ref_value.pubkey.to_string(),
            "asset should match the pubkey"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_assets_by_authority() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let ref_value = generated_assets.authorities[9].clone();
        let payload = GetAssetsByAuthority {
            authority_address: ref_value.authority.to_string(),
            sort_by: None,
            limit: None,
            page: None,
            before: None,
            after: None,
            cursor: None,
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };
        let res = api
            .get_assets_by_authority(payload, mutexed_tasks.clone())
            .await
            .unwrap();
        let res_obj: AssetList = serde_json::from_value(res).unwrap();

        assert_eq!(res_obj.total, 1, "total should be 1");
        assert_eq!(res_obj.items.len(), 1, "items length should be 1");
        assert_eq!(
            res_obj.items[0].id,
            ref_value.pubkey.to_string(),
            "asset should match the pubkey"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_json_middleware() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let url = "http://someUrl.com".to_string();

        let offchain_data = r#"
        {
            "name": "5554",
            "symbol": "SYM",
            "description": "5555 asset",
            "external_url": "https://twitter.com",
            "seller_fee_basis_points": 0,
            "image": "https://arweave.net/",
            "attributes": [
                {
                "trait_type": "Background",
                "value": "Varden"
            },
            {
                "trait_type": "Body",
                "value": "Robot"
            },
            {
                "trait_type": "Clothes",
                "value": "White Space Doodle Sweater"
            }
            ],
            "properties": {
            "files": [
                {
                "uri": "https://arweave.net",
                "type": "image/png"
                }
            ],
            "category": "image"
            }
        }
        "#;

        let mut mock_middleware = MockJsonDownloader::new();
        mock_middleware
            .expect_download_file()
            .with(predicate::eq(url))
            .times(1)
            .returning(move |_| Ok(offchain_data.to_string()));

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            MockJsonDownloader,
            MockJsonPersister,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            50,
            Some(Arc::new(mock_middleware)),
            None,
            JsonMiddlewareConfig {
                is_enabled: true,
                max_urls_to_parse: 10,
            },
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
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
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

        let payload = GetAsset {
            id: pb.to_string(),
            options: Some(Options {
                show_unverified_collections: true,
            }),
        };

        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        let expected_content: Value = serde_json::from_str(
            r#"
        {
            "$schema": "https://schema.metaplex.com/nft1.0.json",
            "json_uri": "http://someUrl.com",
            "files": [
                { "uri": "https://arweave.net/", "mime": "image/png" },
                { "uri": "https://arweave.net", "mime": "image/png" }
            ],
            "metadata": {
            "attributes": [
                { "trait_type": "Background", "value": "Varden" },
                { "trait_type": "Body", "value": "Robot" },
                { "trait_type": "Clothes", "value": "White Space Doodle Sweater" }
                ],
                "description": "5555 asset",
                "name": "name",
                "symbol": "symbol",
                "token_standard": "NonFungible"
            },
            "links": {
                "image": "https://arweave.net/",
                "external_url": "https://twitter.com"
            }
        }
        "#,
        )
        .unwrap();

        assert_eq!(response["id"], pb.to_string());
        assert_eq!(response["grouping"], Value::Array(vec![]));
        assert_eq!(
            response["content"]["metadata"]["token_standard"],
            "NonFungible"
        );
        assert_eq!(response["content"]["json_uri"], json_uri);

        assert_eq!(response["content"], expected_content);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_cannot_service_gaped_tree() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
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
            );
        let asset_id = Pubkey::new_unique();
        let tree_id = Pubkey::new_unique();
        env.rocks_env
            .storage
            .asset_leaf_data
            .put_async(
                asset_id,
                AssetLeaf {
                    pubkey: asset_id,
                    tree_id,
                    leaf: None,
                    nonce: None,
                    data_hash: None,
                    creator_hash: None,
                    leaf_seq: None,
                    slot_updated: 0,
                },
            )
            .await
            .unwrap();
        env.rocks_env
            .storage
            .trees_gaps
            .put_async(tree_id, TreesGaps {})
            .await
            .unwrap();
        let payload = GetAssetProof {
            id: asset_id.to_string(),
        };
        let res = api.get_asset_proof(payload).await.err().unwrap();
        assert!(matches!(res, DasApiError::CannotServiceRequest));
    }
}
