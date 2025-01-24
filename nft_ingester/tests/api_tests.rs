#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::{collections::HashMap, str::FromStr, sync::Arc};

    use base64::{engine::general_purpose, Engine};
    use blockbuster::{
        programs::token_extensions::{
            extension::{
                ShadowInterestBearingConfig, ShadowTransferFee, ShadowTransferFeeConfig,
                UnixTimestamp,
            },
            MintAccountExtensions,
        },
        token_metadata::accounts::Metadata,
    };
    use entities::{
        api_req_params::{
            AssetSortBy, AssetSortDirection, AssetSorting, DisplayOptions, GetAsset, GetAssetProof,
            GetAssetSignatures, GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup,
            GetAssetsByOwner, GetByMethodsOptions, GetCoreFees, GetTokenAccounts, Options,
            SearchAssets, SearchAssetsOptions,
        },
        enums::{
            AssetType, ChainMutability, Interface, OwnerType, OwnershipModel, RoyaltyModel,
            RoyaltyTargetType, SpecificationAssetClass, TokenStandard, TokenType, ASSET_TYPES,
        },
        models::{
            AssetSignature, AssetSignatureKey, BurntMetadataSlot, ChainDataV1, MetadataInfo, Mint,
            TokenAccount, UpdateVersion, Updated,
        },
    };
    use interface::{
        account_balance::MockAccountBalanceGetter,
        json::{MockJsonDownloader, MockJsonPersister},
    };
    use metrics_utils::{ApiMetricsConfig, IngesterMetricsConfig, SynchronizerMetricsConfig};
    use mockall::predicate;
    use mpl_token_metadata::{accounts::MasterEdition, types::Key};
    use nft_ingester::{
        api::{
            dapi::{
                response::{
                    AssetList, CoreFeesAccountsList, TokenAccountsList, TransactionSignatureList,
                },
                rpc_asset_models::Asset,
            },
            error::DasApiError,
        },
        cleaners::indexer_cleaner::clean_syncronized_idxs,
        config::JsonMiddlewareConfig,
        json_worker::JsonWorker,
        price_fetcher::{CoinGeckoPriceFetcher, SolanaPriceUpdater},
        processors::account_based::{
            mplx_updates_processor::MplxAccountsProcessor,
            token_updates_processor::TokenAccountsProcessor,
        },
        raydium_price_fetcher::RaydiumTokenPriceFetcher,
    };
    use rocks_db::{
        batch_savers::BatchSaveStorage,
        column::TypedColumn,
        columns::{
            asset::{
                AssetAuthority, AssetCompleteDetails, AssetDynamicDetails, AssetLeaf, AssetOwner,
                AssetStaticDetails,
            },
            inscriptions::{Inscription, InscriptionData},
            offchain_data::{OffChainData, StorageMutability},
        },
        tree_seq::TreesGaps,
        Storage, ToFlatbuffersConverter,
    };
    use serde_json::{json, Value};
    use setup::rocks::{RocksTestEnvironment, RocksTestEnvironmentSetup};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use spl_pod::{
        optional_keys::OptionalNonZeroPubkey,
        primitives::{PodU16, PodU64},
    };
    use spl_token_2022::extension::interest_bearing_mint::BasisPoints;
    use sqlx::QueryBuilder;
    use tempfile::TempDir;
    use testcontainers::clients::Cli;
    use tokio::{sync::Mutex, task::JoinSet};
    use usecase::proofs::MaybeProofChecker;

    const SLOT_UPDATED: u64 = 100;
    // corresponds to So11111111111111111111111111111111111111112
    pub const NATIVE_MINT_PUBKEY: Pubkey = Pubkey::new_from_array([
        6, 155, 136, 87, 254, 171, 129, 132, 251, 104, 127, 99, 70, 24, 192, 53, 218, 196, 57, 220,
        26, 235, 59, 85, 152, 160, 240, 0, 0, 0, 0, 1,
    ]);

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_assets() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
            assert!(res.is_object());
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            after = res_obj.cursor.clone();
            assert_eq!(res_obj.total, limit, "total should match the limit");
            assert_eq!(res_obj.limit, limit, "limit should match the requested limit");
            assert_eq!(res_obj.items.len(), limit as usize, "assets length should match the limit");

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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
            let res_obj: AssetList = serde_json::from_value(res).unwrap();
            assert!(res_obj.items.is_empty(), "items should be empty");
        }
        // test the 2nd page using after
        {
            let payload = SearchAssets {
                limit: Some(limit),
                after,
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                before,
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                grouping: Some(("collection".to_string(), ref_value.collection.value.to_string())),
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
                options: SearchAssetsOptions {
                    show_unverified_collections: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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

        let asset_authority =
            AssetAuthority { pubkey: pb, authority, slot_updated: 12, write_version: Some(1) };

        let owner = AssetOwner {
            pubkey: pb,
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
            delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
            owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
            owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
            is_current_owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
        };

        let metadata = OffChainData {
            url: Some("https://ping-pong".to_string()),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            storage_mutability: StorageMutability::Immutable,
            last_read_at: 0,
        };
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone().unwrap(), metadata)
            .unwrap();

        let asset_complete_details = AssetCompleteDetails {
            pubkey: pb,
            static_details: Some(asset_static_details),
            dynamic_details: Some(dynamic_details),
            authority: Some(asset_authority),
            owner: Some(owner),
            ..Default::default()
        };
        env.rocks_env
            .storage
            .db
            .put_cf(
                &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                pb,
                asset_complete_details.convert_to_fb_bytes(),
            )
            .unwrap();

        let payload = GetAsset {
            id: pb.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["grouping"], Value::Array(vec![]));
        assert_eq!(response["content"]["metadata"]["token_standard"], "NonFungible");

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_metadata_sanitizer() {
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, 0, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let whitespace_options = " \t\n\r\x0B\x0C\u{00A0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\
        \u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200A}\u{2028}\
        \u{2029}\u{202F}\u{205F}\u{3000}";

        for whitespace in whitespace_options.chars() {
            let pb = Pubkey::new_unique();
            let authority = Pubkey::new_unique();

            let mut chain_data = ChainDataV1 {
                name: format!("{} name {}", whitespace, whitespace).to_string(),
                symbol: format!("{} symbol {}", whitespace, whitespace).to_string(),
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

            let asset_authority =
                AssetAuthority { pubkey: pb, authority, slot_updated: 12, write_version: Some(1) };

            let owner = AssetOwner {
                pubkey: pb,
                owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
                delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
                owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
                owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
                is_current_owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
            };

            let metadata = OffChainData {
                url: Some("https://ping-pong".to_string()),
                metadata: Some("{\"msg\": \"hallo\"}".to_string()),
                storage_mutability: StorageMutability::Immutable,
                last_read_at: 0,
            };
            env.rocks_env
                .storage
                .asset_offchain_data
                .put(metadata.url.clone().unwrap(), metadata)
                .unwrap();

            let asset_complete_details = AssetCompleteDetails {
                pubkey: pb,
                static_details: Some(asset_static_details),
                dynamic_details: Some(dynamic_details),
                authority: Some(asset_authority),
                owner: Some(owner),
                ..Default::default()
            };
            env.rocks_env
                .storage
                .db
                .put_cf(
                    &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                    pb,
                    asset_complete_details.convert_to_fb_bytes(),
                )
                .unwrap();

            let payload = GetAsset {
                id: pb.to_string(),
                options: Options { show_unverified_collections: true, ..Default::default() },
            };
            let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
            assert_eq!(
                response["content"]["metadata"]["name"],
                format!("{} name {}", whitespace, whitespace)
            );
            assert_eq!(
                response["content"]["metadata"]["symbol"],
                format!("{} symbol {}", whitespace, whitespace)
            );
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_asset_without_offchain_data() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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

        let asset_authority =
            AssetAuthority { pubkey: pb, authority, slot_updated: 12, write_version: Some(1) };

        let owner = AssetOwner {
            pubkey: pb,
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
            delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
            owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
            owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
            is_current_owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
        };
        let asset_complete_details = AssetCompleteDetails {
            pubkey: pb,
            static_details: Some(asset_static_details),
            dynamic_details: Some(dynamic_details),
            authority: Some(asset_authority),
            owner: Some(owner),
            ..Default::default()
        };
        env.rocks_env
            .storage
            .db
            .put_cf(
                &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                pb,
                asset_complete_details.convert_to_fb_bytes(),
            )
            .unwrap();

        let payload = GetAsset {
            id: pb.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["id"], pb.to_string());
        assert_eq!(response["grouping"], Value::Array(vec![]));
        assert_eq!(response["content"]["metadata"]["token_standard"], "NonFungible");
        assert_eq!(response["content"]["json_uri"], json_uri);

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore = "FIXME: owner returned is not empty"]
    async fn test_fungible_asset() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let token_updates_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        let mplx_updates_processor =
            MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let token_key = Pubkey::new_unique();
        let mint_key = Pubkey::new_unique();
        let owner_key = Pubkey::new_unique();

        let mint_auth_key = Pubkey::new_unique();

        let token_acc = TokenAccount {
            pubkey: token_key,
            mint: mint_key,
            delegate: None,
            owner: owner_key,
            extensions: None,
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
            token_program: Default::default(),
            extensions: None,
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
                token_standard: Some(mpl_token_metadata::types::TokenStandard::Fungible),
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
        let offchain_data = OffChainData {
            url: Some("https://ping-pong".to_string()),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            ..Default::default()
        };

        env.rocks_env
            .storage
            .asset_offchain_data
            .put(offchain_data.url.clone().unwrap(), offchain_data)
            .unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        token_updates_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint_acc)
            .unwrap();
        token_updates_processor
            .transform_and_save_token_account(&mut batch_storage, token_acc.pubkey, &token_acc)
            .unwrap();

        mplx_updates_processor
            .transform_and_store_metadata_account(&mut batch_storage, mint_key, &metadata)
            .unwrap();
        batch_storage.flush().unwrap();

        let payload = GetAsset {
            id: mint_key.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
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
            token_program: Default::default(),
            extensions: None,
            write_version: 2,
        };

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        token_updates_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint_acc)
            .unwrap();
        batch_storage.flush().unwrap();

        let payload = GetAsset {
            id: mint_key.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let token_updates_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        let mplx_updates_processor =
            MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

        let mut metadata_info = HashMap::new();
        let mut mint_accs = Vec::new();
        let mut token_accs = Vec::new();

        let token_standards = vec![
            mpl_token_metadata::types::TokenStandard::ProgrammableNonFungible,
            mpl_token_metadata::types::TokenStandard::ProgrammableNonFungibleEdition,
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
                extensions: None,
            };

            let mint_acc = Mint {
                pubkey: mint_key,
                slot_updated: 1,
                supply: 1,
                decimals: 0,
                mint_authority: Some(mint_auth_key),
                freeze_authority: None,
                write_version: 1,
                extensions: None,
                token_program: Default::default(),
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

            metadata_info.insert(mint_key, metadata);
            mint_accs.push(mint_acc);
            token_accs.push(token_acc);
        }

        let metadata = OffChainData {
            url: Some("https://ping-pong".to_string()),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            ..Default::default()
        };

        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata.url.clone().unwrap(), metadata)
            .unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        for mint in mint_accs.iter() {
            token_updates_processor
                .transform_and_save_mint_account(&mut batch_storage, mint)
                .unwrap();
        }
        for token_account in token_accs.iter() {
            token_updates_processor
                .transform_and_save_token_account(
                    &mut batch_storage,
                    token_account.pubkey,
                    token_account,
                )
                .unwrap();
        }
        for (key, metadata) in metadata_info.iter() {
            mplx_updates_processor
                .transform_and_store_metadata_account(&mut batch_storage, *key, metadata)
                .unwrap();
        }
        batch_storage.flush().unwrap();

        let payload = GetAsset {
            id: mint_accs[0].pubkey.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["id"], mint_accs[0].pubkey.to_string());
        assert_eq!(response["interface"], "ProgrammableNFT".to_string());

        let payload = GetAsset {
            id: mint_accs[1].pubkey.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let response = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();

        assert_eq!(response["id"], mint_accs[1].pubkey.to_string());
        assert_eq!(response["interface"], "ProgrammableNFT".to_string());

        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore = "FIXME: owner returned is non-empty"]
    async fn test_burnt() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let token_updates_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        let mplx_updates_processor =
            MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

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
            extensions: None,
        };

        let mint_acc = Mint {
            pubkey: mint_key,
            slot_updated: 1,
            supply: 1,
            decimals: 0,
            mint_authority: Some(mint_auth_key),
            freeze_authority: None,
            write_version: 1,
            extensions: None,
            token_program: Default::default(),
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
                token_standard: Some(mpl_token_metadata::types::TokenStandard::Fungible),
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
            url: Some("https://ping-pong".to_string()),
            metadata: Some("{\"msg\": \"hallo\"}".to_string()),
            ..Default::default()
        };

        let (metadata_key, _) = Pubkey::find_program_address(
            &[
                "metadata".as_ref(),
                blockbuster::programs::token_metadata::token_metadata_id().as_ref(),
                mint_key.as_ref(),
            ],
            &blockbuster::programs::token_metadata::token_metadata_id(),
        );

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        mplx_updates_processor
            .transform_and_store_metadata_account(&mut batch_storage, metadata_key, &metadata)
            .unwrap();
        // save metadata_mint map
        batch_storage.flush().unwrap();
        mplx_updates_processor
            .transform_and_store_burnt_metadata(
                &mut batch_storage,
                metadata_key,
                &BurntMetadataSlot { slot_updated: 2, write_version: 100 },
            )
            .unwrap();
        env.rocks_env
            .storage
            .asset_offchain_data
            .put(metadata_ofch.url.clone().unwrap(), metadata_ofch)
            .unwrap();

        token_updates_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint_acc)
            .unwrap();
        token_updates_processor
            .transform_and_save_token_account(&mut batch_storage, token_acc.pubkey, &token_acc)
            .unwrap();
        batch_storage.flush().unwrap();

        let payload = GetAsset {
            id: mint_key.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
                    AssetSignatureKey { tree: first_tree, leaf_idx: first_leaf_idx, seq },
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
                    AssetSignatureKey { tree: second_tree, leaf_idx: second_leaf_idx, seq },
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );

        let token_updates_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

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
                    extensions: None,
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
                    extensions: None,
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
                    extensions: None,
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
                    extensions: None,
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
                    extensions: None,
                },
            );
        }

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        for (key, token_account) in token_accounts.iter() {
            token_updates_processor
                .transform_and_save_token_account(&mut batch_storage, *key, &token_account)
                .unwrap();
        }
        batch_storage.flush().unwrap();

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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
            after: None,
            before: None,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let parsed_response: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(parsed_response.token_accounts.len(), first_owner_with_mint_count);

        let payload = GetTokenAccounts {
            limit: None,
            page: None,
            owner: Some(second_owner.to_string()),
            mint: Some(first_mint.to_string()),
            options: Some(DisplayOptions { show_zero_balance: true }),
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );

        let token_updates_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

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
                    extensions: None,
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
                    extensions: None,
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
                    extensions: None,
                },
            );
        }

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        for (key, token_account) in token_accounts.iter() {
            token_updates_processor
                .transform_and_save_token_account(&mut batch_storage, *key, &token_account)
                .unwrap();
        }
        batch_storage.flush().unwrap();

        check_pagination(&api, Some(first_owner.to_string()), None).await;

        check_pagination(&api, Some(third_owner.to_string()), Some(mint.to_string())).await;

        check_pagination(&api, None, Some(mint.to_string())).await;
    }

    async fn check_pagination(
        api: &nft_ingester::api::api_impl::DasApi<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >,
        owner: Option<String>,
        mint: Option<String>,
    ) {
        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
            // it's safe to do it in test because we want to check how reverse work
            after: first_10.cursor.clone(),
            before: first_20.cursor,
            cursor: None,
        };
        let response = api.get_token_accounts(payload).await.unwrap();
        let first_10_before_after: TokenAccountsList = serde_json::from_value(response).unwrap();

        assert_eq!(first_10_before_after.token_accounts, second_10.token_accounts);

        let payload = GetTokenAccounts {
            limit: Some(10),
            page: None,
            owner: owner.clone(),
            mint: mint.clone(),
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
            options: Some(DisplayOptions { show_zero_balance: true }),
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));

        let ref_value = generated_assets.owners[8].clone();
        let payload = GetAssetsByOwner {
            owner_address: ref_value.owner.value.map(|owner| owner.to_string()).unwrap_or_default(),
            sort_by: None,
            limit: None,
            page: None,
            before: None,
            after: None,
            cursor: None,
            options: GetByMethodsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
        };
        let res = api.get_assets_by_owner(payload, mutexed_tasks.clone()).await.unwrap();
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
            options: GetByMethodsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
        };
        let res = api.get_assets_by_group(payload, mutexed_tasks.clone()).await.unwrap();
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
            options: GetByMethodsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
        };
        let res = api.get_assets_by_creator(payload, mutexed_tasks.clone()).await.unwrap();
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
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
            options: GetByMethodsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
        };
        let res = api.get_assets_by_authority(payload, mutexed_tasks.clone()).await.unwrap();
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
        mock_middleware.expect_download_file().with(predicate::eq(url)).times(1).returning(
            move |_| {
                Ok(interface::json::JsonDownloadResult::JsonContent(offchain_data.to_string()))
            },
        );

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            MockJsonDownloader,
            MockJsonPersister,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            Some(Arc::new(mock_middleware)),
            None,
            JsonMiddlewareConfig { is_enabled: true, max_urls_to_parse: 10 },
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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

        let asset_authority =
            AssetAuthority { pubkey: pb, authority, slot_updated: 12, write_version: Some(1) };

        let owner = AssetOwner {
            pubkey: pb,
            owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(authority)),
            delegate: Updated::new(12, Some(UpdateVersion::Sequence(12)), None),
            owner_type: Updated::new(12, Some(UpdateVersion::Sequence(12)), OwnerType::Single),
            owner_delegate_seq: Updated::new(12, Some(UpdateVersion::Sequence(12)), Some(12)),
            is_current_owner: Updated::new(12, Some(UpdateVersion::Sequence(12)), true),
        };
        let asset_complete_details = AssetCompleteDetails {
            pubkey: pb,
            static_details: Some(asset_static_details),
            dynamic_details: Some(dynamic_details),
            authority: Some(asset_authority),
            owner: Some(owner),
            ..Default::default()
        };
        env.rocks_env
            .storage
            .db
            .put_cf(
                &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                pb,
                asset_complete_details.convert_to_fb_bytes(),
            )
            .unwrap();

        let payload = GetAsset {
            id: pb.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
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
        assert_eq!(response["content"]["metadata"]["token_standard"], "NonFungible");
        assert_eq!(response["content"]["json_uri"], json_uri);

        assert_eq!(response["content"], expected_content);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_cannot_service_gaped_tree() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            Some(env.rocks_env.storage.clone()),
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
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
        env.rocks_env.storage.trees_gaps.put_async(tree_id, TreesGaps {}).await.unwrap();
        let payload = GetAssetProof { id: asset_id.to_string() };
        let res = api.get_asset_proof(payload).await.err().unwrap();
        assert!(matches!(res, DasApiError::CannotServiceRequest));
    }

    #[tokio::test]
    async fn test_core_account_fees() {
        let cnt = 20;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let asset_fees_count = 1000;
        let mut asset_ids = Vec::with_capacity(asset_fees_count);
        for _ in 0..asset_fees_count {
            asset_ids.push(Pubkey::new_unique())
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO core_fees (
              fee_pubkey
            ) ",
        );
        query_builder.push_values(asset_ids, |mut builder, id| {
            builder.push_bind(id.to_bytes().to_vec());
        });
        query_builder.build().execute(&env.pg_env.pool).await.unwrap();

        let payload =
            GetCoreFees { limit: Some(50), page: Some(1), before: None, after: None, cursor: None };
        let res = api.get_core_fees(payload).await.unwrap();
        let res: CoreFeesAccountsList = serde_json::from_value(res).unwrap();
        assert_eq!(res.core_fees_account.len(), 50);

        let payload =
            GetCoreFees { limit: Some(975), page: None, before: None, after: None, cursor: None };
        let res = api.get_core_fees(payload).await.unwrap();
        let res: CoreFeesAccountsList = serde_json::from_value(res).unwrap();
        assert_eq!(res.core_fees_account.len(), 975);

        let payload =
            GetCoreFees { limit: None, page: None, before: None, after: None, cursor: res.cursor };
        let res = api.get_core_fees(payload).await.unwrap();
        let res: CoreFeesAccountsList = serde_json::from_value(res).unwrap();
        assert_eq!(res.core_fees_account.len(), 25);

        let payload =
            GetCoreFees { limit: None, page: None, before: None, after: None, cursor: res.cursor };
        let res = api.get_core_fees(payload).await.unwrap();
        let res: CoreFeesAccountsList = serde_json::from_value(res).unwrap();
        assert_eq!(res.core_fees_account.len(), 0);
    }

    #[tokio::test]
    async fn test_search_assets_grand_total() {
        let total_assets = 2000;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, total_assets, SLOT_UPDATED).await;
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                show_grand_total: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();
        assert_eq!(res.grand_total, Some(total_assets as u32));

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            options: SearchAssetsOptions {
                show_unverified_collections: false,
                show_grand_total: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();
        assert_eq!(res.grand_total, Some(0));

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();
        assert_eq!(res.grand_total, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_balance() {
        let cnt = 0;
        let cli = Cli::default();
        let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let solana_price_updater = SolanaPriceUpdater::new(
            env.rocks_env.storage.clone(),
            CoinGeckoPriceFetcher::new(),
            30,
        );
        solana_price_updater.update_price().await.unwrap();
        let mut mock_account_balance_getter = MockAccountBalanceGetter::new();
        mock_account_balance_getter
            .expect_get_account_balance_lamports()
            .returning(move |_| Ok(10_u64.pow(9)));
        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(mock_account_balance_getter),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(Pubkey::new_unique().to_string()),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                show_native_balance: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();
        assert!(res.native_balance.unwrap().total_price > 0.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collection_metadata() {
        let cnt = 100;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        generated_assets.collections.iter().for_each(|collection| {
            env.rocks_env
                .storage
                .db
                .put_cf(
                    &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                    collection.collection.value,
                    AssetCompleteDetails {
                        pubkey: collection.collection.value,
                        dynamic_details: Some(AssetDynamicDetails {
                            pubkey: collection.collection.value,
                            url: Updated::new(
                                100,
                                Some(UpdateVersion::Sequence(100)),
                                "http://example.com".to_string(),
                            ),
                            onchain_data: Some(Updated::new(
                                100,
                                Some(UpdateVersion::Sequence(100)),
                                "{
                            \"name\": \"WIF Drop\",
                            \"symbol\": \"6WIF\"\
                         }"
                                .to_string(),
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                    .convert_to_fb_bytes(),
                )
                .expect("insert asset complete details");
        });
        let o = env.rocks_env.storage.asset_offchain_data.put_async(
            "http://example.com".to_string(),
            OffChainData {
                url: Some("http://example.com".to_string()),
                metadata: Some(
                    "{
                      \"name\": \"WIF Drop\",
                      \"symbol\": \"6WIF\",
                      \"description\": \"Random Drop event! https://3000wif.com\",
                      \"seller_fee_basis_points\": 0,
                      \"image\": \"https://img.hi-hi.vip/json/img/3000wif.png\",
                      \"attributes\": [
                        {
                          \"trait_type\": \"Website\",
                          \"value\": \"https://3000wif.com\"
                        },
                        {
                          \"trait_type\": \"Verified\",
                          \"value\": \"True\"
                        },
                        {
                          \"trait_type\": \"Amount\",
                          \"value\": \"3,000+ WIF\"
                        },
                        {
                          \"trait_type\": \"Time Left\",
                          \"value\": \"25 minutes!\"
                        }
                      ],
                        \"external_url\": \"https://3000wif.com\",
                      \"properties\": {
                        \"creators\": [
                          {
                            \"address\": \"Gx31E1GUzEeqjCHPtyLkNQHCqwi6REpyp66Hi262vCtG\",
                            \"share\": 100
                          }
                        ]
                      }
                    }"
                    .to_string(),
                ),
                ..Default::default()
            },
        );
        o.await.unwrap();

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = GetAsset {
            id: generated_assets.collections.first().unwrap().pubkey.to_string(),
            options: Options {
                show_unverified_collections: true,
                show_collection_metadata: true,
                ..Default::default()
            },
        };
        let res = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
        let res: Asset = serde_json::from_value(res).unwrap();

        assert_eq!(
            res.grouping
                .clone()
                .unwrap()
                .first()
                .unwrap()
                .collection_metadata
                .clone()
                .unwrap()
                .get_item("description")
                .unwrap()
                .to_string(),
            "\"Random Drop event! https://3000wif.com\"".to_string()
        );
        assert_eq!(
            res.grouping
                .clone()
                .unwrap()
                .first()
                .unwrap()
                .collection_metadata
                .clone()
                .unwrap()
                .get_item("external_url")
                .unwrap()
                .to_string(),
            "\"https://3000wif.com\"".to_string()
        );
        assert_eq!(
            res.grouping
                .clone()
                .unwrap()
                .first()
                .unwrap()
                .collection_metadata
                .clone()
                .unwrap()
                .get_item("image")
                .unwrap()
                .to_string(),
            "\"https://img.hi-hi.vip/json/img/3000wif.png\"".to_string()
        );
        assert_eq!(
            res.grouping
                .clone()
                .unwrap()
                .first()
                .unwrap()
                .collection_metadata
                .clone()
                .unwrap()
                .get_item("name")
                .unwrap()
                .to_string(),
            "\"WIF Drop\"".to_string()
        );
        assert_eq!(
            res.grouping
                .clone()
                .unwrap()
                .first()
                .unwrap()
                .collection_metadata
                .clone()
                .unwrap()
                .get_item("symbol")
                .unwrap()
                .to_string(),
            "\"6WIF\"".to_string()
        );

        let payload = GetAsset {
            id: generated_assets.collections.first().unwrap().pubkey.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let res = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
        let res: Asset = serde_json::from_value(res).unwrap();

        assert!(res.grouping.clone().unwrap().first().unwrap().collection_metadata.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_inscriptions() {
        let inscription_data_pk =
            Pubkey::from_str("GKcyym3BLXizQJWfH8H3YA5wLJTYVHrNMoGzbCsHtogn").unwrap();
        let cnt = 10;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let asset_pk = generated_assets.static_details.first().unwrap().pubkey;
        env.rocks_env
            .storage
            .inscriptions
            .put(
                asset_pk,
                Inscription {
                    authority: Pubkey::default(),
                    root: asset_pk,
                    content_type: "application/text".to_string(),
                    encoding: "base64".to_string(),
                    inscription_data_account: inscription_data_pk,
                    order: 308243,
                    size: 67,
                    validation_hash: Some(
                        "f5320ef28d5375d7b1a6ae70ec43edd115fd12ea933e0165306a788dcbeeca31"
                            .to_string(),
                    ),
                    write_version: 100,
                },
            )
            .unwrap();
        env.rocks_env.storage.inscription_data.put(inscription_data_pk, InscriptionData {
            pubkey: inscription_data_pk,
            data: general_purpose::STANDARD.decode("eyJwIjoic3BsLTIwIiwib3AiOiJkZXBsb3kiLCJ0aWNrIjoiaGVsaXVzIiwibWF4IjoiMTAwMCIsImxpbSI6IjEifQ==").unwrap(),
            write_version: 1000,
        }).unwrap();

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = GetAsset {
            id: asset_pk.to_string(),
            options: Options {
                show_unverified_collections: true,
                show_inscription: true,
                ..Default::default()
            },
        };
        let res = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
        let res: Asset = serde_json::from_value(res).unwrap();
        assert_eq!(
            res.inscription.unwrap().validation_hash.unwrap(),
            "f5320ef28d5375d7b1a6ae70ec43edd115fd12ea933e0165306a788dcbeeca31"
        );
        assert_eq!(res.spl20.unwrap().get("op").unwrap().as_str().unwrap(), "deploy");

        let payload = GetAsset {
            id: asset_pk.to_string(),
            options: Options {
                show_unverified_collections: true,
                show_inscription: false,
                ..Default::default()
            },
        };
        let res = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
        let res: Asset = serde_json::from_value(res).unwrap();
        assert_eq!(res.inscription, None);
        assert_eq!(res.spl20, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_token_type() {
        let cnt = 100;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

        let synchronizer = nft_ingester::index_syncronizer::Synchronizer::new(
            env.rocks_env.storage.clone(),
            env.pg_env.client.clone(),
            200_000,
            "".to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
            1,
        );
        let fungible_token_mint1 = Pubkey::new_unique(); // non-existed token
        let fungible_token_mint2 =
            Pubkey::from_str("METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m").unwrap(); // MPLX token
        let mint1 = Mint {
            pubkey: fungible_token_mint1,
            supply: 100000,
            decimals: 2,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 10,
            write_version: 10,
            extensions: None,
        };
        let mint2 = Mint {
            pubkey: fungible_token_mint2,
            supply: 100000,
            decimals: 2,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 7,
            write_version: 10,
            extensions: None,
        };

        let owner = generated_assets.owners[50].owner.value.unwrap();
        let fungible_token_account1 = Pubkey::new_unique();
        let fungible_token_account2 = Pubkey::new_unique();
        let token_account1 = TokenAccount {
            pubkey: fungible_token_account1,
            mint: fungible_token_mint1,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 10,
            amount: 0,
            write_version: 10,
        };
        let token_account2 = TokenAccount {
            pubkey: fungible_token_account2,
            mint: fungible_token_mint2,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 10,
            amount: 30000,
            write_version: 10,
        };

        let ftm_complete1 = AssetCompleteDetails {
            pubkey: fungible_token_mint1,
            static_details: Some(AssetStaticDetails {
                pubkey: fungible_token_mint1,
                specification_asset_class: SpecificationAssetClass::FungibleAsset,
                royalty_target_type: RoyaltyTargetType::Single,
                created_at: 10,
                edition_address: None,
            }),
            owner: Some(AssetOwner {
                pubkey: fungible_token_mint1,
                owner: Updated::new(10, Some(UpdateVersion::WriteVersion(10)), None),
                delegate: Default::default(),
                owner_type: Default::default(),
                owner_delegate_seq: Default::default(),
                is_current_owner: Default::default(),
            }),
            ..Default::default()
        };

        let ftm_complete2 = AssetCompleteDetails {
            pubkey: fungible_token_mint2,
            static_details: Some(AssetStaticDetails {
                pubkey: fungible_token_mint2,
                specification_asset_class: SpecificationAssetClass::FungibleAsset,
                royalty_target_type: RoyaltyTargetType::Single,
                created_at: 10,
                edition_address: None,
            }),
            owner: Some(AssetOwner {
                pubkey: fungible_token_mint2,
                owner: Updated::new(10, Some(UpdateVersion::WriteVersion(10)), None),
                delegate: Default::default(),
                owner_type: Default::default(),
                owner_delegate_seq: Default::default(),
                is_current_owner: Default::default(),
            }),
            ..Default::default()
        };

        env.rocks_env
            .storage
            .db
            .put_cf(
                &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                fungible_token_mint1,
                ftm_complete1.convert_to_fb_bytes(),
            )
            .unwrap();

        env.rocks_env
            .storage
            .db
            .put_cf(
                &env.rocks_env.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                fungible_token_mint2,
                ftm_complete2.convert_to_fb_bytes(),
            )
            .unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        token_accounts_processor
            .transform_and_save_fungible_token_account(
                &mut batch_storage,
                fungible_token_account1,
                &token_account1,
            )
            .unwrap();
        token_accounts_processor
            .transform_and_save_fungible_token_account(
                &mut batch_storage,
                fungible_token_account2,
                &token_account2,
            )
            .unwrap();
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint1)
            .unwrap();
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint2)
            .unwrap();
        batch_storage.flush().unwrap();
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);

        let synchronizer = Arc::new(synchronizer);
        let mut tasks = JoinSet::new();

        for asset_type in ASSET_TYPES {
            let rx = rx.resubscribe();
            let synchronizer = synchronizer.clone();
            match asset_type {
                AssetType::Fungible => {
                    tasks.spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await
                    });
                },
                AssetType::NonFungible => {
                    tasks.spawn(
                        async move { synchronizer.synchronize_nft_asset_indexes(&rx, 0).await },
                    );
                },
            }
        }

        while let Some(res) = tasks.join_next().await {
            if let Err(err) = res {
                panic!("{err}");
            }
        }

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_fungible: true,
                show_zero_balance: true,
                show_unverified_collections: true,
                ..Default::default()
            },
            token_type: Some(TokenType::Fungible),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // We created 2 fungible tokens^ 1 with real pubkey (MPLX)
        // so this token contain info about symbol and price
        // and 1 non-existed token, so response for it do not include such info
        assert_eq!(res.items.len(), 2);
        assert_eq!(res.items[0].clone().token_info.unwrap().symbol.unwrap(), "MPLX".to_string());
        assert_eq!(
            res.items[0].clone().token_info.unwrap().associated_token_address.unwrap(),
            fungible_token_account2.to_string()
        );
        assert_eq!(
            res.items[0].clone().token_info.unwrap().price_info.unwrap().currency.unwrap(),
            "USDC".to_string()
        );
        assert!(
            res.items[0].clone().token_info.unwrap().price_info.unwrap().total_price.unwrap() > 0.0
        );
        assert!(
            res.items[0].clone().token_info.unwrap().price_info.unwrap().price_per_token.unwrap()
                > 0.0
        );

        assert!(res.items[1].clone().token_info.unwrap().symbol.is_none());
        assert!(res.items[1].clone().token_info.unwrap().price_info.is_none());
        assert_eq!(
            res.items[1].clone().token_info.unwrap().associated_token_address.unwrap(),
            fungible_token_account1.to_string()
        );

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
            token_type: Some(TokenType::NonFungible),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // We have 1 NonFungible token, created in setup::TestEnvironment::create fn
        assert_eq!(res.items.len(), 1);
        assert!(res.items[0].token_info.is_none());

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
            token_type: Some(TokenType::CompressedNFT),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // Our NonFungible token is not compressed
        assert!(res.items.is_empty());

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                ..Default::default()
            },
            token_type: Some(TokenType::RegularNFT),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // Our NonFungible token is not compressed
        assert_eq!(res.items.len(), 1);
        assert!(res.items[0].token_info.is_none());

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_unverified_collections: true,
                show_zero_balance: true,
                ..Default::default()
            },
            token_type: Some(TokenType::All),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // Totally we have 3 assets with required owner. show_fungible is false by default, so we don't have token info.
        assert_eq!(res.items.len(), 3);
        assert!(res.items[0].mint_extensions.is_none());
        assert!(res.items[0].clone().token_info.unwrap().symbol.is_none());
        assert!(res.items[0].clone().token_info.unwrap().price_info.is_none());

        assert!(res.items[1].mint_extensions.is_none());
        assert!(res.items[1].clone().token_info.map_or(true, |info| info.symbol.is_none()));
        assert!(res.items[1].clone().token_info.map_or(true, |info| info.price_info.is_none()));

        assert!(res.items[2].mint_extensions.is_none());
        assert!(res.items[2].clone().token_info.map_or(true, |info| info.symbol.is_none()));
        assert!(res.items[2].clone().token_info.map_or(true, |info| info.price_info.is_none()));

        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_zero_balance: false,
                show_unverified_collections: true,
                ..Default::default()
            },
            token_type: Some(TokenType::Fungible),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // We have only 1 token account with non-zero balance
        assert_eq!(res.items.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mint_extentions() {
        let cnt = 100;
        let cli = Cli::default();
        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;

        let fungible_token_mint1 = generated_assets.pubkeys[0]; // non-existed token
        let mint1 = Mint {
            pubkey: fungible_token_mint1,
            supply: 100000,
            decimals: 2,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 10,
            write_version: 10,
            extensions: Some(MintAccountExtensions {
                default_account_state: None,
                confidential_transfer_mint: None,
                confidential_transfer_account: None,
                confidential_transfer_fee_config: None,
                interest_bearing_config: Some(ShadowInterestBearingConfig {
                    rate_authority: OptionalNonZeroPubkey::try_from(Some(
                        Pubkey::from_str("4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY").unwrap(),
                    ))
                    .unwrap(),
                    initialization_timestamp: UnixTimestamp::from(1699797096),
                    pre_update_average_rate: BasisPoints::from(2),
                    last_update_timestamp: UnixTimestamp::from(1699797096),
                    current_rate: BasisPoints::from(2),
                }),
                transfer_fee_config: Some(ShadowTransferFeeConfig {
                    transfer_fee_config_authority: OptionalNonZeroPubkey::try_from(Some(
                        Pubkey::from_str("4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY").unwrap(),
                    ))
                    .unwrap(),
                    withdraw_withheld_authority: OptionalNonZeroPubkey::try_from(Some(
                        Pubkey::from_str("4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY").unwrap(),
                    ))
                    .unwrap(),
                    withheld_amount: PodU64::from(0),
                    older_transfer_fee: ShadowTransferFee {
                        epoch: PodU64::from(531),
                        maximum_fee: PodU64::from(1000000000000),
                        transfer_fee_basis_points: PodU16::from(300),
                    },
                    newer_transfer_fee: ShadowTransferFee {
                        epoch: PodU64::from(531),
                        maximum_fee: PodU64::from(1000000000000),
                        transfer_fee_basis_points: PodU16::from(300),
                    },
                }),
                mint_close_authority: None,
                permanent_delegate: None,
                metadata_pointer: None,
                metadata: None,
                transfer_hook: None,
                group_pointer: None,
                token_group: None,
                group_member_pointer: None,
                token_group_member: None,
            }),
        };

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint1)
            .unwrap();
        batch_storage.flush().unwrap();

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = GetAsset {
            id: fungible_token_mint1.to_string(),
            options: Options { show_unverified_collections: true, ..Default::default() },
        };
        let res = api.get_asset(payload, mutexed_tasks.clone()).await.unwrap();
        let res: Asset = serde_json::from_value(res).unwrap();

        let reference = json!({
            "transfer_fee_config": {
                "withheld_amount": 0,
                "newer_transfer_fee": {
                    "epoch": 531,
                    "maximum_fee": 1000000000000i64,
                    "transfer_fee_basis_points": 300
                },
                "older_transfer_fee": {
                    "epoch": 531,
                    "maximum_fee": 1000000000000i64,
                    "transfer_fee_basis_points": 300
                },
                "withdraw_withheld_authority": "4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY",
                "transfer_fee_config_authority": "4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY"
            },
            "interest_bearing_config": {
                "current_rate": 2,
                "rate_authority": "4TM4eaLauigrqjDytC49K2iWKB4kAHsNRCJz48mdriHY",
                "last_update_timestamp": 1699797096,
                "pre_update_average_rate": 2,
                "initialization_timestamp": 1699797096
            }
        });
        assert_eq!(res.mint_extensions.unwrap(), reference)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_writing_fungible_into_dedicated_table() {
        let cnt = 100;
        let cli = Cli::default();

        let (env, generated_assets) = setup::TestEnvironment::create_and_setup_from_closures(
            &cli,
            cnt,
            100,
            RocksTestEnvironmentSetup::static_data_for_fungible,
            RocksTestEnvironmentSetup::with_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_without_authority,
        )
        .await;

        let synchronizer = nft_ingester::index_syncronizer::Synchronizer::new(
            env.rocks_env.storage.clone(),
            env.pg_env.client.clone(),
            200_000,
            "".to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
            1,
        );
        let fungible_token_mint1 = generated_assets.pubkeys[0]; // non-existed token
        let fungible_token_mint2 = generated_assets.pubkeys[1]; // MPLX token
        let mint1 = Mint {
            pubkey: fungible_token_mint1,
            supply: 100000,
            decimals: 2,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 10,
            write_version: 10,
            extensions: None,
        };
        let mint2 = Mint {
            pubkey: fungible_token_mint2,
            supply: 100000,
            decimals: 2,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 7,
            write_version: 10,
            extensions: None,
        };

        let owner = generated_assets.owners[50].owner.value.unwrap();
        let fungible_token_account1 = Pubkey::new_unique();
        let fungible_token_account2 = Pubkey::new_unique();
        let token_account1 = TokenAccount {
            pubkey: fungible_token_account1,
            mint: fungible_token_mint1,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 10,
            amount: 0,
            write_version: 10,
        };
        let token_account2 = TokenAccount {
            pubkey: fungible_token_account2,
            mint: fungible_token_mint2,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 10,
            amount: 30000,
            write_version: 10,
        };
        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        token_accounts_processor
            .transform_and_save_fungible_token_account(
                &mut batch_storage,
                fungible_token_account1,
                &token_account1,
            )
            .unwrap();
        token_accounts_processor
            .transform_and_save_fungible_token_account(
                &mut batch_storage,
                fungible_token_account2,
                &token_account2,
            )
            .unwrap();
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint1)
            .unwrap();
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint2)
            .unwrap();
        batch_storage.flush().unwrap();

        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);

        let synchronizer = Arc::new(synchronizer);
        let mut tasks = JoinSet::new();

        for asset_type in ASSET_TYPES {
            let rx = rx.resubscribe();
            let synchronizer = synchronizer.clone();
            match asset_type {
                AssetType::Fungible => {
                    tasks.spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await
                    });
                },
                AssetType::NonFungible => {
                    tasks.spawn(
                        async move { synchronizer.synchronize_nft_asset_indexes(&rx, 0).await },
                    );
                },
            }
        }

        while let Some(res) = tasks.join_next().await {
            if let Err(err) = res {
                panic!("{err}");
            }
        }

        let api = nft_ingester::api::api_impl::DasApi::<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            MockAccountBalanceGetter,
            RaydiumTokenPriceFetcher,
            Storage,
        >::new(
            env.pg_env.client.clone(),
            env.rocks_env.storage.clone(),
            Arc::new(ApiMetricsConfig::new()),
            None,
            None,
            50,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(MockAccountBalanceGetter::new()),
            None,
            Arc::new(RaydiumTokenPriceFetcher::default()),
            NATIVE_MINT_PUBKEY.to_string(),
        );
        let tasks = JoinSet::new();
        let mutexed_tasks = Arc::new(Mutex::new(tasks));
        let payload = SearchAssets {
            limit: Some(1000),
            page: Some(1),
            owner_address: Some(owner.to_string()),
            options: SearchAssetsOptions {
                show_zero_balance: true,
                show_unverified_collections: true,
                ..Default::default()
            },
            // They will be stored as non-fungible tokens
            // because they didn't get into the list of popular fungible tokens
            token_type: Some(TokenType::Fungible),
            ..Default::default()
        };
        let res = api.search_assets(payload, mutexed_tasks.clone()).await.unwrap();
        let res: AssetList = serde_json::from_value(res).unwrap();

        // We created 2 fungible tokens^ 1 with real pubkey (MPLX)
        // so this token contain info about symbol and price
        // and 1 non-existed token, so response for it do not include such info
        assert_eq!(res.items.len(), 2);
        assert_eq!(
            res.items[0].clone().token_info.unwrap().associated_token_address.unwrap(),
            fungible_token_account2.to_string()
        );
        assert_eq!(
            res.items[1].clone().token_info.unwrap().associated_token_address.unwrap(),
            fungible_token_account1.to_string()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "FIXME: non fungible count is not reset"]
    async fn test_idx_cleaner() {
        let cnt = 100;
        let cli = Cli::default();

        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, 100).await;
        let _synchronizer = nft_ingester::index_syncronizer::Synchronizer::new(
            env.rocks_env.storage.clone(),
            env.pg_env.client.clone(),
            200_000,
            "./tmp/test_idx_cleaner_dump".to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
            1,
        );
        let non_fungible_token_mint = generated_assets.pubkeys[1];
        let mint = Mint {
            pubkey: non_fungible_token_mint,
            supply: 100000,
            decimals: 0,
            mint_authority: None,
            freeze_authority: None,
            token_program: Default::default(),
            slot_updated: 10,
            write_version: 10,
            extensions: None,
        };

        let owner: Pubkey = generated_assets.owners[1].owner.value.unwrap();
        let token_account_addr = Pubkey::new_unique();
        let token_account = TokenAccount {
            pubkey: token_account_addr,
            mint: non_fungible_token_mint,
            delegate: None,
            owner,
            extensions: None,
            frozen: false,
            delegated_amount: 0,
            slot_updated: 10,
            amount: 100,
            write_version: 10,
        };
        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        token_accounts_processor
            .transform_and_save_token_account(
                &mut batch_storage,
                token_account_addr,
                &token_account,
            )
            .unwrap();
        token_accounts_processor
            .transform_and_save_mint_account(&mut batch_storage, &mint)
            .unwrap();
        batch_storage.flush().unwrap();

        // one more idx shoul've been added
        let idx_fungible_asset_iter = env.rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = env.rocks_env.storage.assets_update_idx.iter_start();
        assert_eq!(idx_fungible_asset_iter.count(), 1);
        assert_eq!(idx_non_fungible_asset_iter.count(), cnt + 2);

        for asset_type in ASSET_TYPES {
            clean_syncronized_idxs(
                env.pg_env.client.clone(),
                env.rocks_env.storage.clone(),
                asset_type,
            )
            .await
            .unwrap();
        }

        // after sync idxs should be cleaned again
        let idx_fungible_asset_iter = env.rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = env.rocks_env.storage.assets_update_idx.iter_start();
        assert_eq!(idx_fungible_asset_iter.count(), 1);
        assert_eq!(idx_non_fungible_asset_iter.count(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_idx_cleaner_does_not_erase_updates() {
        let cnt = 1;
        let slot = 0;
        let cli = Cli::default();

        let (env, generated_assets) = setup::TestEnvironment::create(&cli, cnt, slot).await;
        let nft_token_mint = generated_assets.pubkeys[0];
        let owner: Pubkey = generated_assets.owners[0].owner.value.unwrap();

        let mut batch_storage = BatchSaveStorage::new(
            env.rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        let token_account_addr = Pubkey::new_unique();

        // receive 10 updates for the asset
        for i in 0..10 {
            let token_account = TokenAccount {
                pubkey: token_account_addr,
                mint: nft_token_mint,
                delegate: None,
                owner,
                extensions: None,
                frozen: false,
                delegated_amount: 0,
                slot_updated: i,
                amount: 100 + i,
                write_version: i as u64,
            };
            token_accounts_processor
                .transform_and_save_token_account(
                    &mut batch_storage,
                    token_account_addr,
                    &token_account,
                )
                .unwrap();
            batch_storage.flush().unwrap();
        }

        let idx_fungible_asset_iter = env.rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = env.rocks_env.storage.assets_update_idx.iter_start();
        // 10 fungibles because we have 10 updates and created token account is not counted as fungible, then
        // it will be a part of both indexes
        assert_eq!(idx_fungible_asset_iter.count(), 10);
        assert_eq!(idx_non_fungible_asset_iter.count(), 11);

        // no data was syncronized, then no data should be erased
        for asset_type in ASSET_TYPES {
            clean_syncronized_idxs(
                env.pg_env.client.clone(),
                env.rocks_env.storage.clone(),
                asset_type,
            )
            .await
            .unwrap();
        }

        // after sync idxs should be cleaned again
        let idx_fungible_asset_iter = env.rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = env.rocks_env.storage.assets_update_idx.iter_start();
        // 10 fungibles because we have 10 updates and created token account is not counted as fungible, then
        // it will be a part of both indexes
        assert_eq!(idx_fungible_asset_iter.count(), 10);
        assert_eq!(idx_non_fungible_asset_iter.count(), 11);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_idx_cleaner_erases_updates_partially() {
        let rocks_env: RocksTestEnvironment = RocksTestEnvironment::new(&[]);
        let number_of_assets = 1;
        let slot = 0;
        let generated_assets = rocks_env.generate_assets(number_of_assets, slot).await;
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let temp_dir_path = temp_dir.path();

        let cli: Cli = Cli::default();
        let pg_env =
            setup::pg::TestEnvironment::new_with_mount(&cli, temp_dir_path.to_str().unwrap()).await;
        let syncronizer = Arc::new(nft_ingester::index_syncronizer::Synchronizer::new(
            rocks_env.storage.clone(),
            pg_env.client.clone(),
            10,
            temp_dir_path.to_str().unwrap().to_string(),
            Arc::new(SynchronizerMetricsConfig::new()),
            1,
        ));

        let nft_token_mint = generated_assets.pubkeys[0];
        let owner: Pubkey = generated_assets.owners[0].owner.value.unwrap();
        let mut batch_storage = BatchSaveStorage::new(
            rocks_env.storage.clone(),
            10,
            Arc::new(IngesterMetricsConfig::new()),
        );
        let token_accounts_processor =
            TokenAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));
        let token_account_addr = Pubkey::new_unique();

        // receive 5 updates for the assset and update idxs accordingly
        for i in 0..5 {
            let token_account = TokenAccount {
                pubkey: token_account_addr,
                mint: nft_token_mint,
                delegate: None,
                owner,
                extensions: None,
                frozen: false,
                delegated_amount: 0,
                slot_updated: i,
                amount: 100 + i,
                write_version: i as u64,
            };
            token_accounts_processor
                .transform_and_save_token_account(
                    &mut batch_storage,
                    token_account_addr,
                    &token_account,
                )
                .unwrap();
            batch_storage.flush().unwrap();
        }
        let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        for asset_type in ASSET_TYPES {
            syncronizer.full_syncronize(&rx, asset_type).await.unwrap();
        }

        // receive 5 more updates for the same asset
        for i in 0..5 {
            let token_account = TokenAccount {
                pubkey: token_account_addr,
                mint: nft_token_mint,
                delegate: None,
                owner,
                extensions: None,
                frozen: false,
                delegated_amount: 0,
                slot_updated: i,
                amount: 100 + i,
                write_version: i as u64,
            };
            token_accounts_processor
                .transform_and_save_token_account(
                    &mut batch_storage,
                    token_account_addr,
                    &token_account,
                )
                .unwrap();
            batch_storage.flush().unwrap();
        }

        // full story of idxs is stored
        let idx_fungible_asset_iter = rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = rocks_env.storage.assets_update_idx.iter_start();
        assert_eq!(idx_fungible_asset_iter.count(), 10);
        assert_eq!(idx_non_fungible_asset_iter.count(), 11);

        for asset_type in ASSET_TYPES {
            clean_syncronized_idxs(pg_env.client.clone(), rocks_env.storage.clone(), asset_type)
                .await
                .unwrap();
        }

        // after sync idxs should be half cleaned
        let idx_fungible_asset_iter = rocks_env.storage.fungible_assets_update_idx.iter_start();
        let idx_non_fungible_asset_iter = rocks_env.storage.assets_update_idx.iter_start();
        assert_eq!(idx_fungible_asset_iter.count(), 6);
        assert_eq!(idx_non_fungible_asset_iter.count(), 6);
    }
}
