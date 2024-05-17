#[cfg(feature = "integration_tests")]
#[cfg(test)]
mod tests {
    use entities::api_req_params::Options;
    use postgre_client::model::*;
    use postgre_client::storage_traits::{AssetIndexStorage, AssetPubkeyFilteredFetcher};
    use setup::pg::*;
    use testcontainers::clients::Cli;
    use tokio;

    #[tokio::test]
    async fn test_get_asset_pubkeys_filtered_on_empty_db() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_filter_storage = &env.client;

        let filter = SearchAssetsFilter {
            specification_version: Some(SpecificationVersions::V1),
            specification_asset_class: Some(SpecificationAssetClass::Nft),
            owner_address: Some(generate_random_vec(32)),
            owner_type: Some(OwnerType::Single),
            creator_address: Some(generate_random_vec(32)),
            creator_verified: Some(true),
            authority_address: Some(generate_random_vec(32)),
            collection: Some(generate_random_vec(32)),
            delegate: Some(generate_random_vec(32)),
            frozen: Some(false),
            supply: Some(AssetSupply::Equal(1)),
            supply_mint: Some(generate_random_vec(32)),
            compressed: Some(false),
            compressible: Some(false),
            royalty_target_type: Some(RoyaltyTargetType::Creators),
            royalty_target: Some(generate_random_vec(32)),
            royalty_amount: Some(10),
            burnt: Some(false),
            json_uri: Some("https://www.google.com".to_string()),
        };
        let order = AssetSorting {
            sort_by: AssetSortBy::SlotCreated,
            sort_direction: AssetSortDirection::Asc,
        };
        let limit = 10;
        let page = Some(0);
        let before = Some("nonb64string".to_string());
        let after = Some("other_nonb64string".to_string());
        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(
                &filter,
                &order,
                limit,
                page,
                before,
                after,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_get_asset_pubkeys_filtered_on_filled_db() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_filter_storage = &env.client;

        // Generate random asset indexes
        let asset_indexes = generate_asset_index_records(100);
        let last_known_key = generate_random_vec(8 + 8 + 32);

        // Insert assets and last key using update_asset_indexes_batch
        asset_filter_storage
            .update_asset_indexes_batch(asset_indexes.as_slice())
            .await
            .unwrap();
        asset_filter_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();
        let ref_value = &asset_indexes[asset_indexes.len() - 1];
        let filter = SearchAssetsFilter {
            specification_version: Some(ref_value.specification_version.into()),
            specification_asset_class: Some(ref_value.specification_asset_class.into()),
            owner_address: ref_value.owner.map(|k| k.to_bytes().to_vec()),
            owner_type: ref_value.owner_type.map(|k| k.into()),
            creator_address: Some(ref_value.creators[0].creator.to_bytes().to_vec()),
            creator_verified: Some(ref_value.creators[0].creator_verified),
            authority_address: ref_value.authority.map(|k| k.to_bytes().to_vec()),
            collection: ref_value.collection.map(|k| k.to_bytes().to_vec()),
            delegate: ref_value.delegate.map(|k| k.to_bytes().to_vec()),
            frozen: Some(ref_value.is_frozen),
            supply: ref_value.supply.map(|s| AssetSupply::Equal(s as u64)),
            supply_mint: Some(ref_value.pubkey.to_bytes().to_vec()),
            compressed: Some(ref_value.is_compressed),
            compressible: Some(ref_value.is_compressible),
            royalty_target_type: Some(ref_value.royalty_target_type.into()),
            royalty_target: Some(ref_value.creators[0].creator.to_bytes().to_vec()),
            royalty_amount: Some(ref_value.royalty_amount as u32),
            burnt: Some(ref_value.is_burnt),
            json_uri: ref_value
                .metadata_url
                .clone()
                .map(|url_with_status| url_with_status.metadata_url),
        };
        let order: AssetSorting = AssetSorting {
            sort_by: AssetSortBy::SlotUpdated,
            sort_direction: AssetSortDirection::Asc,
        };
        let limit = 10;
        let page = Some(0);

        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(
                &filter,
                &order,
                limit,
                page,
                None,
                None,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        let filter = SearchAssetsFilter {
            specification_version: Some(ref_value.specification_version.into()),
            specification_asset_class: Some(ref_value.specification_asset_class.into()),
            owner_type: ref_value.owner_type.map(|k| k.into()),
            ..Default::default()
        };
        let limit = 5;
        for i in 0..20usize {
            let page = Some((i + 1) as u64);
            let res = asset_filter_storage
                .get_asset_pubkeys_filtered(
                    &filter,
                    &order,
                    limit,
                    page,
                    None,
                    None,
                    &Options {
                        show_unverified_collections: true,
                    },
                )
                .await
                .unwrap();
            assert_eq!(res.len(), 5);
            for j in 0..5usize {
                assert_eq!(
                    res[j].pubkey,
                    asset_indexes[i * 5 + j].pubkey.to_bytes(),
                    "i: {}, j: {}",
                    i,
                    j
                );
            }
        }
        let mut after: Option<String> = None;
        let mut last_before = None;
        let page = None;
        for i in 0..20usize {
            let res = asset_filter_storage
                .get_asset_pubkeys_filtered(
                    &filter,
                    &order,
                    limit,
                    page,
                    None,
                    after,
                    &Options {
                        show_unverified_collections: true,
                    },
                )
                .await
                .unwrap();
            assert_eq!(res.len(), 5);
            for j in 0..5usize {
                assert_eq!(res[j].pubkey, asset_indexes[i * 5 + j].pubkey.to_bytes());
            }
            let a = res[4].sorting_id.clone();
            after = Some(a);
            last_before = Some(res[0].sorting_id.clone());
        }
        let mut before = last_before.clone();
        for i in (0..19usize).rev() {
            let res = asset_filter_storage
                .get_asset_pubkeys_filtered(
                    &filter,
                    &order,
                    limit,
                    page,
                    before,
                    None,
                    &Options {
                        show_unverified_collections: true,
                    },
                )
                .await
                .unwrap();
            assert_eq!(res.len(), 5);
            for j in 0..5usize {
                assert_eq!(
                    res[j].pubkey,
                    asset_indexes[i * 5 + j].pubkey.to_bytes(),
                    "i: {}, j: {}",
                    i,
                    j
                );
            }
            let b = res[0].sorting_id.clone();
            before = Some(b);
        }
        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(
                &Default::default(),
                &order,
                limit,
                None,
                None,
                None,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), limit as usize);

        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(
                &SearchAssetsFilter {
                    json_uri: ref_value
                        .metadata_url
                        .clone()
                        .map(|url_with_status| url_with_status.metadata_url),
                    ..Default::default()
                },
                &order,
                1000,
                None,
                None,
                None,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 100);
        env.teardown().await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_upsert_meatadata_urls() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_filter_storage = &env.client;

        // Generate random asset indexes
        let asset_indexes = generate_asset_index_records(1);
        let last_known_key = generate_random_vec(8 + 8 + 32);
        let ref_value = &asset_indexes[asset_indexes.len() - 1];

        // Insert assets and last key using update_asset_indexes_batch
        asset_filter_storage
            .update_asset_indexes_batch(asset_indexes.as_slice())
            .await
            .unwrap();
        asset_filter_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();
        let asset_indexes = generate_asset_index_records(100);
        let last_known_key = generate_random_vec(8 + 8 + 32);

        // Insert assets and last key using update_asset_indexes_batch
        asset_filter_storage
            .update_asset_indexes_batch(asset_indexes.as_slice())
            .await
            .unwrap();
        asset_filter_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();
        let order = AssetSorting {
            sort_by: AssetSortBy::SlotCreated,
            sort_direction: AssetSortDirection::Asc,
        };

        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(
                &SearchAssetsFilter {
                    json_uri: ref_value
                        .metadata_url
                        .clone()
                        .map(|url_with_status| url_with_status.metadata_url),
                    ..Default::default()
                },
                &order,
                1000,
                None,
                None,
                None,
                &Options {
                    show_unverified_collections: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 101);
        env.teardown().await;
    }
}
