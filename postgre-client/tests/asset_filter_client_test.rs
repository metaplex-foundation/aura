mod db_setup;

#[cfg(feature = "integration_tests")]
#[cfg(test)]
mod tests {
    use super::db_setup;
    use postgre_client::model::*;
    use postgre_client::storage_traits::{AssetIndexStorage, AssetPubkeyFilteredFetcher};
    use postgre_client::PgClient;
    use testcontainers::clients::Cli;
    use testcontainers::*;
    use tokio;

    #[tokio::test]
    async fn test_get_asset_pubkeys_filtered_on_empty_db() {
        let cli = Cli::default();
        let node = cli.run(images::postgres::Postgres::default());
        let (pool, db_name) = db_setup::setup_database(&node).await;

        let asset_filter_storage = PgClient::new_with_pool(pool.clone());

        let filter = SearchAssetsFilter {
            specification_version: Some(SpecificationVersions::V1),
            specification_asset_class: Some(SpecificationAssetClass::Nft),
            owner_address: Some(db_setup::generate_random_vec(32)),
            owner_type: Some(OwnerType::Single),
            creator_address: Some(db_setup::generate_random_vec(32)),
            creator_verified: Some(true),
            authority_address: Some(db_setup::generate_random_vec(32)),
            collection: Some(db_setup::generate_random_vec(32)),
            delegate: Some(db_setup::generate_random_vec(32)),
            frozen: Some(false),
            supply: Some(1),
            supply_mint: Some(db_setup::generate_random_vec(32)),
            compressed: Some(false),
            compressible: Some(false),
            royalty_target_type: Some(RoyaltyTargetType::Creators),
            royalty_target: Some(db_setup::generate_random_vec(32)),
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
            .get_asset_pubkeys_filtered(&filter, &order, limit, page, before, after)
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        pool.close().await;
        db_setup::teardown(&node, &db_name).await;
    }

    #[tokio::test]
    async fn test_get_asset_pubkeys_filtered_on_filled_db() {
        let cli = Cli::default();
        let node = cli.run(images::postgres::Postgres::default());
        let (pool, db_name) = db_setup::setup_database(&node).await;

        let asset_filter_storage = PgClient::new_with_pool(pool.clone());
        // Generate random asset indexes
        let asset_indexes = db_setup::generate_asset_index_records(100);
        let last_known_key = db_setup::generate_random_vec(8 + 8 + 32);

        // Insert assets and last key using update_asset_indexes_batch
        asset_filter_storage
            .update_asset_indexes_batch(asset_indexes.as_slice(), &last_known_key)
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
            supply: ref_value.supply.map(|s| s as u64),
            supply_mint: Some(ref_value.pubkey.to_bytes().to_vec()),
            compressed: Some(ref_value.is_compressed),
            compressible: Some(ref_value.is_compressible),
            royalty_target_type: Some(ref_value.royalty_target_type.into()),
            royalty_target: Some(ref_value.creators[0].creator.to_bytes().to_vec()),
            royalty_amount: Some(ref_value.royalty_amount as u32),
            burnt: Some(ref_value.is_burnt),
            json_uri: ref_value.metadata_url.clone(),
        };
        let order: AssetSorting = AssetSorting {
            sort_by: AssetSortBy::SlotUpdated,
            sort_direction: AssetSortDirection::Asc,
        };
        let limit = 10;
        let page = Some(0);

        let res = asset_filter_storage
            .get_asset_pubkeys_filtered(&filter, &order, limit, page, None, None)
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
                .get_asset_pubkeys_filtered(&filter, &order, limit, page, None, None)
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
                .get_asset_pubkeys_filtered(&filter, &order, limit, page, None, after)
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
                .get_asset_pubkeys_filtered(&filter, &order, limit, page, before, None)
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
            .get_asset_pubkeys_filtered(&Default::default(), &order, limit, None, None, None)
            .await
            .unwrap();
        assert_eq!(res.len(), limit as usize);

        pool.close().await;
        db_setup::teardown(&node, &db_name).await;
    }
}
