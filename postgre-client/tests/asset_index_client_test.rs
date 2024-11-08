#[cfg(feature = "integration_tests")]
#[cfg(test)]
mod tests {
    use setup::pg::*;

    use entities::models::{AssetIndex, Creator};
    use postgre_client::{asset_index_client::AssetType, storage_traits::AssetIndexStorage};
    use rand::Rng;
    use testcontainers::clients::Cli;
    use tokio;

    #[tokio::test]
    async fn test_fetch_last_synced_id_and_update_asset_indexes_batch() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        // Verify initial fetch_last_synced_id returns None
        let asset_types = [AssetType::Fungible, AssetType::NonFungible];
        for asset_type in asset_types.iter() {
            assert!(asset_index_storage
                .fetch_last_synced_id(*asset_type)
                .await
                .unwrap()
                .is_none());
        }

        // Generate random asset indexes
        let asset_indexes = generate_asset_index_records(100);
        let last_known_key = generate_random_vec(8 + 8 + 32);
        // Insert assets and last key using update_asset_indexes_batch
        asset_index_storage
            .update_asset_indexes_batch(asset_indexes.as_slice())
            .await
            .unwrap();

        asset_index_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();
        // Check fetch_last_synced_id again
        let last_synced_key = asset_index_storage
            .fetch_last_synced_id(AssetType::NonFungible)
            .await
            .unwrap();
        assert!(last_synced_key.is_some());
        assert_eq!(last_synced_key.unwrap().as_slice(), &last_known_key[..]);

        // Check the assets are in the db using plain SQL
        let cnt = env.count_rows_in_assets().await.unwrap();
        assert_eq!(cnt, 100);
        // ... (Write a SQL query to assert the inserted assets here)

        let new_known_key = generate_random_vec(8 + 8 + 32);
        // upsert the existing keys
        let updated_asset_indexes = asset_indexes
            .iter()
            .map(|asset_index| {
                let mut ai = asset_index.clone();
                ai.slot_updated = 200;
                ai.creators[0].creator_verified = !ai.creators[0].creator_verified;
                ai
            })
            .collect::<Vec<AssetIndex>>();
        asset_index_storage
            .update_asset_indexes_batch(updated_asset_indexes.as_slice())
            .await
            .unwrap();
        asset_index_storage
            .update_last_synced_key(&new_known_key)
            .await
            .unwrap();
        let last_synced_key = asset_index_storage
            .fetch_last_synced_id(AssetType::NonFungible)
            .await
            .unwrap();
        assert!(last_synced_key.is_some());
        assert_eq!(last_synced_key.unwrap().as_slice(), &new_known_key[..]);

        let cnt = env.count_rows_in_assets().await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = env.count_rows_in_metadata().await.unwrap();
        assert_eq!(cnt, 1);

        env.teardown().await;
    }

    #[tokio::test]
    async fn test_update_asset_indexes_batch_w_creators_update() {
        let cli = Cli::default();
        let env = TestEnvironment::new(&cli).await;
        let asset_index_storage = &env.client;

        // Generate random asset indexes
        let mut asset_indexes = generate_asset_index_records(100);
        // every asset_index will have 3 creators
        for asset_index in asset_indexes.iter_mut() {
            asset_index.creators.push(Creator {
                creator: generate_random_pubkey(),
                creator_verified: rand::thread_rng().gen_bool(0.5),
                creator_share: 30,
            });
            asset_index.creators.push(Creator {
                creator: generate_random_pubkey(),
                creator_verified: rand::thread_rng().gen_bool(0.5),
                creator_share: 30,
            });
        }

        let last_known_key = generate_random_vec(8 + 8 + 32);

        // Insert assets and last key using update_asset_indexes_batch
        asset_index_storage
            .update_asset_indexes_batch(asset_indexes.as_slice())
            .await
            .unwrap();
        asset_index_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();

        // Check the assets are in the db using plain SQL
        let cnt = env.count_rows_in_assets().await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = env.count_rows_in_creators().await.unwrap();
        assert_eq!(cnt, 300);

        // upsert the existing keys with 2 last creators replaced with a new one
        let updated_asset_indexes = asset_indexes
            .iter()
            .map(|asset_index| {
                let mut ai = asset_index.clone();
                ai.creators.pop();
                ai.creators[1].creator = generate_random_pubkey();
                ai.creators[0].creator_verified = !ai.creators[0].creator_verified;
                ai
            })
            .collect::<Vec<AssetIndex>>();

        asset_index_storage
            .update_asset_indexes_batch(updated_asset_indexes.as_slice())
            .await
            .unwrap();
        asset_index_storage
            .update_last_synced_key(&last_known_key)
            .await
            .unwrap();

        let cnt = env.count_rows_in_assets().await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = env.count_rows_in_creators().await.unwrap();
        assert_eq!(cnt, 200);

        env.teardown().await;
    }
}
