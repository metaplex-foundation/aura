#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use entities::enums::OwnerType;
    use entities::models::Updated;
    use solana_sdk::pubkey::Pubkey;
    use tempfile::TempDir;

    use metrics_utils::red::RequestErrorDurationMetrics;
    use rocks_db::key_encoders::encode_u64x2_pubkey;
    use rocks_db::storage_traits::AssetUpdateIndexStorage;
    use rocks_db::{AssetDynamicDetails, AssetOwner, Storage};
    use tokio::sync::Mutex;
    use tokio::task::JoinSet;

    use setup::rocks::{
        create_test_dynamic_data, RocksTestEnvironment, DEFAULT_PUBKEY_OF_ONES, PUBKEY_OF_TWOS,
    };

    #[test]
    fn test_process_asset_updates_batch_empty_db() {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
            red_metrics.clone(),
        )
        .expect("Failed to create a database");

        // Call fetch_asset_updated_keys on an empty database
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, None, 10, None)
            .expect("Failed to fetch asset updated keys");
        // Assertions
        assert!(keys.is_empty(), "Expected no keys from an empty database");
        assert!(
            last_key.is_none(),
            "Expected no last key from an empty database"
        );
    }

    #[test]
    fn test_process_asset_updates_batch_with_same_key_records_iteration_order() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()),
            (2, DEFAULT_PUBKEY_OF_ONES.clone()),
        ])
        .storage;
        // Verify iteration order
        let mut iter = storage.assets_update_idx.iter_start();
        let first_key = iter.next().unwrap().unwrap().0; // Get the first key
        let expected_key = encode_u64x2_pubkey(1, 4, DEFAULT_PUBKEY_OF_ONES.clone());
        assert_eq!(
            first_key.as_ref(),
            expected_key.as_slice(),
            "The first key does not match the expected key"
        );

        let second_key = iter.next().unwrap().unwrap().0; // Get the second key
        let expected_key = encode_u64x2_pubkey(2, 2, DEFAULT_PUBKEY_OF_ONES.clone());
        assert_eq!(
            second_key.as_ref(),
            expected_key.as_slice(),
            "The second key does not match the expected key"
        );
    }

    #[test]
    fn test_process_asset_updates_batch_with_same_key_records() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()),
            (2, DEFAULT_PUBKEY_OF_ONES.clone()),
        ])
        .storage;
        // Verify fetch_asset_updated_keys with None as last key
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, None, 10, None)
            .unwrap();
        assert_eq!(keys.len(), 1, "Expected a single key");
        assert_eq!(
            keys.iter().next().unwrap(),
            &DEFAULT_PUBKEY_OF_ONES,
            "Expected the specific pubkey"
        );
        assert!(last_key.is_some(), "Expected a last key");
        // Verify fetch_asset_updated_keys with the last key from previous call
        let (new_keys, new_last_key) = storage
            .fetch_asset_updated_keys(last_key, None, 10, None)
            .unwrap();
        assert!(
            new_keys.is_empty(),
            "Expected no new keys, but found: {:?}",
            new_keys
        );
        assert_eq!(new_last_key, last_key, "Expected no new last key");
    }

    #[test]
    fn test_fetch_asset_updated_keys_with_limit_and_skip() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()),
            (2, DEFAULT_PUBKEY_OF_ONES.clone()),
        ])
        .storage;
        // Verify fetch_asset_updated_keys with None as last key
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, None, 1, None)
            .unwrap();
        assert_eq!(keys.len(), 1, "Expected a single key");
        assert_eq!(
            keys.iter().next().unwrap(),
            &DEFAULT_PUBKEY_OF_ONES,
            "Expected the specific pubkey"
        );
        assert!(last_key.is_some(), "Expected a last key");
        // Verify fetch_asset_updated_keys with the last key from previous call
        let (new_keys, new_last_key) = storage
            .fetch_asset_updated_keys(last_key, None, 1, Some(keys))
            .unwrap();
        assert!(
            new_keys.is_empty(),
            "Expected no new keys, but found: {:?}",
            new_keys
        );
        assert_ne!(new_last_key, last_key, "Expected a new last key");
    }

    #[test]
    fn test_fetch_asset_updated_keys_with_skip() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()),
            (2, DEFAULT_PUBKEY_OF_ONES.clone()),
        ])
        .storage;
        // Verify fetch_asset_updated_keys with None as last key
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(
                None,
                None,
                1,
                Some(HashSet::from_iter(vec![DEFAULT_PUBKEY_OF_ONES.clone()])),
            )
            .unwrap();
        assert_eq!(keys.len(), 0, "Expected no keys");
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (2, 2, DEFAULT_PUBKEY_OF_ONES.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key"
        );
    }

    #[test]
    fn test_up_to_filter() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 1
            (2, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 2
            (5, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 3
            (5, PUBKEY_OF_TWOS.clone()),         // seq = 4
        ])
        .storage;

        // Verify fetch_asset_updated_keys with up to key which is less then the first key
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, Some((0, 2, DEFAULT_PUBKEY_OF_ONES.clone())), 10, None)
            .unwrap();
        assert_eq!(keys.len(), 0, "Expected no keys");
        assert!(last_key.is_none(), "Expected an empty last key");

        // verify fetch_asset_updated_keys with up to key which is equal to the first key
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, Some((1, 4, DEFAULT_PUBKEY_OF_ONES.clone())), 10, None)
            .unwrap();
        assert_eq!(keys.len(), 1, "Expected a single key");
        assert_eq!(
            keys.iter().next().unwrap(),
            &DEFAULT_PUBKEY_OF_ONES,
            "Expected the specific pubkey"
        );
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (1, 4, DEFAULT_PUBKEY_OF_ONES.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key {:?}, got {:?}",
            expected_key,
            last_key
        );

        // verify fetch_asset_updated_keys with up to key which is equal to the last key returns all the keys
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, Some((4, 5, PUBKEY_OF_TWOS.clone())), 10, None)
            .unwrap();
        assert_eq!(keys.len(), 2, "Expected 2 keys, got {:?}", keys);
        assert!(
            keys.contains(&DEFAULT_PUBKEY_OF_ONES),
            "Expected the specific pubkey"
        );
        assert!(
            keys.contains(&PUBKEY_OF_TWOS),
            "Expected the specific pubkey"
        );
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (4, 5, PUBKEY_OF_TWOS.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key {:?}, got {:?}",
            expected_key,
            last_key
        );
    }

    #[test]
    fn test_last_known_asset_updated_key_on_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let last_key = storage
            .last_known_asset_updated_key()
            .expect("Failed to get last known asset updated key");
        assert!(last_key.is_none(), "Expected no last key");
    }

    #[test]
    fn test_last_known_asset_updated_key_on_non_empty_db() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 1
            (2, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 2
            (5, DEFAULT_PUBKEY_OF_ONES.clone()), // seq = 3
            (5, PUBKEY_OF_TWOS.clone()),         // seq = 4
        ])
        .storage;
        let last_key = storage
            .last_known_asset_updated_key()
            .expect("Failed to get last known asset updated key");
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (4, 5, PUBKEY_OF_TWOS.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key {:?}, got {:?}",
            expected_key,
            last_key
        );
    }

    #[test]
    fn test_process_asset_updates_batch_iteration_results() {
        let storage = RocksTestEnvironment::new(&[
            (4, DEFAULT_PUBKEY_OF_ONES.clone()),
            (2, PUBKEY_OF_TWOS.clone()),
        ])
        .storage;
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(None, None, 10, None)
            .unwrap();

        assert_eq!(keys.len(), 2, "Expected 2 keys");
        assert!(
            keys.contains(&DEFAULT_PUBKEY_OF_ONES),
            "Expected the specific pubkey"
        );
        assert!(
            keys.contains(&PUBKEY_OF_TWOS),
            "Expected the specific pubkey"
        );
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (2, 2, PUBKEY_OF_TWOS.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key {:?}, got {:?}",
            expected_key,
            last_key
        );
        let key = Pubkey::new_unique();
        storage.asset_updated(5, key.clone()).unwrap();
        let (keys, last_key) = storage
            .fetch_asset_updated_keys(last_key, None, 10, None)
            .unwrap();

        assert_eq!(keys.len(), 1, "Expected 1 key");
        assert!(
            keys.contains(&key),
            "Expected the specific pubkey {:?}, got {:?}",
            key,
            keys
        );
        assert!(last_key.is_some(), "Expected a last key");
        let expected_key = (3, 5, key.clone());
        assert_eq!(
            last_key.unwrap(),
            expected_key,
            "Expected the specific last key {:?}, got {:?}",
            expected_key,
            last_key
        );

        // generate 10k more records and then batch read those
        let mut keys = Vec::new();
        for i in 0..10000 {
            let key = Pubkey::new_unique();
            storage.asset_updated(i, key.clone()).unwrap();
            keys.push((4 + i, i, key.clone()));
        }
        let mut last_seen_key = last_key.clone();
        for i in 0..10 {
            let (new_keys, last_key) = storage
                .fetch_asset_updated_keys(last_seen_key, None, 1000, None)
                .unwrap();
            assert_eq!(new_keys.len(), 1000, "Expected 1000 keys");
            assert!(last_key.is_some(), "Expected a last key");
            let expected_key = keys[i * 1000 + 999].clone();
            assert_eq!(
                last_key.unwrap(),
                expected_key,
                "Expected the specific last key {:?}, got {:?} for {:?} iteration",
                expected_key,
                last_key,
                i
            );
            for j in 0..1000 {
                assert!(
                    new_keys.contains(&keys[i * 1000 + j].2),
                    "Expected the specific pubkey {:?}, got {:?}",
                    keys[i * 1000 + j].2,
                    new_keys
                );
            }
            last_seen_key = last_key.clone();
        }
    }

    #[tokio::test]
    async fn test_multiple_slot_updates() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let pk = Pubkey::new_unique();
        let dynamic_data = create_test_dynamic_data(pk, 0, "http://example.com".to_string());

        storage
            .asset_dynamic_data
            .merge(dynamic_data.pubkey, dynamic_data.clone())
            .await
            .unwrap();

        let new_data = AssetDynamicDetails {
            pubkey: pk,
            is_compressible: Updated::new(10, None, None, true),
            is_compressed: Updated::new(0, None, None, true),
            supply: Some(Updated::new(0, None, None, 5)),
            ..Default::default()
        };
        storage
            .asset_dynamic_data
            .merge(dynamic_data.pubkey, new_data)
            .await
            .unwrap();

        let selected_data = storage.asset_dynamic_data.get(pk).unwrap().unwrap();
        assert_eq!(
            selected_data.is_compressible,
            Updated::new(10, None, None, true)
        );
        assert_eq!(
            selected_data.is_compressed,
            Updated::new(0, None, None, false)
        ); // slot in new_data not greater than slot in start data, so that field must not change
        assert_eq!(selected_data.supply, Some(Updated::new(0, None, None, 1))); // slot in new_data not greater than slot in start data, so that field must not change

        let new_data = AssetDynamicDetails {
            pubkey: pk,
            is_compressible: Updated::new(5, None, None, false),
            is_compressed: Updated::new(0, None, None, true),
            supply: Some(Updated::new(3, None, None, 5)),
            ..Default::default()
        };
        storage
            .asset_dynamic_data
            .merge(dynamic_data.pubkey, new_data)
            .await
            .unwrap();

        let selected_data = storage.asset_dynamic_data.get(pk).unwrap().unwrap();
        assert_eq!(
            selected_data.is_compressible,
            Updated::new(10, None, None, true)
        );
        assert_eq!(
            selected_data.is_compressed,
            Updated::new(0, None, None, false)
        );
        assert_eq!(selected_data.supply, Some(Updated::new(3, None, None, 5)));

        let new_data = AssetDynamicDetails {
            pubkey: pk,
            is_compressible: Updated::new(5, None, Some(1), false),
            ..Default::default()
        };
        storage
            .asset_dynamic_data
            .merge(dynamic_data.pubkey, new_data)
            .await
            .unwrap();

        let selected_data = storage.asset_dynamic_data.get(pk).unwrap().unwrap();
        assert_eq!(
            selected_data.is_compressible,
            Updated::new(10, None, None, true)
        );
        // data will not be updated because slot is lower
        // and to update data based of seq both new and old records have to have that value
    }

    #[tokio::test]
    async fn test_asset_delegate_update() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let pk = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let asset_owner_data = AssetOwner {
            pubkey: pk,
            owner: Updated::new(1, None, Some(1), owner),
            delegate: Updated::new(1, None, Some(1), Some(owner)),
            owner_type: Updated::new(1, None, Some(1), OwnerType::Single),
            owner_delegate_seq: Updated::new(1, None, Some(1), Some(1)),
        };

        storage
            .asset_owner_data
            .merge(pk, asset_owner_data.clone())
            .await
            .unwrap();

        let selected_data = storage.asset_owner_data.get(pk).unwrap().unwrap();

        assert_eq!(selected_data.delegate, asset_owner_data.delegate);

        let new_owner = Pubkey::new_unique();

        let updated_owner_data = AssetOwner {
            pubkey: pk,
            owner: Updated::new(2, None, Some(2), new_owner),
            delegate: Updated::new(2, None, Some(2), None),
            owner_type: Updated::new(2, None, Some(2), OwnerType::Single),
            owner_delegate_seq: Updated::new(2, None, Some(2), Some(2)),
        };

        storage
            .asset_owner_data
            .merge(pk, updated_owner_data.clone())
            .await
            .unwrap();

        let selected_data = storage.asset_owner_data.get(pk).unwrap().unwrap();

        assert_eq!(selected_data.delegate.value, None);
    }
}
