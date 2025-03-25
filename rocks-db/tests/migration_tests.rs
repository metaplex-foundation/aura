#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use entities::models::Updated;
    use metrics_utils::red::RequestErrorDurationMetrics;
    use rocks_db::{
        column::TypedColumn,
        columns::{
            asset::AssetLeaf,
            cl_items::{ClItemDeprecated, ClItemKey, ClItemV2},
            offchain_data::{OffChainData, OffChainDataDeprecated},
        },
        migrations::asset_leaf_v2::AssetLeafDeprecated,
        migrator::MigrationState,
        Storage,
    };
    use solana_sdk::{hash::Hash, pubkey::Pubkey};
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migration() {
        let dir = TempDir::new().unwrap();
        let v1 = OffChainDataDeprecated {
            url: "https://mail.com".to_string(),
            metadata: "".to_string(),
        };
        let url2 = "ipfs://abcdefg";
        let v2 = OffChainDataDeprecated {
            url: url2.to_string(),
            metadata: format!("{{\"image\":\"{}\",\"type\":\"{}\"}}", url2, "image/gif")
                .to_string(),
        };
        let path = dir.path().to_str().unwrap();
        let old_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(0),
        )
        .unwrap();
        old_storage
            .asset_offchain_data_deprecated
            .put(v1.url.clone(), v1.clone())
            .expect("should put");
        old_storage
            .asset_offchain_data_deprecated
            .put(v2.url.clone(), v2.clone())
            .expect("should put data");
        drop(old_storage);
        let secondary_storage_dir = TempDir::new().unwrap();
        let migration_version_manager = Storage::open_secondary(
            path,
            secondary_storage_dir.path().to_str().unwrap(),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(4),
        )
        .unwrap();
        let binding = TempDir::new().unwrap();
        let migration_storage_path = binding.path().to_str().unwrap();
        Storage::apply_all_migrations(
            path,
            migration_storage_path,
            Arc::new(migration_version_manager),
        )
        .await
        .unwrap();

        let new_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(4),
        )
        .unwrap();
        let mut it =
            new_storage.db.raw_iterator_cf(&new_storage.db.cf_handle(OffChainData::NAME).unwrap());
        it.seek_to_first();
        assert!(it.valid(), "iterator should be valid on start");
        while it.valid() {
            println!("has key {:?} with value {:?}", it.key(), it.value());
            it.next();
        }
        let migrated_v1 = new_storage
            .db
            .get_pinned_cf(
                &new_storage.db.cf_handle(OffChainData::NAME).unwrap(),
                OffChainData::encode_key(v1.url.clone()),
            )
            .expect("expect to get value successfully")
            .expect("value to be present");

        print!("migrated is {:?}", migrated_v1.to_vec());
        let migrated_v1 = new_storage
            .asset_offchain_data
            .get(v1.url.clone())
            .expect("should get value successfully")
            .expect("the value should be not empty");
        assert_eq!(
            migrated_v1.storage_mutability,
            rocks_db::columns::offchain_data::StorageMutability::Mutable
        );
        assert_eq!(migrated_v1.url, Some(v1.url.to_string()));
        assert_eq!(migrated_v1.metadata, Some(v1.metadata));
        assert_eq!(migrated_v1.last_read_at, 0);
        let migrated_v2 = new_storage
            .asset_offchain_data
            .get(v2.url.clone())
            .expect("should get value successfully")
            .expect("the value should be not empty");
        assert_eq!(
            migrated_v2.storage_mutability,
            rocks_db::columns::offchain_data::StorageMutability::Immutable
        );
        assert_eq!(migrated_v2.url, Some(v2.url.to_string()));
        assert_eq!(migrated_v2.metadata, Some(v2.metadata));
        assert_eq!(migrated_v2.last_read_at, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_clitems_v2_migration() {
        let dir = TempDir::new().unwrap();
        let node_id = 32782;
        let tree_id = Pubkey::from_str("6EdzmXrunmS1gqkuWzDuP94o1YPNc2cb8z45G1eQaMpp")
            .expect("a valid pubkey");
        let hash = [
            93, 208, 232, 135, 101, 117, 109, 249, 149, 77, 57, 114, 173, 168, 145, 196, 185, 190,
            21, 121, 205, 253, 143, 155, 82, 119, 9, 143, 73, 176, 233, 179,
        ]
        .to_vec();
        let seq = 32;
        let v1 = ClItemDeprecated {
            cli_node_idx: node_id,
            cli_tree_key: tree_id,
            cli_leaf_idx: None,
            cli_seq: seq,
            cli_level: 2,
            cli_hash: hash.clone(),
            slot_updated: 239021690,
        };
        let key = ClItemKey::new(node_id, tree_id);
        let path = dir.path().to_str().unwrap();
        let old_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(0),
        )
        .unwrap();
        old_storage.cl_items_deprecated.put(key.clone(), v1.clone()).expect("should put");
        drop(old_storage);
        let secondary_storage_dir = TempDir::new().unwrap();
        let migration_version_manager = Storage::open_secondary(
            path,
            secondary_storage_dir.path().to_str().unwrap(),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(4),
        )
        .unwrap();
        let binding = TempDir::new().unwrap();
        let migration_storage_path = binding.path().to_str().unwrap();
        Storage::apply_all_migrations(
            path,
            migration_storage_path,
            Arc::new(migration_version_manager),
        )
        .await
        .unwrap();

        let new_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(4),
        )
        .unwrap();
        let migrated_v1 = new_storage
            .db
            .get_pinned_cf(
                &new_storage.db.cf_handle(ClItemV2::NAME).unwrap(),
                ClItemV2::encode_key(key.clone()),
            )
            .expect("expect to get value successfully")
            .expect("value to be present");

        print!("migrated is {:?}", migrated_v1.to_vec());
        let migrated_v1 = new_storage
            .cl_items
            .get(key.clone())
            .expect("should get value successfully")
            .expect("the value should be not empty");
        assert_eq!(migrated_v1.finalized_hash, None);
        assert_eq!(migrated_v1.leaf_idx, None);
        assert_eq!(migrated_v1.level, 2);
        assert_eq!(
            migrated_v1.pending_hash,
            Some(Updated::new(
                239021690,
                Some(entities::models::UpdateVersion::Sequence(seq)),
                hash
            ))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_asset_leaf_v2_migration() {
        let dir = TempDir::new().unwrap();
        let v0 = AssetLeafDeprecated {
            pubkey: Pubkey::new_unique(),
            tree_id: Pubkey::new_unique(),
            leaf: Some(vec![1, 2, 3, 4, 5]),
            nonce: Some(rand::random::<u64>()),
            data_hash: Some(Hash::new_unique()),
            creator_hash: Some(Hash::new_unique()),
            leaf_seq: Some(rand::random::<u64>()),
            slot_updated: rand::random::<u64>(),
        };

        let key = Pubkey::new_unique();
        let path = dir.path().to_str().unwrap();
        let old_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(0),
        )
        .unwrap();
        old_storage.asset_leaf_data_deprecated.put(key.clone(), v0.clone()).expect("should put");
        drop(old_storage);

        let secondary_storage_dir = TempDir::new().unwrap();
        let migration_version_manager = Storage::open_secondary(
            path,
            secondary_storage_dir.path().to_str().unwrap(),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(6),
        )
        .unwrap();

        let binding = TempDir::new().unwrap();
        let migration_storage_path = binding.path().to_str().unwrap();
        Storage::apply_all_migrations(
            path,
            migration_storage_path,
            Arc::new(migration_version_manager),
        )
        .await
        .unwrap();

        let new_storage = Storage::open(
            path,
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(6),
        )
        .unwrap();
        let migrated_v1 = new_storage
            .db
            .get_pinned_cf(
                &new_storage.db.cf_handle(AssetLeaf::NAME).unwrap(),
                AssetLeaf::encode_key(key.clone()),
            )
            .expect("expect to get value successfully")
            .expect("value to be present");

        print!("migrated is {:?}", migrated_v1.to_vec());
        let migrated_v1 = new_storage
            .asset_leaf_data
            .get(key.clone())
            .expect("should get value successfully")
            .expect("the value should be not empty");
        assert_eq!(migrated_v1.flags, None);
        assert_eq!(migrated_v1.collection_hash, None);
        assert_eq!(migrated_v1.asset_data_hash, None);
        assert_eq!(migrated_v1.pubkey, v0.pubkey,);
    }
}
