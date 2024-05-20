#[cfg(test)]
mod tests {
    use bincode::serialize;
    use metrics_utils::red::RequestErrorDurationMetrics;
    use rocks_db::asset::AssetCollection;
    use rocks_db::column::TypedColumn;
    use rocks_db::column_migrator::{AssetCollectionVersion0, MigrationState};
    use rocks_db::Storage;
    use solana_sdk::pubkey::Pubkey;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tokio::task::JoinSet;

    fn put_unmerged_value_to_storage(path: &str) -> (Pubkey, AssetCollectionVersion0) {
        let old_storage = Storage::open(
            path,
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(0),
        )
        .unwrap();
        let key = Pubkey::new_unique();
        old_storage
            .asset_collection_data
            .backend
            .put_cf(
                &old_storage
                    .asset_collection_data
                    .backend
                    .cf_handle(AssetCollection::NAME)
                    .unwrap(),
                AssetCollection::encode_key(key),
                serialize(&AssetCollectionVersion0 {
                    pubkey: Pubkey::new_unique(),
                    collection: Pubkey::new_unique(),
                    is_collection_verified: false,
                    collection_seq: Some(5),
                    slot_updated: 5,
                    write_version: Some(5),
                })
                .unwrap(),
            )
            .unwrap();

        let new_val = AssetCollectionVersion0 {
            pubkey: Pubkey::new_unique(),
            collection: Pubkey::new_unique(),
            is_collection_verified: false,
            collection_seq: Some(10),
            slot_updated: 10,
            write_version: Some(10),
        };
        old_storage
            .asset_collection_data
            .backend
            .merge_cf(
                &old_storage
                    .asset_collection_data
                    .backend
                    .cf_handle(AssetCollection::NAME)
                    .unwrap(),
                AssetCollection::encode_key(key),
                serialize(&new_val).unwrap(),
            )
            .unwrap();
        // close connection in the end of the scope
        (key, new_val)
    }

    #[test]
    fn test_merge_fail() {
        let dir = TempDir::new().unwrap();
        put_unmerged_value_to_storage(dir.path().to_str().unwrap());
        assert_eq!(
            Storage::open(
                dir.path().to_str().unwrap(),
                Arc::new(Mutex::new(JoinSet::new())),
                Arc::new(RequestErrorDurationMetrics::new()),
                MigrationState::Last,
            )
            .err()
            .unwrap()
            .to_string(),
            "storage error: RocksDb(Error { message: \"Corruption: Merge operator failed\" })"
        );
    }

    #[tokio::test]
    async fn test_migration() {
        let dir = TempDir::new().unwrap();
        let (key, val) = put_unmerged_value_to_storage(dir.path().to_str().unwrap());
        let secondary_storage_dir = TempDir::new().unwrap();
        let migration_version_manager = Storage::open_secondary(
            dir.path().to_str().unwrap(),
            secondary_storage_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Last,
        )
        .unwrap();
        Storage::apply_all_migrations(
            dir.path().to_str().unwrap(),
            Arc::new(migration_version_manager),
        )
        .await
        .unwrap();

        let new_storage = Storage::open(
            dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Last,
        )
        .unwrap();

        let selected_val = new_storage.asset_collection_data.get(key).unwrap().unwrap();

        assert_eq!(selected_val.pubkey, val.pubkey)
    }
}
