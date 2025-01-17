#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metrics_utils::red::RequestErrorDurationMetrics;
    use rocks_db::{
        column::TypedColumn,
        columns::offchain_data::{OffChainData, OffChainDataDeprecated},
        migrator::MigrationState,
        Storage,
    };
    use tempfile::TempDir;
    use tokio::{sync::Mutex, task::JoinSet};

    #[tokio::test]
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
            Arc::new(Mutex::new(JoinSet::new())),
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
            Arc::new(Mutex::new(JoinSet::new())),
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
            Arc::new(Mutex::new(JoinSet::new())),
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

    #[test]
    #[ignore = "TODO: test migrations on relevant columns"]
    fn test_merge_fail() {
        let dir = TempDir::new().unwrap();
        // put_unmerged_value_to_storage(dir.path().to_str().unwrap());
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
}
