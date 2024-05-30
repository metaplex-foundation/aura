use crate::asset::AssetCollection;
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::migrator::{MigrationState, MigrationVersions, BATCH_SIZE};
use crate::Storage;
use bincode::deserialize;
use entities::models::{UpdateVersion, Updated};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::{IteratorMode, MergeOperands};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetCollectionVersion0 {
    pub pubkey: Pubkey,
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
    pub slot_updated: u64,
    pub write_version: Option<u64>,
}

impl From<AssetCollectionVersion0> for AssetCollection {
    fn from(value: AssetCollectionVersion0) -> Self {
        let update_version = if let Some(write_version) = value.write_version {
            Some(UpdateVersion::WriteVersion(write_version))
        } else {
            value.collection_seq.map(UpdateVersion::Sequence)
        };
        Self {
            pubkey: value.pubkey,
            collection: Updated::new(value.slot_updated, update_version.clone(), value.collection),
            is_collection_verified: Updated::new(
                value.slot_updated,
                update_version,
                value.is_collection_verified,
            ),
            authority: Default::default(),
        }
    }
}

impl AssetCollectionVersion0 {
    pub fn merge_asset_collection(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        let mut collection_seq = None;
        let mut write_version = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    collection_seq = value.collection_seq;
                    write_version = value.write_version;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: AssetCollectionV0 deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    if write_version.is_some() && new_val.write_version.is_some() {
                        if new_val.write_version.unwrap() > write_version.unwrap() {
                            slot = new_val.slot_updated;
                            write_version = new_val.write_version;
                            collection_seq = new_val.collection_seq;
                            result = op.to_vec();
                        }
                    } else if collection_seq.is_some() && new_val.collection_seq.is_some() {
                        if new_val.collection_seq.unwrap() > collection_seq.unwrap() {
                            slot = new_val.slot_updated;
                            write_version = new_val.write_version;
                            collection_seq = new_val.collection_seq;
                            result = op.to_vec();
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        write_version = new_val.write_version;
                        collection_seq = new_val.collection_seq;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetCollectionV0 deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

pub(crate) async fn apply_migration(
    db_path: &str,
    migration_storage_path: &str,
) -> crate::Result<()> {
    info!("Start executing migration V0");
    let temporary_migration_storage = Storage::open(
        migration_storage_path,
        Arc::new(Mutex::new(JoinSet::new())),
        Arc::new(RequestErrorDurationMetrics::new()),
        MigrationState::Version(0),
    )?;
    {
        let old_storage = Storage::open(
            db_path,
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(0),
        )?;
        let iter = old_storage.db.iterator_cf(
            &old_storage
                .db
                .cf_handle(AssetCollection::NAME)
                .ok_or(StorageError::Common(
                    "Cannot get cf_handle for AssetCollection".to_string(),
                ))?,
            IteratorMode::Start,
        );

        info!("Start coping data into temporary storage");
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (key, value) in iter.flatten() {
            batch.put_cf(
                &temporary_migration_storage
                    .db
                    .cf_handle(AssetCollection::NAME)
                    .ok_or(StorageError::Common(
                        "Cannot get cf_handle for AssetCollection".to_string(),
                    ))?,
                key,
                value,
            );
            if batch.len() >= BATCH_SIZE {
                temporary_migration_storage.db.write(batch)?;
                batch = rocksdb::WriteBatchWithTransaction::<false>::default();
            }
        }
        temporary_migration_storage.db.write(batch)?;

        info!("Finish coping data into temporary storage");
        old_storage.db.drop_cf(AssetCollection::NAME)?;
    }

    let new_storage = Storage::open(
        db_path,
        Arc::new(Mutex::new(JoinSet::new())),
        Arc::new(RequestErrorDurationMetrics::new()),
        MigrationState::Version(1),
    )?;
    let mut batch = HashMap::new();
    for (key, value) in temporary_migration_storage
        .asset_collection_data
        .iter_start()
        .filter_map(std::result::Result::ok)
    {
        let key_decoded = match temporary_migration_storage
            .asset_collection_data
            .decode_key(key.to_vec())
        {
            Ok(key_decoded) => key_decoded,
            Err(e) => {
                error!("collection data decode_key: {:?}, {}", key.to_vec(), e);
                continue;
            }
        };
        let value_decoded = match deserialize::<AssetCollectionVersion0>(&value) {
            Ok(value_decoded) => value_decoded,
            Err(e) => {
                error!("collection data deserialize: {}, {}", key_decoded, e);
                continue;
            }
        };
        batch.insert(key_decoded, value_decoded.into());
        if batch.len() >= BATCH_SIZE {
            new_storage
                .asset_collection_data
                .put_batch(std::mem::take(&mut batch))
                .await?;
        }
    }
    new_storage.asset_collection_data.put_batch(batch).await?;
    info!("Finish migration V0");

    new_storage
        .migration_version
        .put_async(0, MigrationVersions {})
        .await?;
    temporary_migration_storage
        .db
        .drop_cf(AssetCollection::NAME)?;
    Ok(())
}
