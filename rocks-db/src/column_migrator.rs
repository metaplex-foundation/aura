use crate::asset::AssetCollection;
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{decode_u64, encode_u64};
use crate::Result;
use crate::Storage;
use bincode::deserialize;
use entities::models::{UpdateVersion, Updated};
use interface::migration_version_manager::PrimaryStorageMigrationVersionManager;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};

const CURRENT_MIGRATION_VERSION: u64 = 0;
const BATCH_SIZE: usize = 100_000;

pub enum MigrationState {
    Last,
    Version(u64),
}

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
                    error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
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
                    error!("RocksDB: AssetCollection deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

impl Storage {
    pub async fn apply_all_migrations(
        db_path: &str,
        migration_version_manager: Arc<impl PrimaryStorageMigrationVersionManager>,
    ) -> Result<()> {
        let applied_migrations = migration_version_manager
            .get_all_applied_migrations()
            .map_err(StorageError::Common)?;
        for version in 0..=CURRENT_MIGRATION_VERSION {
            if !applied_migrations.contains(&version) {
                Storage::apply_migration(db_path, version).await?;
            }
        }
        Ok(())
    }

    async fn apply_migration(db_path: &str, version: u64) -> Result<()> {
        match version {
            0 => Storage::apply_migration_v0(db_path).await?,
            _ => return Err(StorageError::InvalidMigrationVersion(version)),
        }

        Ok(())
    }

    async fn apply_migration_v0(db_path: &str) -> Result<()> {
        info!("Start execute migration V0");
        {
            let old_storage = Storage::open(
                db_path,
                Arc::new(Mutex::new(JoinSet::new())),
                Arc::new(RequestErrorDurationMetrics::new()),
                MigrationState::Version(0),
            )?;
            // "force-merge" logic: the merge is happening only on read operations,
            // so we iterate over all records inside column in order to merge them
            for _ in old_storage.asset_collection_data.iter_start() {}
            // close db connection in the end of the scope
        }

        let new_storage = Storage::open(
            db_path,
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Last,
        )?;
        let mut batch = HashMap::new();
        for (key, value) in new_storage
            .asset_collection_data
            .iter_start()
            .filter_map(std::result::Result::ok)
        {
            let key_decoded = match new_storage.asset_collection_data.decode_key(key.to_vec()) {
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
            if batch.len() > BATCH_SIZE {
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
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrationVersions {}

impl TypedColumn for MigrationVersions {
    type KeyType = u64;
    type ValueType = Self;
    const NAME: &'static str = "MIGRATION_VERSIONS";

    fn encode_key(version: u64) -> Vec<u8> {
        encode_u64(version)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64(bytes)
    }
}

impl PrimaryStorageMigrationVersionManager for Storage {
    fn get_all_applied_migrations(&self) -> std::result::Result<HashSet<u64>, String> {
        Ok(self
            .migration_version
            .iter_start()
            .filter_map(std::result::Result::ok)
            .flat_map(|(key, _)| MigrationVersions::decode_key(key.as_ref().to_vec()))
            .fold(HashSet::new(), |mut acc, version| {
                acc.insert(version);
                acc
            }))
    }
}
