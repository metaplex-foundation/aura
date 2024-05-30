use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{decode_u64, encode_u64};
use crate::Result;
use crate::Storage;
use interface::migration_version_manager::PrimaryStorageMigrationVersionManager;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

const CURRENT_MIGRATION_VERSION: u64 = 0;
pub(crate) const BATCH_SIZE: usize = 100_000;

pub enum MigrationState {
    Last,
    Version(u64),
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
            0 => crate::migrations::collection_authority::apply_migration(db_path).await?,
            _ => return Err(StorageError::InvalidMigrationVersion(version)),
        }

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
