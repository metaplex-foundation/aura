use crate::asset::{AssetCollection, AssetCompleteDetails};
use crate::column::{Column, TypedColumn};
use crate::errors::StorageError;
use crate::key_encoders::{decode_u64, encode_u64};
use crate::Storage;
use crate::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result};
use bincode::deserialize;
use interface::migration_version_manager::PrimaryStorageMigrationVersionManager;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::{IteratorMode, DB};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};

pub(crate) const BATCH_SIZE: usize = 1_000_000;

pub enum MigrationState {
    Last,
    CreateColumnFamilies,
    Version(u64),
}

pub enum SerializationType {
    Bincode,
    Cbor,
}

pub trait RocksMigration {
    const VERSION: u64;
    const SERIALIZATION_TYPE: SerializationType;
    type NewDataType: Sync + Serialize + DeserializeOwned + Send + TypedColumn;
    type OldDataType: Sync
        + Serialize
        + DeserializeOwned
        + Send
        + Into<<Self::NewDataType as TypedColumn>::ValueType>;
}

#[macro_export]
macro_rules! convert_and_merge {
    ($column:expr, $builder:expr, $handle: expr, $db:expr) => {{
        let iter = $column.pairs_iterator($column.iter_start());
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (k, v) in iter {
            let asset_data = v.convert_to_fb(&mut $builder);
            $builder.finish_minimal(asset_data);
            batch.put_cf(
                $handle,
                Column::<AssetCompleteDetails>::encode_key(k),
                $builder.finished_data(),
            );
            $builder.reset();
        }
        $db.write(batch)?;
    }};
}

impl Storage {
    pub async fn apply_all_migrations(
        db_path: &str,
        migration_storage_path: &str,
        migration_version_manager: Arc<impl PrimaryStorageMigrationVersionManager>,
    ) -> Result<()> {
        let applied_migrations = migration_version_manager
            .get_all_applied_migrations()
            .map_err(StorageError::Common)?;
        let migration_applier =
            MigrationApplier::new(db_path, migration_storage_path, applied_migrations);

        // apply all migrations
        migration_applier
            .apply_migration(crate::migrations::collection_authority::CollectionAuthorityMigration)
            .await?;
        migration_applier
            .apply_migration(crate::migrations::external_plugins::ExternalPluginsMigration)
            .await?;
        migration_applier
            .apply_migration(
                crate::migrations::clean_update_authorities::CleanCollectionAuthoritiesMigration,
            )
            .await?;
        migration_applier
            .apply_migration(crate::migrations::spl2022::TokenAccounts2022ExtentionsMigration)
            .await?;
        migration_applier
            .apply_migration(
                crate::migrations::spl2022::DynamicDataToken2022MintExtentionsMigration,
            )
            .await?;
        Ok(())
    }

    pub async fn apply_migration_merge(&self) -> Result<()> {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(2500);
        convert_and_merge!(
            self.asset_static_data,
            builder,
            &self.asset_data.handle(),
            self.db
        );
        convert_and_merge!(
            self.asset_dynamic_data,
            builder,
            &self.asset_data.handle(),
            self.db
        );
        convert_and_merge!(
            self.asset_authority_data,
            builder,
            &self.asset_data.handle(),
            self.db
        );
        convert_and_merge!(
            self.asset_owner_data,
            builder,
            &self.asset_data.handle(),
            self.db
        );
        convert_and_merge!(
            self.asset_collection_data,
            builder,
            &self.asset_data.handle(),
            self.db
        );

        self.db.drop_cf(AssetStaticDetails::NAME)?;
        self.db.drop_cf(AssetDynamicDetails::NAME)?;
        self.db.drop_cf(AssetAuthority::NAME)?;
        self.db.drop_cf(AssetOwner::NAME)?;
        self.db.drop_cf(AssetCollection::NAME)?;

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

struct MigrationApplier<'a> {
    db_path: &'a str,
    migration_storage_path: &'a str,
    applied_migration_versions: HashSet<u64>,
}

impl<'a> MigrationApplier<'a> {
    fn new(
        db_path: &'a str,
        migration_storage_path: &'a str,
        applied_migration_versions: HashSet<u64>,
    ) -> Self {
        Self {
            db_path,
            migration_storage_path,
            applied_migration_versions,
        }
    }

    async fn apply_migration<M: RocksMigration>(&self, _: M) -> Result<()>
    where
        <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType: 'static + Clone,
        <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType: 'static + Hash + Eq,
    {
        if self.applied_migration_versions.contains(&M::VERSION) {
            return Ok(());
        }
        info!("Start executing migration Version {}", M::VERSION);
        let temporary_migration_storage =
            Self::open_migration_storage(self.migration_storage_path, M::VERSION)?;
        {
            let old_storage = Self::open_migration_storage(self.db_path, M::VERSION)?;
            Self::copy_data_to_temporary_storage::<M>(&old_storage, &temporary_migration_storage)?;
            old_storage
                .db
                .drop_cf(<<M as RocksMigration>::NewDataType as TypedColumn>::NAME)?;
        }
        let new_storage = Self::open_migration_storage(self.db_path, M::VERSION + 1)?;
        let column_to_migrate = Storage::column::<M::NewDataType>(
            new_storage.db.clone(),
            new_storage.red_metrics.clone(),
        );

        Self::migrate_data::<M>(&temporary_migration_storage, &column_to_migrate).await?;
        // Mark migration as applied and drop the temporary column family
        new_storage
            .migration_version
            .put_async(M::VERSION, MigrationVersions {})
            .await?;
        temporary_migration_storage
            .db
            .drop_cf(<<M as RocksMigration>::NewDataType as TypedColumn>::NAME)?;

        info!("Finish migration Version {}", M::VERSION);

        Ok(())
    }

    fn open_migration_storage(db_path: &str, version: u64) -> Result<Storage> {
        Storage::open(
            db_path,
            Arc::new(Mutex::new(JoinSet::new())),
            Arc::new(RequestErrorDurationMetrics::new()),
            MigrationState::Version(version),
        )
    }

    fn copy_data_to_temporary_storage<M: RocksMigration>(
        old_storage: &Storage,
        temporary_migration_storage: &Storage,
    ) -> Result<()>
    where
        <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType: 'static + Clone,
        <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType: 'static + Hash + Eq,
    {
        info!(
            "Start copying data into temporary storage Version {}",
            M::VERSION
        );

        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (key, value) in Self::migration_column_iter::<M>(&old_storage.db)? {
            batch.put_cf(
                &temporary_migration_storage
                    .db
                    .cf_handle(<<M as RocksMigration>::NewDataType as TypedColumn>::NAME)
                    .ok_or(StorageError::Common(format!(
                        "Cannot get cf_handle for {}",
                        <<M as RocksMigration>::NewDataType as TypedColumn>::NAME
                    )))?,
                key,
                value,
            );
            if batch.len() >= BATCH_SIZE {
                temporary_migration_storage.db.write(batch)?;
                batch = rocksdb::WriteBatchWithTransaction::<false>::default();
            }
        }
        temporary_migration_storage.db.write(batch)?;

        info!(
            "Finish copying data into temporary storage Version {}",
            M::VERSION
        );

        Ok(())
    }

    async fn migrate_data<M: RocksMigration>(
        temporary_migration_storage: &Storage,
        column: &Column<M::NewDataType>,
    ) -> Result<()>
    where
        <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType: 'static + Clone,
        <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType: 'static + Hash + Eq,
    {
        let mut batch = HashMap::new();
        for (key, value) in Self::migration_column_iter::<M>(&temporary_migration_storage.db)? {
            let key_decoded = match column.decode_key(key.to_vec()) {
                Ok(key_decoded) => key_decoded,
                Err(e) => {
                    error!("migration data decode_key: {:?}, {}", key.to_vec(), e);
                    continue;
                }
            };
            let Ok(value_decoded) = Self::decode_value::<M>(&value, &key_decoded) else {
                continue;
            };

            batch.insert(
                key_decoded,
                Into::<<M::NewDataType as TypedColumn>::ValueType>::into(value_decoded),
            );
            if batch.len() >= BATCH_SIZE {
                Self::put_batch_value::<M>(&mut batch, column).await?;
            }
        }
        Self::put_batch_value::<M>(&mut batch, column).await?;

        Ok(())
    }

    fn migration_column_iter<M: RocksMigration>(
        db: &Arc<DB>,
    ) -> Result<impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_> {
        Ok(db
            .iterator_cf(
                &db.cf_handle(<<M as RocksMigration>::NewDataType as TypedColumn>::NAME)
                    .ok_or(StorageError::Common(format!(
                        "Cannot get cf_handle for {}",
                        <<M as RocksMigration>::NewDataType as TypedColumn>::NAME
                    )))?,
                IteratorMode::Start,
            )
            .flatten())
    }

    fn decode_value<M: RocksMigration>(
        value: &[u8],
        key_decoded: &<M::NewDataType as TypedColumn>::KeyType,
    ) -> Result<M::OldDataType>
    where
        <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType: 'static + Clone,
        <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType: 'static + Hash + Eq,
    {
        match M::SERIALIZATION_TYPE {
            SerializationType::Bincode => deserialize::<M::OldDataType>(value).map_err(|e| {
                error!("migration data deserialize: {:?}, {}", key_decoded, e);
                e.into()
            }),
            SerializationType::Cbor => {
                serde_cbor::from_slice::<M::OldDataType>(value).map_err(|e| {
                    error!("migration data deserialize: {:?}, {}", key_decoded, e);
                    StorageError::Common(e.to_string())
                })
            }
        }
    }

    async fn put_batch_value<M: RocksMigration>(
        batch: &mut HashMap<
            <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType,
            <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType,
        >,
        column: &Column<M::NewDataType>,
    ) -> Result<()>
    where
        <<M as RocksMigration>::NewDataType as TypedColumn>::ValueType: 'static + Clone,
        <<M as RocksMigration>::NewDataType as TypedColumn>::KeyType: 'static + Hash + Eq,
    {
        match M::SERIALIZATION_TYPE {
            SerializationType::Bincode => column.put_batch(std::mem::take(batch)).await,
            SerializationType::Cbor => column.put_batch_cbor(std::mem::take(batch)).await,
        }
    }
}
