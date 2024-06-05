use crate::asset::{update_field, update_optional_field};
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::migrator::{MigrationState, MigrationVersions, BATCH_SIZE};
use crate::{AssetDynamicDetails, Storage};
use bincode::{deserialize, serialize};
use entities::enums::ChainMutability;
use entities::models::Updated;
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
pub struct AssetDynamicDetailsV0 {
    pub pubkey: Pubkey,
    pub is_compressible: Updated<bool>,
    pub is_compressed: Updated<bool>,
    pub is_frozen: Updated<bool>,
    pub supply: Option<Updated<u64>>,
    pub seq: Option<Updated<u64>>,
    pub is_burnt: Updated<bool>,
    pub was_decompressed: Updated<bool>,
    pub onchain_data: Option<Updated<String>>,
    pub creators: Updated<Vec<entities::models::Creator>>,
    pub royalty_amount: Updated<u16>,
    pub url: Updated<String>,
    pub chain_mutability: Option<Updated<ChainMutability>>,
    pub lamports: Option<Updated<u64>>,
    pub executable: Option<Updated<bool>>,
    pub metadata_owner: Option<Updated<String>>,
    pub raw_name: Option<Updated<String>>,
    pub plugins: Option<Updated<String>>,
    pub unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
}

impl From<AssetDynamicDetailsV0> for AssetDynamicDetails {
    fn from(value: AssetDynamicDetailsV0) -> Self {
        Self {
            pubkey: value.pubkey,
            is_compressible: value.is_compressible,
            is_compressed: value.is_compressed,
            is_frozen: value.is_frozen,
            supply: value.supply,
            seq: value.seq,
            is_burnt: value.is_burnt,
            was_decompressed: value.was_decompressed,
            onchain_data: value.onchain_data,
            creators: value.creators,
            royalty_amount: value.royalty_amount,
            url: value.url,
            chain_mutability: value.chain_mutability,
            lamports: value.lamports,
            executable: value.executable,
            metadata_owner: value.metadata_owner,
            raw_name: value.raw_name,
            mpl_core_plugins: value.plugins,
            mpl_core_unknown_plugins: value.unknown_plugins,
            rent_epoch: value.rent_epoch,
            num_minted: value.num_minted,
            current_size: value.current_size,
            plugins_json_version: value.plugins_json_version,
            mpl_core_external_plugins: None,
            mpl_core_unknown_external_plugins: None,
        }
    }
}

impl AssetDynamicDetailsV0 {
    pub fn merge_dynamic_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                }
                Err(e) => {
                    error!(
                        "RocksDB: AssetDynamicDetailsV0 deserialize existing_val: {}",
                        e
                    )
                }
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.is_compressible, &new_val.is_compressible);
                        update_field(&mut current_val.is_compressed, &new_val.is_compressed);
                        update_field(&mut current_val.is_frozen, &new_val.is_frozen);
                        update_optional_field(&mut current_val.supply, &new_val.supply);
                        update_optional_field(&mut current_val.seq, &new_val.seq);
                        update_field(&mut current_val.is_burnt, &new_val.is_burnt);
                        update_field(&mut current_val.creators, &new_val.creators);
                        update_field(&mut current_val.royalty_amount, &new_val.royalty_amount);
                        update_field(&mut current_val.was_decompressed, &new_val.was_decompressed);
                        update_optional_field(&mut current_val.onchain_data, &new_val.onchain_data);
                        update_field(&mut current_val.url, &new_val.url);
                        update_optional_field(
                            &mut current_val.chain_mutability,
                            &new_val.chain_mutability,
                        );
                        update_optional_field(&mut current_val.lamports, &new_val.lamports);
                        update_optional_field(&mut current_val.executable, &new_val.executable);
                        update_optional_field(
                            &mut current_val.metadata_owner,
                            &new_val.metadata_owner,
                        );
                        update_optional_field(&mut current_val.raw_name, &new_val.raw_name);
                        update_optional_field(&mut current_val.plugins, &new_val.plugins);
                        update_optional_field(
                            &mut current_val.unknown_plugins,
                            &new_val.unknown_plugins,
                        );
                        update_optional_field(&mut current_val.num_minted, &new_val.num_minted);
                        update_optional_field(&mut current_val.current_size, &new_val.current_size);
                        update_optional_field(&mut current_val.rent_epoch, &new_val.rent_epoch);
                        update_optional_field(
                            &mut current_val.plugins_json_version,
                            &new_val.plugins_json_version,
                        );

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!("RocksDB: AssetDynamicDetailsV0 deserialize new_val: {}", e)
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

pub(crate) async fn apply_migration(
    db_path: &str,
    migration_storage_path: &str,
) -> crate::Result<()> {
    info!("Start executing migration V1");
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
            MigrationState::Version(1),
        )?;
        let iter = old_storage.db.iterator_cf(
            &old_storage
                .db
                .cf_handle(AssetDynamicDetails::NAME)
                .ok_or(StorageError::Common(
                    "Cannot get cf_handle for AssetDynamicDetails".to_string(),
                ))?,
            IteratorMode::Start,
        );

        info!("Start coping data into temporary storage V1");
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (key, value) in iter.flatten() {
            batch.put_cf(
                &temporary_migration_storage
                    .db
                    .cf_handle(AssetDynamicDetails::NAME)
                    .ok_or(StorageError::Common(
                        "Cannot get cf_handle for AssetDynamicDetails".to_string(),
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

        info!("Finish coping data into temporary storage V1");
        old_storage.db.drop_cf(AssetDynamicDetails::NAME)?;
    }
    let new_storage = Storage::open(
        db_path,
        Arc::new(Mutex::new(JoinSet::new())),
        Arc::new(RequestErrorDurationMetrics::new()),
        MigrationState::Version(2),
    )?;
    let mut batch = HashMap::new();
    for (key, value) in temporary_migration_storage
        .asset_dynamic_data
        .iter_start()
        .filter_map(std::result::Result::ok)
    {
        let key_decoded = match temporary_migration_storage
            .asset_dynamic_data
            .decode_key(key.to_vec())
        {
            Ok(key_decoded) => key_decoded,
            Err(e) => {
                error!("dynamic data decode_key: {:?}, {}", key.to_vec(), e);
                continue;
            }
        };
        let value_decoded = match deserialize::<AssetDynamicDetailsV0>(&value) {
            Ok(value_decoded) => value_decoded,
            Err(e) => {
                error!("dynamic data deserialize: {}, {}", key_decoded, e);
                continue;
            }
        };
        batch.insert(key_decoded, value_decoded.into());
        if batch.len() >= BATCH_SIZE {
            new_storage
                .asset_dynamic_data
                .put_batch(std::mem::take(&mut batch))
                .await?;
        }
    }
    new_storage.asset_dynamic_data.put_batch(batch).await?;
    info!("Finish migration V1");

    new_storage
        .migration_version
        .put_async(1, MigrationVersions {})
        .await?;
    temporary_migration_storage
        .db
        .drop_cf(AssetDynamicDetails::NAME)?;
    Ok(())
}
