use crate::asset::AssetCollection;
use crate::migrator::{RocksMigration, SerializationType};
use crate::ToFlatbuffersConverter;
use bincode::deserialize;
use entities::models::{UpdateVersion, Updated};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

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

pub(crate) struct CollectionAuthorityMigration;
impl RocksMigration for CollectionAuthorityMigration {
    const VERSION: u64 = 0;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = AssetCollection;
    type OldDataType = AssetCollectionVersion0;
}
