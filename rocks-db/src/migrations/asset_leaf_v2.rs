use std::cmp::Ordering;

use bincode::{deserialize, serialize};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use tracing::error;

use crate::{
    column::TypedColumn,
    columns::asset::AssetLeaf,
    key_encoders::{decode_pubkey, encode_pubkey},
    migrator::{RocksMigration, SerializationType},
};

pub(crate) struct AssetLeafV2Migration;
impl RocksMigration for AssetLeafV2Migration {
    const VERSION: u64 = 6;
    const DESERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type KeyType = Pubkey;
    type NewDataType = AssetLeaf;
    type OldDataType = AssetLeafDeprecated;
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeafDeprecated {
    pub pubkey: Pubkey,
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SourcedAssetLeafDeprecated {
    pub leaf: AssetLeafDeprecated,
    pub is_from_finalized_source: bool,
}

impl TypedColumn for AssetLeafDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_LEAF";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl From<AssetLeafDeprecated> for AssetLeaf {
    fn from(old: AssetLeafDeprecated) -> Self {
        AssetLeaf {
            pubkey: old.pubkey,
            tree_id: old.tree_id,
            leaf: old.leaf,
            nonce: old.nonce,
            data_hash: old.data_hash,
            creator_hash: old.creator_hash,
            leaf_seq: old.leaf_seq,
            slot_updated: old.slot_updated,
            collection_hash: None,
            asset_data_hash: None,
            flags: None,
        }
    }
}

impl AssetLeafDeprecated {
    pub fn merge_asset_leaf(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0u64;
        let mut leaf_seq: Option<u64> = None;

        // Decode existing value as AssetLeaf, since historically only AssetLeaf was stored.
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetLeafDeprecated>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    leaf_seq = value.leaf_seq;
                    result = existing_val.to_vec();
                },
                Err(e) => {
                    error!("RocksDB: AssetLeaf deserialize existing_val: {}", e);
                },
            }
        }

        let len = operands.len();

        for (i, op) in operands.iter().enumerate() {
            // Try to decode operand as SourcedAssetLeaf first
            let new_val = match deserialize::<SourcedAssetLeafDeprecated>(op) {
                Ok(si) => si,
                Err(_e_sourced) => {
                    // If fails, try decoding as AssetLeaf
                    match deserialize::<AssetLeafDeprecated>(op) {
                        Ok(al) => {
                            SourcedAssetLeafDeprecated { leaf: al, is_from_finalized_source: false }
                        },
                        Err(e_leaf) => {
                            error!(
                                "RocksDB: AssetLeaf deserialize new_val failed: {}. Data: {:?}",
                                e_leaf, op
                            );
                            // If last operand and still no result chosen, store empty if needed
                            if i == len - 1 && result.is_empty() {
                                error!(
                                    "RocksDB: last operand in AssetLeaf new_val could not be \
                                     deserialized as SourcedAssetLeaf or AssetLeaf. Empty array will be saved: {}",
                                    e_leaf
                                );
                                return Some(vec![]);
                            } else {
                                error!("RocksDB: AssetLeaf deserialize new_val failed: {}", e_leaf);
                            }
                            continue;
                        },
                    }
                },
            };

            let new_slot = new_val.leaf.slot_updated;
            let new_seq = new_val.leaf.leaf_seq;

            // Determine if this new value outranks the existing one
            // Outranking conditions:
            // 1. Higher slot than current.
            // 2. If slot is equal, but leaf_seq is strictly greater.
            // 3. If from a finalized source and has a strictly greater leaf_seq than current.
            let newer = match new_slot.cmp(&slot) {
                Ordering::Greater => true,
                Ordering::Equal => match (leaf_seq, new_seq) {
                    (Some(current_seq), Some(candidate_seq)) => candidate_seq > current_seq,
                    (None, Some(_)) => true, // previously no sequence, now we have one, lets use it
                    _ => false, // either both none or candidate_seq is none and current_seq is some
                },
                Ordering::Less => false,
            };

            let finalized_newer = new_val.is_from_finalized_source
                && match (leaf_seq, new_seq) {
                    (Some(current_seq), Some(candidate_seq)) => candidate_seq > current_seq,
                    (None, Some(_)) => true, // previously no sequence, now we have one
                    _ => false,              // same logic as above
                };

            if newer || finalized_newer {
                // If this new_val outranks the existing value:
                // store only the AssetLeaf portion
                match serialize(&new_val.leaf) {
                    Ok(serialized) => {
                        result = serialized;
                        slot = new_slot;
                        leaf_seq = new_seq;
                    },
                    Err(e) => {
                        error!(
                            "RocksDB: Failed to serialize AssetLeaf from SourcedAssetLeaf: {}",
                            e
                        );
                    },
                }
            }
        }

        Some(result)
    }
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
