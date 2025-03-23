use solana_sdk::pubkey::Pubkey;

use crate::{
    columns::asset::{AssetLeaf, AssetLeafDeprecated},
    migrator::{RocksMigration, SerializationType},
};

pub(crate) struct ClItemsV2Migration;
impl RocksMigration for ClItemsV2Migration {
    const VERSION: u64 = 6;
    const DESERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type KeyType = Pubkey;
    type NewDataType = AssetLeaf;
    type OldDataType = AssetLeafDeprecated;
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
