use entities::models::Updated;

use crate::{
    columns::cl_items::{ClItemDeprecated, ClItemV2},
    migrator::{RocksMigration, SerializationType},
};

pub(crate) struct ClItemsV2Migration;
impl RocksMigration for ClItemsV2Migration {
    const VERSION: u64 = 5;
    const DESERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = ClItemV2;
    type OldDataType = ClItemDeprecated;
}

impl From<ClItemDeprecated> for ClItemV2 {
    fn from(old: ClItemDeprecated) -> Self {
        ClItemV2 {
            tree_key: old.cli_tree_key,
            level: old.cli_level,
            node_idx: old.cli_node_idx,
            leaf_idx: old.cli_leaf_idx,
            finalized_hash: None,
            pending_hash: Some(Updated::new(
                old.slot_updated,
                Some(entities::models::UpdateVersion::Sequence(old.cli_seq)),
                old.cli_hash,
            )),
        }
    }
}
