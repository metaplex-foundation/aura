use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClItemsModel {
    pub id: i64,
    pub tree: Vec<u8>,
    pub node_idx: i64,
    pub leaf_idx: Option<i64>,
    pub seq: i64,
    pub level: i64,
    pub hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChainMutability {
    Immutable,
    Mutable,
    Unknown,
}

impl From<ChainMutability> for bool {
    fn from(s: ChainMutability) -> Self {
        match s {
            ChainMutability::Mutable => true,
            ChainMutability::Immutable => false,
            _ => true,
        }
    }
}

impl From<entities::enums::ChainMutability> for ChainMutability {
    fn from(value: entities::enums::ChainMutability) -> Self {
        match value {
            entities::enums::ChainMutability::Immutable => ChainMutability::Immutable,
            entities::enums::ChainMutability::Mutable => ChainMutability::Mutable,
        }
    }
}
