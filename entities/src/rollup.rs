use blockbuster::programs::bubblegum::{BubblegumInstruction, Payload};
use mpl_bubblegum::{InstructionName, LeafSchemaEvent};
use std::collections::HashMap;

use mpl_bubblegum::types::{LeafSchema, MetadataArgs, Version};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize)]
pub struct Rollup {
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub tree_id: Pubkey,
    pub rolled_mints: Vec<RolledMintInstruction>,
    pub raw_metadata_map: HashMap<String, Box<RawValue>>, // map by uri
    pub max_depth: u32,
    pub max_buffer_size: u32,

    // derived data
    pub merkle_root: [u8; 32],    // validate
    pub last_leaf_hash: [u8; 32], // validate
}

#[derive(Serialize, Deserialize)]
pub struct RolledMintInstruction {
    pub tree_update: ChangeLogEventV1, // validate // derive from nonce
    pub leaf_update: LeafSchema,       // validate
    pub mint_args: MetadataArgs,
    // V0.1: enforce collection.verify == false
    // V0.1: enforce creator.verify == false
    // V0.2: add pub collection_signature: Option<Signature> - sign asset_id with collection authority
    // V0.2: add pub creator_signature: Option<Map<Pubkey, Signature>> - sign asset_id with creator authority to ensure verified creator
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub authority: Pubkey,
}

#[derive(Default)]
pub struct BatchMintInstruction {
    pub max_depth: u32,
    pub max_buffer_size: u32,
    pub num_minted: u64,
    pub root: [u8; 32],
    pub leaf: [u8; 32],
    pub index: u32,
    pub metadata_url: String,
}

#[derive(Serialize, Deserialize)]
pub struct ChangeLogEventV1 {
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub id: Pubkey,
    pub path: Vec<PathNode>,
    pub seq: u64,
    pub index: u32,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

impl From<&PathNode> for spl_account_compression::state::PathNode {
    fn from(value: &PathNode) -> Self {
        Self {
            node: value.node,
            index: value.index,
        }
    }
}
impl From<spl_account_compression::state::PathNode> for PathNode {
    fn from(value: spl_account_compression::state::PathNode) -> Self {
        Self {
            node: value.node,
            index: value.index,
        }
    }
}
impl From<&ChangeLogEventV1> for blockbuster::programs::bubblegum::ChangeLogEventV1 {
    fn from(value: &ChangeLogEventV1) -> Self {
        Self {
            id: value.id,
            path: value.path.iter().map(Into::into).collect::<Vec<_>>(),
            seq: value.seq,
            index: value.index,
        }
    }
}
impl From<blockbuster::programs::bubblegum::ChangeLogEventV1> for ChangeLogEventV1 {
    fn from(value: blockbuster::programs::bubblegum::ChangeLogEventV1) -> Self {
        Self {
            id: value.id,
            path: value.path.into_iter().map(Into::into).collect::<Vec<_>>(),
            seq: value.seq,
            index: value.index,
        }
    }
}

impl From<RolledMintInstruction> for BubblegumInstruction {
    fn from(value: RolledMintInstruction) -> Self {
        let hash = value.leaf_update.hash();
        Self {
            instruction: InstructionName::MintV1,
            tree_update: Some((&value.tree_update).into()),
            leaf_update: Some(LeafSchemaEvent::new(Version::V1, value.leaf_update, hash)),
            payload: Some(Payload::MintV1 {
                args: value.mint_args,
                authority: value.authority,
                tree_id: value.tree_update.id,
            }),
        }
    }
}
