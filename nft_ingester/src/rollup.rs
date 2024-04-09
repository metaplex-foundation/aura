use std::collections::HashMap;

use mpl_bubblegum::types::MetadataArgs;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize)]
pub struct Rollup {
    pub tree_authority: Pubkey,
    pub tree_nonce: u64,

    pub rolled_mints: Vec<RolledMintInstruction>,
    pub raw_metadata_map: HashMap<String, Box<RawValue>>, // map by uri

    // derived data
    pub tree_id: Pubkey, // derived from the tree authority and nonce PDA("rollup", tree_authority, tree_nonce) // validate
    pub merkle_root: Pubkey, // validate
    pub last_leaf_hash: [u8; 32], // validate
}

#[derive(Serialize, Deserialize)]
pub struct RolledMintInstruction {
    // pub tree_update: ChangeLogEvent, // validate // derive from nonce
    // pub leaf_update: LeafSchema, // validate
    pub mint_args: MetadataArgs,
    // V0.1: enforce collection.verify == false
    // V0.1: enforce creator.verify == false
    // V0.2: add pub collection_signature: Option<Signature> - sign asset_id with collection authority
    // V0.2: add pub creator_signature: Option<Map<Pubkey, Signature>> - sign asset_id with creator authority to ensure verified creator
    pub authority: Pubkey,
    pub owner: Pubkey,
    pub delegate: Pubkey,
    pub nonce: u64, // equals index in the rolled_mints vector?

    // derived data
    pub id: Pubkey, // PDA("asset", tree_id, nonce?) // validate
}
