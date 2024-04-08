use mpl_bubblegum::types::{LeafSchema, MetadataArgs};
use serde_json::value::RawValue;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::events::ChangeLogEvent;

// todo: add serde derive
// #[derive(Serialize, Deserialize)]
pub struct RolledMintInstruction {
    pub tree_update: ChangeLogEvent,
    pub leaf_update: LeafSchema,
    pub mint_args: MetadataArgs,
    pub authority: Pubkey,
    pub raw_metadata: Box<RawValue>,
}

// todo: add serde derive
// #[derive(Serialize, Deserialize)]
pub struct Rollup {
    pub tree_id: Pubkey,
    pub tree_authority: Pubkey,

    pub rolled_mints: Vec<RolledMintInstruction>,
}
