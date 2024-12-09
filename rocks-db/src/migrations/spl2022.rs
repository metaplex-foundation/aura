use crate::asset::{update_field, update_optional_field};
use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::migrator::{RocksMigration, SerializationType};
use crate::{impl_merge_values, AssetDynamicDetails};
use bincode::{deserialize, serialize};
use entities::enums::ChainMutability;
use entities::models::{TokenAccount, Updated};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::log::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAccountWithoutExtentions {
    pub pubkey: Pubkey,
    pub mint: Pubkey,
    pub delegate: Option<Pubkey>,
    pub owner: Pubkey,
    pub frozen: bool,
    pub delegated_amount: i64,
    pub slot_updated: i64,
    pub amount: i64,
    pub write_version: u64,
}

impl_merge_values!(TokenAccountWithoutExtentions);

impl From<TokenAccountWithoutExtentions> for TokenAccount {
    fn from(value: TokenAccountWithoutExtentions) -> Self {
        Self {
            pubkey: value.pubkey,
            mint: value.mint,
            delegate: value.delegate,
            owner: value.owner,
            extensions: None,
            frozen: value.frozen,
            delegated_amount: value.delegated_amount,
            slot_updated: value.slot_updated,
            amount: value.amount,
            write_version: value.write_version,
        }
    }
}

pub(crate) struct TokenAccounts2022ExtentionsMigration;
impl RocksMigration for TokenAccounts2022ExtentionsMigration {
    const VERSION: u64 = 3;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = TokenAccount;
    type OldDataType = TokenAccountWithoutExtentions;
}
