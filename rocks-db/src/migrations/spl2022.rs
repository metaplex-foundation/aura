use crate::asset::{update_field, update_optional_field};
use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::migrator::{RocksMigration, SerializationType};
use crate::AssetDynamicDetails;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetDynamicDetailsWithoutExtentions {
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
    pub mpl_core_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
    pub mpl_core_external_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_external_plugins: Option<Updated<String>>,
}

impl TypedColumn for AssetDynamicDetailsWithoutExtentions {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_DYNAMIC_V2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetDynamicDetailsWithoutExtentions {
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
                        "RocksDB: AssetDynamicDetailsWithoutExtentions deserialize existing_val: {}",
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
                        update_optional_field(
                            &mut current_val.mpl_core_plugins,
                            &new_val.mpl_core_plugins,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_unknown_plugins,
                            &new_val.mpl_core_unknown_plugins,
                        );
                        update_optional_field(&mut current_val.num_minted, &new_val.num_minted);
                        update_optional_field(&mut current_val.current_size, &new_val.current_size);
                        update_optional_field(&mut current_val.rent_epoch, &new_val.rent_epoch);
                        update_optional_field(
                            &mut current_val.plugins_json_version,
                            &new_val.plugins_json_version,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_external_plugins,
                            &new_val.mpl_core_external_plugins,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_unknown_external_plugins,
                            &new_val.mpl_core_unknown_external_plugins,
                        );

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!(
                        "RocksDB: AssetDynamicDetailsWithoutExtentions deserialize new_val: {}",
                        e
                    )
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

impl From<AssetDynamicDetailsWithoutExtentions> for AssetDynamicDetails {
    fn from(value: AssetDynamicDetailsWithoutExtentions) -> Self {
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
            mpl_core_plugins: value.mpl_core_plugins,
            mpl_core_unknown_plugins: value.mpl_core_unknown_plugins,
            rent_epoch: value.rent_epoch,
            num_minted: value.num_minted,
            current_size: value.current_size,
            plugins_json_version: value.plugins_json_version,
            mpl_core_external_plugins: value.mpl_core_external_plugins,
            mpl_core_unknown_external_plugins: value.mpl_core_unknown_external_plugins,
            mint_extensions: None,
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

pub(crate) struct DynamicDataToken2022MintExtentionsMigration;
impl RocksMigration for DynamicDataToken2022MintExtentionsMigration {
    const VERSION: u64 = 4;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = AssetDynamicDetails;
    type OldDataType = AssetDynamicDetailsWithoutExtentions;
}
