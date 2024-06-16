use crate::asset::{update_field, update_optional_field};
use crate::column::TypedColumn;
use crate::migrator::{RocksMigration, SerializationType};
use crate::AssetDynamicDetails;
use bincode::{deserialize, serialize};
use entities::enums::ChainMutability;
use entities::models::Updated;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetDynamicDetailsV0 {
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
    pub plugins: Option<Updated<String>>,
    pub unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
}

impl From<AssetDynamicDetailsV0> for AssetDynamicDetails {
    fn from(value: AssetDynamicDetailsV0) -> Self {
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
            mpl_core_plugins: value.plugins,
            mpl_core_unknown_plugins: value.unknown_plugins,
            rent_epoch: value.rent_epoch,
            num_minted: value.num_minted,
            current_size: value.current_size,
            plugins_json_version: value.plugins_json_version,
            mpl_core_external_plugins: None,
            mpl_core_unknown_external_plugins: None,
        }
    }
}

impl AssetDynamicDetailsV0 {
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
                        "RocksDB: AssetDynamicDetailsV0 deserialize existing_val: {}",
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
                        update_optional_field(&mut current_val.plugins, &new_val.plugins);
                        update_optional_field(
                            &mut current_val.unknown_plugins,
                            &new_val.unknown_plugins,
                        );
                        update_optional_field(&mut current_val.num_minted, &new_val.num_minted);
                        update_optional_field(&mut current_val.current_size, &new_val.current_size);
                        update_optional_field(&mut current_val.rent_epoch, &new_val.rent_epoch);
                        update_optional_field(
                            &mut current_val.plugins_json_version,
                            &new_val.plugins_json_version,
                        );

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!("RocksDB: AssetDynamicDetailsV0 deserialize new_val: {}", e)
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

pub(crate) struct ExternalPluginsMigration;
impl RocksMigration for ExternalPluginsMigration {
    const VERSION: u64 = 1;
    const COLUMN_TO_MIGRATE: &'static str = AssetDynamicDetails::NAME;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = AssetDynamicDetails;
    type OldDataType = AssetDynamicDetailsV0;
}
