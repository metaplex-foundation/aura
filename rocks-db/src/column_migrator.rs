use crate::{AssetDynamicDetails, AssetOwner, Storage};
use bincode::deserialize;
use entities::enums::{ChainMutability, OwnerType};
use entities::models::Updated;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OldAssetDynamicDetails {
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
}

impl From<OldAssetDynamicDetails> for AssetDynamicDetails {
    fn from(value: OldAssetDynamicDetails) -> Self {
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
            raw_name: None,
            plugins: None,
            unknown_plugins: None,
            rent_epoch: None,
            num_minted: None,
            current_size: None,
            plugins_json_version: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OldAssetOwner {
    pub pubkey: Pubkey,
    pub owner: Updated<Pubkey>,
    pub delegate: Updated<Option<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Updated<Option<u64>>,
}

impl From<OldAssetOwner> for AssetOwner {
    fn from(value: OldAssetOwner) -> Self {
        Self {
            pubkey: value.pubkey,
            owner: Updated::new(
                value.owner.slot_updated,
                value.owner.update_version,
                Some(value.owner.value),
            ),
            delegate: value.delegate,
            owner_type: value.owner_type,
            owner_delegate_seq: value.owner_delegate_seq,
        }
    }
}

impl Storage {
    pub async fn migrate_columns(&self) {
        info!("Start migrating columns");
        self.migrate_dynamic_data().await;
        self.migrate_owner_data().await;
        info!("Columns migrated");
    }

    async fn migrate_dynamic_data(&self) {
        for (key, value) in self.asset_dynamic_data.iter_start().filter_map(Result::ok) {
            let key_decoded = match self.asset_dynamic_data.decode_key(key.to_vec()) {
                Ok(key_decoded) => key_decoded,
                Err(e) => {
                    error!("dynamic data decode_key: {:?}, {}", key.to_vec(), e);
                    continue;
                }
            };
            let value_decoded = match deserialize::<OldAssetDynamicDetails>(&value) {
                Ok(value_decoded) => value_decoded,
                Err(e) => {
                    error!("dynamic data deserialize: {}, {}", key_decoded, e);
                    continue;
                }
            };
            if let Err(e) = self
                .asset_dynamic_data
                .put_async(key_decoded, value_decoded.into())
                .await
            {
                error!("dynamic data put_async: {}, {}", key_decoded, e)
            };
        }
    }

    async fn migrate_owner_data(&self) {
        for (key, value) in self.asset_owner_data.iter_start().filter_map(Result::ok) {
            let key_decoded = match self.asset_owner_data.decode_key(key.to_vec()) {
                Ok(key_decoded) => key_decoded,
                Err(e) => {
                    error!("owner data decode_key: {:?}, {}", key.to_vec(), e);
                    continue;
                }
            };
            let value_decoded = match deserialize::<OldAssetOwner>(&value) {
                Ok(value_decoded) => value_decoded,
                Err(e) => {
                    error!("owner data deserialize: {}, {}", key_decoded, e);
                    continue;
                }
            };
            if let Err(e) = self
                .asset_owner_data
                .put_async(key_decoded, value_decoded.into())
                .await
            {
                error!("owner data put_async: {}, {}", key_decoded, e)
            };
        }
    }
}
