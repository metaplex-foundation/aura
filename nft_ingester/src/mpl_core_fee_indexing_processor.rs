use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::mplx_updates_processor::CoreAssetFee;
use crate::process_accounts;
use anchor_lang::prelude::borsh::{BorshDeserialize, BorshSerialize};
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use solana_program::rent::Rent;
use solana_program::sysvar::Sysvar;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use num_derive::{FromPrimitive, ToPrimitive};
use shank::ShankAccount;
use tokio::sync::broadcast::Receiver;

pub(crate) const COLLECT_FEE_AMOUNT_LAMPORTS: u64 = 1_500_000;

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_mpl_asset_at: Option<SystemTime>,
}

pub trait DataBlob: BorshSerialize + BorshDeserialize {
    /// Get the size of an empty instance of the data blob.
    fn get_initial_size() -> usize;
    /// Get the current size of the data blob.
    fn get_size(&self) -> usize;
}

/// An enum representing account discriminators.
#[derive(
Clone, Copy, BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, ToPrimitive, FromPrimitive,
)]
pub enum Key {
    /// Uninitialized or invalid account.
    Uninitialized,
    /// An account holding an uncompressed asset.
    AssetV1,
    /// An account holding a compressed asset.
    HashedAssetV1,
    /// A discriminator indicating the plugin header.
    PluginHeaderV1,
    /// A discriminator indicating the plugin registry.
    PluginRegistryV1,
    /// A discriminator indicating the collection.
    CollectionV1,
}

impl Key {
    /// Get the size of the Key.
    pub fn get_initial_size() -> usize {
        1
    }
}

/// An enum representing the types of accounts that can update data on an asset.
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, Eq, PartialEq)]
pub enum UpdateAuthority {
    /// No update authority, used for immutability.
    None,
    /// A standard address or PDA.
    Address(Pubkey),
    /// Authority delegated to a collection.
    Collection(Pubkey),
}


#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, ShankAccount, Eq, PartialEq)]
pub struct AssetV1 {
    /// The account discriminator.
    pub key: Key, //1
    /// The owner of the asset.
    pub owner: Pubkey, //32
    //TODO: Fix this for dynamic size
    /// The update authority of the asset.
    pub update_authority: UpdateAuthority, //33
    /// The name of the asset.
    pub name: String, //4
    /// The URI of the asset that points to the off-chain data.
    pub uri: String, //4
    /// The sequence number used for indexing with compression.
    pub seq: Option<u64>, //1
}

impl DataBlob for AssetV1 {
    fn get_initial_size() -> usize {
        AssetV1::BASE_LENGTH
    }

    fn get_size(&self) -> usize {
        let mut size = AssetV1::BASE_LENGTH + self.name.len() + self.uri.len();
        if self.seq.is_some() {
            size += size_of::<u64>();
        }
        size
    }
}

impl AssetV1 {
    pub const BASE_LENGTH: usize = 1 + 32 + 33 + 4 + 4 + 1;
}

impl MplCoreFeeProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            buffer,
            batch_size,
            metrics,
            last_received_mpl_asset_at: None,
        }
    }

    pub async fn process(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.mpl_core_fee_assets,
            self.batch_size,
            |s: CoreAssetFee| s,
            self.last_received_mpl_asset_at,
            Self::store_mpl_assets_fee,
            "mpl_core_asset_fee"
        );
    }

    pub async fn store_mpl_assets_fee(&self, metadata_info: &HashMap<Pubkey, CoreAssetFee>) {
        // let mut fees = HashMap::new();
        for (pk, asset_fee) in metadata_info.iter() {
            let Ok(rent) = Self::calculate_rent_amount(asset_fee) else {
                continue;
            };
            if asset_fee.lamports
                > rent
                    .checked_add(COLLECT_FEE_AMOUNT_LAMPORTS)
                    .unwrap_or_default()
            {
                self.rocks_db.save_non_paid_asset(*pk)?;
            } else {
                self.rocks_db.save_non_paid_asset(*pk)?;
            };

            // fees.insert(*pk, rent_paid)
        }

        let begin_processing = Instant::now();
        self.metrics.set_latency(
            "mpl_core_asset_fee",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    fn calculate_rent_amount(account_info: &CoreAssetFee) -> Result<u64, IngesterError> {
        let rent = Rent::get()?;
        let rent_amount = match &account_info.indexable_asset {
            MplCoreAccountData::EmptyAccount => {
                account_info.assign(&system_program::ID);
                rent.minimum_balance(1)
            }
            MplCoreAccountData::Asset(_) => {
                let (asset, header, registry) = Self::fetch_core_data::<AssetV1>(account_info.data.as_slice())?;
                let header_size = match header {
                    Some(header) => header.get_size(),
                    None => 0,
                };
                let registry_size = match registry {
                    Some(registry) => registry.get_size(),
                    None => 0,
                };
                rent.minimum_balance(
                    asset
                        .get_size()
                        .checked_add(header_size)
                        .ok_or(MplCoreError::NumericalOverflowError)?
                        .checked_add(registry_size)
                        .ok_or(MplCoreError::NumericalOverflowError)?,
                )
            }
            MplCoreAccountData::HashedAsset => {
                rent.minimum_balance(HashedAssetV1::LENGTH);
            }
            _ => return Err(MplCoreError::IncorrectAccount.into()),
        };

        Ok(rent_amount)
    }

    fn load<T: BorshDeserialize>(data: &[u8], offset: u64) -> Result<T, IngesterError> {
        let mut bytes: &[u8] = data[offset..];
        T::deserialize(&mut bytes).map_err(|e| IngesterError::DeserializationError(e.to_string()))
    }

    fn fetch_core_data<T: DataBlob>(data: &[u8]) -> Result<(T, Option<PluginHeaderV1>, Option<PluginRegistryV1>), IngesterError> {
        let asset = Self::load(data, 0)?;

        if asset.get_size() != data.len() {
            let plugin_header = PluginHeaderV1::load(account, asset.get_size())?;
            let plugin_registry =
                PluginRegistryV1::load(account, plugin_header.plugin_registry_offset)?;

            Ok((asset, Some(plugin_header), Some(plugin_registry)))
        } else {
            Ok((asset, None, None))
        }
    }
}
