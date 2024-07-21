use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::mplx_updates_processor::CoreAssetFee;
use crate::process_accounts;
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use borsh::{BorshDeserialize, BorshSerialize};
use entities::models::CoreFee;
use metrics_utils::IngesterMetricsConfig;
use num_derive::{FromPrimitive, ToPrimitive};
use postgre_client::PgClient;
use shank::ShankAccount;
use solana_program::pubkey::Pubkey;
use solana_program::rent::Rent;
use solana_program::sysvar::Sysvar;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use strum::EnumCount;
use tokio::sync::broadcast::Receiver;
use tracing::error;

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub storage: Arc<PgClient>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_mpl_asset_at: Option<SystemTime>,
}

pub trait DataBlob {
    /// Get the size of an empty instance of the data blob.
    fn get_initial_size() -> usize;
    /// Get the current size of the data blob.
    fn get_size(&self) -> usize;
}

/// The structure representing the hash of the asset.
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, ShankAccount, PartialEq, Eq)]
pub struct HashedAssetV1 {
    /// The account discriminator.
    pub key: Key, //1
    /// The hash of the asset content.
    pub hash: [u8; 32], //32
}

impl HashedAssetV1 {
    /// The length of the hashed asset account.
    pub const LENGTH: usize = 1 + 32;

    /// Create a new hashed asset.
    pub fn new(hash: [u8; 32]) -> Self {
        Self {
            key: Key::HashedAssetV1,
            hash,
        }
    }
}

impl DataBlob for HashedAssetV1 {
    fn get_initial_size() -> usize {
        HashedAssetV1::LENGTH
    }

    fn get_size(&self) -> usize {
        HashedAssetV1::LENGTH
    }
}

/// The plugin header is the first part of the plugin metadata.
/// This field stores the Key
/// And a pointer to the Plugin Registry stored at the end of the account.
#[repr(C)]
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, ShankAccount)]
pub struct PluginHeaderV1 {
    /// The Discriminator of the header which doubles as a Plugin metadata version.
    pub key: Key, // 1
    /// The offset to the plugin registry stored at the end of the account.
    pub plugin_registry_offset: usize, // 8
}

impl DataBlob for PluginHeaderV1 {
    fn get_initial_size() -> usize {
        1 + 8
    }

    fn get_size(&self) -> usize {
        1 + 8
    }
}

/// List of first party plugin types.
#[repr(C)]
#[derive(
    Clone,
    Copy,
    Debug,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    ToPrimitive,
    EnumCount,
    PartialOrd,
    Ord,
)]
pub enum PluginType {
    /// Royalties plugin.
    Royalties,
    /// Freeze Delegate plugin.
    FreezeDelegate,
    /// Burn Delegate plugin.
    BurnDelegate,
    /// Transfer Delegate plugin.
    TransferDelegate,
    /// Update Delegate plugin.
    UpdateDelegate,
    /// The Permanent Freeze Delegate plugin.
    PermanentFreezeDelegate,
    /// The Attributes plugin.
    Attributes,
    /// The Permanent Transfer Delegate plugin.
    PermanentTransferDelegate,
    /// The Permanent Burn Delegate plugin.
    PermanentBurnDelegate,
    /// The Edition plugin.
    Edition,
    /// The Master Edition plugin.
    MasterEdition,
    /// AddBlocker plugin.
    AddBlocker,
    /// ImmutableMetadata plugin.
    ImmutableMetadata,
    /// VerifiedCreators plugin.
    VerifiedCreators,
    /// Autograph plugin.
    Autograph,
}

/// Variants representing the different types of authority that can have permissions over plugins.
#[repr(u8)]
#[derive(Copy, Clone, BorshSerialize, BorshDeserialize, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Authority {
    /// No authority, used for immutability.
    None,
    /// The owner of the core asset.
    Owner,
    /// The update authority of the core asset.
    UpdateAuthority,
    /// A pubkey that is the authority over a plugin.
    Address {
        /// The address of the authority.
        address: Pubkey,
    },
}

/// A simple type to store the mapping of plugin type to plugin data.
#[repr(C)]
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct RegistryRecord {
    /// The type of plugin.
    pub plugin_type: PluginType, // 2
    /// The authority who has permission to utilize a plugin.
    pub authority: Authority, // Variable
    /// The offset to the plugin in the account.
    pub offset: usize, // 8
}

/// A type to store the mapping of third party plugin type to third party plugin header and data.
#[repr(C)]
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct ExternalRegistryRecord {
    /// The adapter, third party plugin type.
    pub plugin_type: ExternalPluginAdapterType,
    /// The authority of the external plugin adapter.
    pub authority: Authority,
    /// The lifecyle events for which the the external plugin adapter is active.
    pub lifecycle_checks: Option<Vec<(HookableLifecycleEvent, ExternalCheckResult)>>,
    /// The offset to the plugin in the account.
    pub offset: usize, // 8
    /// For plugins with data, the offset to the data in the account.
    pub data_offset: Option<usize>,
    /// For plugins with data, the length of the data in the account.
    pub data_len: Option<usize>,
}

/// Lifecycle permissions for adapter, third party plugins.
/// Third party plugins use this field to indicate their permission to listen, approve, and/or
/// deny a lifecycle event.
#[derive(BorshDeserialize, BorshSerialize, Eq, PartialEq, Copy, Clone, Debug)]
pub struct ExternalCheckResult {
    /// Bitfield for external plugin adapter check results.
    pub flags: u32,
}

#[repr(C)]
#[derive(Eq, PartialEq, Clone, BorshSerialize, BorshDeserialize, Debug, PartialOrd, Ord, Hash)]
/// An enum listing all the lifecyle events available for external plugin adapter hooks.  Note that some
/// lifecycle events such as adding and removing plugins will be checked by default as they are
/// inherently part of the external plugin adapter system.
pub enum HookableLifecycleEvent {
    /// Add a plugin.
    Create,
    /// Transfer an Asset.
    Transfer,
    /// Burn an Asset or a Collection.
    Burn,
    /// Update an Asset or a Collection.
    Update,
}

/// List of third party plugin types.
#[repr(C)]
#[derive(
    Clone, Copy, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq, EnumCount, PartialOrd, Ord,
)]
pub enum ExternalPluginAdapterType {
    /// Lifecycle Hook.
    LifecycleHook,
    /// Oracle.
    Oracle,
    /// Data Store.
    DataStore,
}

/// The Plugin Registry stores a record of all plugins, their location, and their authorities.
#[repr(C)]
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, ShankAccount)]
pub struct PluginRegistryV1 {
    /// The Discriminator of the header which doubles as a plugin metadata version.
    pub key: Key, // 1
    /// The registry of all plugins.
    pub registry: Vec<RegistryRecord>, // 4
    /// The registry of all adapter, third party, plugins.
    pub external_registry: Vec<ExternalRegistryRecord>, // 4
}

impl DataBlob for PluginRegistryV1 {
    fn get_initial_size() -> usize {
        9
    }

    fn get_size(&self) -> usize {
        9 //TODO: Fix this
    }
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
        storage: Arc<PgClient>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            storage,
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
        let mut fees = Vec::new();
        for (pk, asset) in metadata_info.iter() {
            let rent = match Self::calculate_rent_amount(asset) {
                Ok(rent) => rent,
                Err(err) => {
                    error!("calculate_rent_amount: {:?}", err);
                    continue;
                }
            };
            if asset.lamports > rent {
                fees.push(CoreFee {
                    pubkey: *pk,
                    is_paid: false,
                    current_balance: asset.lamports,
                    minimum_rent: rent,
                    slot_updated: asset.slot_updated,
                });
            } else {
                fees.push(CoreFee {
                    pubkey: *pk,
                    is_paid: true,
                    current_balance: asset.lamports,
                    minimum_rent: rent,
                    slot_updated: asset.slot_updated,
                });
            };
        }

        let begin_processing = Instant::now();
        if let Err(err) = self.storage.save_core_fees(fees).await {
            error!("save_core_fees: {}", err);
        };
        self.metrics.set_latency(
            "mpl_core_asset_fee",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    fn calculate_rent_amount(account_info: &CoreAssetFee) -> Result<u64, IngesterError> {
        let rent = Rent::default();
        let rent_amount = match &account_info.indexable_asset {
            MplCoreAccountData::EmptyAccount => rent.minimum_balance(1),
            MplCoreAccountData::Asset(_) => {
                let (asset, header, registry) =
                    Self::fetch_core_data::<AssetV1>(account_info.data.as_slice())?;
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
                        .ok_or(IngesterError::NumericalOverflowError)?
                        .checked_add(registry_size)
                        .ok_or(IngesterError::NumericalOverflowError)?,
                )
            }
            MplCoreAccountData::HashedAsset => rent.minimum_balance(HashedAssetV1::LENGTH),
            _ => return Err(IngesterError::IncorrectAccount),
        };

        Ok(rent_amount)
    }

    fn load<T: BorshDeserialize>(data: &[u8], offset: usize) -> Result<T, IngesterError> {
        let mut bytes: &[u8] = &data[offset..];
        T::deserialize(&mut bytes).map_err(|e| IngesterError::DeserializationError(e.to_string()))
    }

    fn fetch_core_data<T: DataBlob + BorshSerialize + BorshDeserialize>(
        data: &[u8],
    ) -> Result<(T, Option<PluginHeaderV1>, Option<PluginRegistryV1>), IngesterError> {
        let asset = Self::load::<T>(data, 0)?;

        if asset.get_size() != data.len() {
            let plugin_header = Self::load::<PluginHeaderV1>(data, asset.get_size())?;
            let plugin_registry =
                Self::load::<PluginRegistryV1>(data, plugin_header.plugin_registry_offset)?;

            Ok((asset, Some(plugin_header), Some(plugin_registry)))
        } else {
            Ok((asset, None, None))
        }
    }
}
