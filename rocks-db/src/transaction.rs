use async_trait::async_trait;
use entities::{
    enums::TaskStatus,
    models::{BufferedTransaction, SignatureWithSlot},
};
use interface::error::StorageError;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::events::ChangeLogEventV1;
use spl_account_compression::state::PathNode;

use crate::{
    asset::{AssetCollection, AssetLeaf},
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
};

#[derive(Debug, Clone, Default)]
pub struct Task {
    pub ofd_metadata_url: String,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
    pub ofd_status: TaskStatus,
}

pub trait TransactionProcessor: Sync + Send + 'static {
    fn get_ingest_transaction_results(
        &self,
        tx: BufferedTransaction,
    ) -> Result<TransactionResult, StorageError>;
}

#[async_trait]
pub trait TransactionResultPersister: Sync + Send + 'static {
    async fn store_block(&self, txs: Vec<TransactionResult>) -> Result<(), StorageError>;
}

#[derive(Clone, Default)]
pub struct AssetUpdateEvent {
    pub event: CopyableChangeLogEventV1,
    pub slot: u64,
    pub update: Option<AssetDynamicUpdate>,
    pub static_update: Option<AssetUpdate<AssetStaticDetails>>,
    pub owner_update: Option<AssetUpdate<AssetOwner>>,
    pub authority_update: Option<AssetUpdate<AssetAuthority>>,
    pub collection_update: Option<AssetUpdate<AssetCollection>>,
}

#[derive(Clone, Default)]
pub struct CopyableChangeLogEventV1 {
    /// Public key of the ConcurrentMerkleTree
    pub id: Pubkey,

    /// Nodes of off-chain merkle tree needed by indexer
    pub path: Vec<PathNode>,

    /// Index corresponding to the number of successful operations on this tree.
    /// Used by the off-chain indexer to figure out when there are gaps to be backfilled.
    pub seq: u64,

    /// Bitmap of node parity (used when hashing)
    pub index: u32,
}

impl From<&ChangeLogEventV1> for CopyableChangeLogEventV1 {
    fn from(event: &ChangeLogEventV1) -> Self {
        Self {
            id: event.id,
            path: event.path.clone(),
            seq: event.seq,
            index: event.index,
        }
    }
}
#[derive(Clone, Default)]
pub struct AssetDynamicUpdate {
    pub pk: Pubkey,
    pub slot: u64,
    pub leaf: Option<AssetLeaf>,
    pub dynamic_data: Option<AssetDynamicDetails>,
}
#[derive(Clone, Default)]
pub struct AssetUpdate<T> {
    pub pk: Pubkey,
    pub details: T,
}
#[derive(Clone, Default)]
pub struct InstructionResult {
    pub update: Option<AssetUpdateEvent>,
    pub task: Option<Task>,
    pub decompressed: Option<AssetUpdate<AssetDynamicDetails>>,
}

impl From<AssetUpdateEvent> for InstructionResult {
    fn from(update: AssetUpdateEvent) -> Self {
        Self {
            update: Some(update),
            ..Default::default()
        }
    }
}

impl From<(AssetUpdateEvent, Task)> for InstructionResult {
    fn from((update, task): (AssetUpdateEvent, Task)) -> Self {
        Self {
            update: Some(update),
            task: Some(task),
            ..Default::default()
        }
    }
}

impl From<AssetUpdate<AssetDynamicDetails>> for InstructionResult {
    fn from(decompressed: AssetUpdate<AssetDynamicDetails>) -> Self {
        Self {
            decompressed: Some(decompressed),
            ..Default::default()
        }
    }
}

pub struct TransactionResult {
    pub instruction_results: Vec<InstructionResult>,
    pub transaction_signature: Option<(Pubkey, SignatureWithSlot)>,
}