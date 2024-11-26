//! This module contains core functionality for storing and manipulating
//! so called p2p consistency checking data - checksums for bubblegum
//! and account NFT updates.
//!
//! The main idea is that we split slots "timeline" into so called epochs,
//! (each epoch is 10 000 slots) and calculate checksum for each epoch.
//! 10 epochs shape a grand epoch.
//!
//! Later aura instances can exchange these checksums one with each other
//! to identify whether an instance has missed a portion of changes.
//!
//! Bubblgum update: (tree, slot, seq) => (signature)
//!                     V
//! Bubblgum epoch: (tree, epoch) => (checksum)
//!                     V
//! Bubblegum grand epoch: (tree, grand epoch) => (checksum)
use async_trait::async_trait;
use interface::checksums_storage::{
    AccBucketCksm, AccChecksumServiceApi, AccGrandBucketCksm, AccLastChange, BbgmChangePos,
    BbgmChangeRecord, BbgmChecksumServiceApi, BbgmEpochCksm, BbgmGrandEpochCksm,
};
use interface::checksums_storage::{BbgmGrandEpochCksmWithNumber, Chksm};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{column::TypedColumn, transaction::TreeUpdate, Storage};

use std::{collections::HashSet, sync::atomic::AtomicU64, u64};

/// Last slot seen by any of solana blocks processors.
static LAST_SLOT: AtomicU64 = AtomicU64::new(0);

/// Returns current epoch based on the last seen solana slot
pub fn current_estimated_epoch() -> u32 {
    epoch_of_slot(LAST_SLOT.load(std::sync::atomic::Ordering::Relaxed))
}

pub fn last_tracked_slot() -> u64 {
    // Doesn't guarantee to be fully accurate, which is fine
    LAST_SLOT.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn track_slot_counter(slot: u64) -> u64 {
    let prev = LAST_SLOT.load(std::sync::atomic::Ordering::Relaxed);
    if slot > prev {
        // This code does not guaranteed to store the top slot processed, rather one of the top slots.
        LAST_SLOT.store(slot, std::sync::atomic::Ordering::Relaxed);
    }
    prev
}

pub fn epoch_of_slot(slot: u64) -> u32 {
    (slot / 10_000) as u32
}

pub fn grand_epoch_of_slot(slot: u64) -> u16 {
    (slot / 100_000) as u16
}

pub fn grand_epoch_of_epoch(epoch: u32) -> u16 {
    (epoch / 10) as u16
}

pub fn first_slot_in_epoch(epoch: u32) -> u64 {
    epoch as u64 * 10_000
}

pub fn first_epoch_in_grand_epoch(grand_epoch: u16) -> u32 {
    grand_epoch as u32 * 10
}

pub fn slots_to_next_epoch(slot: u64) -> u64 {
    slot % 100_000
}

/// For an apoch calculates a minimal slot after which we can start exchanging
/// bubblegum and accounts checksums with peers.
/// It is ~10 minutes after the next epoch start.
pub fn calc_exchange_slot_for_epoch(epoch: u32) -> u64 {
    (epoch + 1) as u64 * 10_000 + 1500
}

pub fn slots_to_time(slots: u64) -> std::time::Duration {
    std::time::Duration::from_millis(slots * 400)
}

/// We use 2 leading bytes of an account pubkey as a bucket number,
/// which means we have 65536 buckets.
/// This allows to have records in "account NFT changes" collumn family
/// "grouped" by the bucket number.
pub fn bucket_for_acc(account_pubkey: Pubkey) -> u16 {
    let bytes = account_pubkey.to_bytes();
    let mut b = <[u8; 2]>::default();
    b.clone_from_slice(&bytes[0..2]);

    u16::from_be_bytes(b)
}

/// We use first 10 bits of an account pubkey as a grand bucket number,
/// i.e. we have 1024 grand buckets.
pub fn grand_bucket_for_bucket(bucket: u16) -> u16 {
    bucket >> 6
}

pub fn grand_bucket_for_acc(account_pubkey: Pubkey) -> u16 {
    grand_bucket_for_bucket(bucket_for_acc(account_pubkey))
}

pub const BUBBLEGUM_EPOCH_INVALIDATED: BubblegumEpoch = BubblegumEpoch {
    checksum: Checksum::Invalidated,
};

pub const BUBBLEGUM_EPOCH_CALCULATING: BubblegumEpoch = BubblegumEpoch {
    checksum: Checksum::Calculating,
};

pub const BUBBLEGUM_GRAND_EPOCH_INVALIDATED: BubblegumGrandEpoch = BubblegumGrandEpoch {
    checksum: Checksum::Invalidated,
};

pub const BUBBLEGUM_GRAND_EPOCH_CALCULATING: BubblegumGrandEpoch = BubblegumGrandEpoch {
    checksum: Checksum::Calculating,
};

pub const ACC_BUCKET_INVALIDATE: AccountNftBucket = AccountNftBucket {
    checksum: Checksum::Invalidated,
};

pub const ACC_GRAND_BUCKET_INVALIDATED: AccountNftGrandBucket = AccountNftGrandBucket {
    checksum: Checksum::Invalidated,
};

/// Checksum value for bubblegum epoch/account bucket.
/// Since the arrival of Solana data is asynchronous and has no strict order guarantees,
/// we can easily fall into a situation when we are in the process of calculation
/// of a checksum for an epoch, and a new update came befor the checksum has been written.
/// ```img
/// epoch end     a change for previous epoch arrived
///   |               |
///   V               V
/// ---------------------------------------------------> timeline
///          ^ \____________  ____________/ ^
///          |              \/              |
///        read        calculating        write
///         all         checksum          epoch
///       changes                        checksum
/// ```
/// To prevent such inconsistency of a checksum, roght before the calulating,
/// we mark the epoch checksum to be calculated is "Calculating",
/// and after the checksum is calculated, we write this value only in case
/// if the previous value is still in "Calculated" state.
///
/// At the same time, when the Bubblegum updated processor receives
/// a new update with slot that epoch is from the previous epoch perioud,
/// it not only writed the bubblegum change, but also updated
/// corresponding epoch state to "Invalidated", which prevents
/// the checksum that might be in the process of calculation
/// to be written.
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub enum Checksum {
    Invalidated,
    Calculating,
    Value(Chksm),
}

impl Checksum {
    pub fn ok(&self) -> Option<Chksm> {
        match self {
            Checksum::Value(chksm) => Some(chksm.to_owned()),
            _ => None,
        }
    }
}

/// Key for storing a change detected for bubblegum contract.
/// The value is supposed to be `solana_sdk::signature::Signature``
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumChangeKey {
    pub epoch: u32,
    pub tree_pubkey: Pubkey,
    pub slot: u64,
    pub seq: u64,
}

impl BubblegumChangeKey {
    pub fn new(tree_pubkey: Pubkey, slot: u64, seq: u64) -> BubblegumChangeKey {
        BubblegumChangeKey {
            epoch: epoch_of_slot(slot),
            tree_pubkey,
            slot,
            seq,
        }
    }
    pub fn epoch_start_key(epoch: u32) -> BubblegumChangeKey {
        BubblegumChangeKey {
            epoch,
            tree_pubkey: Pubkey::from([0u8; 32]),
            slot: first_slot_in_epoch(epoch),
            seq: 0,
        }
    }
    pub fn tree_epoch_start_key(tree_pubkey: Pubkey, epoch: u32) -> BubblegumChangeKey {
        BubblegumChangeKey {
            epoch,
            tree_pubkey,
            slot: first_slot_in_epoch(epoch),
            seq: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumChange {
    /// Original signature can be restored as
    /// `solana_sdk::signature::Signature::from_str(...)`
    pub signature: String,
}

impl TypedColumn for BubblegumChange {
    type KeyType = BubblegumChangeKey;
    type ValueType = Self;
    const NAME: &'static str = "BUBBLEGUM_CHANGES";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        // fields are incoded in the order they are defined
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumEpochKey {
    pub tree_pubkey: Pubkey,
    pub epoch_num: u32,
}

impl BubblegumEpochKey {
    pub fn new(tree_pubkey: Pubkey, epoch_num: u32) -> BubblegumEpochKey {
        BubblegumEpochKey {
            tree_pubkey,
            epoch_num,
        }
    }
    pub fn grand_epoch_start_key(grand_epoch: u16) -> BubblegumEpochKey {
        BubblegumEpochKey {
            tree_pubkey: Pubkey::from([0u8; 32]),
            epoch_num: first_epoch_in_grand_epoch(grand_epoch),
        }
    }
    pub fn tree_grand_epoch_start_key(tree_pubkey: Pubkey, grand_epoch: u16) -> BubblegumEpochKey {
        BubblegumEpochKey {
            tree_pubkey,
            epoch_num: first_epoch_in_grand_epoch(grand_epoch),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumEpoch {
    pub checksum: Checksum,
}

impl From<Chksm> for BubblegumEpoch {
    fn from(value: Chksm) -> Self {
        BubblegumEpoch {
            checksum: Checksum::Value(value),
        }
    }
}

impl TypedColumn for BubblegumEpoch {
    type KeyType = BubblegumEpochKey;
    type ValueType = Self;
    const NAME: &'static str = "BUBBLEGUM_EPOCHS";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumGrandEpochKey {
    pub grand_epoch_num: u16,
    pub tree_pubkey: Pubkey,
}

impl BubblegumGrandEpochKey {
    pub fn new(tree_pubkey: Pubkey, grand_epoch_num: u16) -> BubblegumGrandEpochKey {
        BubblegumGrandEpochKey {
            tree_pubkey,
            grand_epoch_num,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumGrandEpoch {
    pub checksum: Checksum,
}

impl From<Chksm> for BubblegumGrandEpoch {
    fn from(value: Chksm) -> Self {
        BubblegumGrandEpoch {
            checksum: Checksum::Value(value),
        }
    }
}

impl TypedColumn for BubblegumGrandEpoch {
    type KeyType = BubblegumGrandEpochKey;
    type ValueType = Self;
    const NAME: &'static str = "BUBBLEGUM_GRAND_EPOCHS";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftChangeKey {
    pub epoch: u32,
    pub account_pubkey: Pubkey,
    pub slot: u64,
    pub write_version: u64,
    pub data_hash: u64,
}

impl AccountNftChangeKey {
    pub fn new(
        account_pubkey: Pubkey,
        slot: u64,
        write_version: u64,
        data_hash: u64,
    ) -> AccountNftChangeKey {
        let epoch = epoch_of_slot(slot);
        AccountNftChangeKey {
            epoch,
            account_pubkey,
            slot,
            write_version,
            data_hash,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftChange {}

impl TypedColumn for AccountNftChange {
    type KeyType = AccountNftChangeKey;
    type ValueType = Self;
    const NAME: &'static str = "ACC_NFT_CHANGES";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftKey {
    pub account_pubkey: Pubkey,
}

impl AccountNftKey {
    pub fn new(account_pubkey: Pubkey) -> AccountNftKey {
        AccountNftKey { account_pubkey }
    }

    /// bincode which is used to encode the AccountNftKey,
    /// preserves same bytes as in unencoded version of account Pubkey.
    pub fn extract_bucket(key_raw_bytes: &[u8]) -> u16 {
        let mut arr = [0u8; 2];
        arr[0] = key_raw_bytes[0];
        arr[1] = key_raw_bytes[1];
        u16::from_be_bytes(arr)
    }

    pub fn bucket_start_key(bucket: u16) -> AccountNftKey {
        let leading_bytes = bucket.to_be_bytes();
        let mut pk = [0u8; 32];
        pk[0] = leading_bytes[0];
        pk[1] = leading_bytes[1];
        AccountNftKey {
            account_pubkey: Pubkey::new_from_array(pk),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNft {
    pub last_slot: u64,
    pub last_write_version: u64,
    pub last_data_hash: u64,
}

impl AccountNft {
    pub fn new(last_slot: u64, last_write_version: u64, last_data_hash: u64) -> AccountNft {
        AccountNft {
            last_slot,
            last_write_version,
            last_data_hash,
        }
    }
    pub fn clear_slot_bytes(raw_db_bytes: &[u8]) -> Vec<u8> {
        let mut result = raw_db_bytes.to_vec();
        // first 8 bytes - slot number
        for b in result.iter_mut().take(8) {
            *b = 0;
        }
        result
    }
}

impl TypedColumn for AccountNft {
    type KeyType = AccountNftKey;

    type ValueType = AccountNft;

    const NAME: &'static str = "ACC_NFT_LAST";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftBucketKey {
    pub bucket: u16,
}

impl AccountNftBucketKey {
    pub fn new(bucket: u16) -> AccountNftBucketKey {
        AccountNftBucketKey { bucket }
    }
    pub fn grand_bucket_start_key(grand_bucket: u16) -> AccountNftBucketKey {
        AccountNftBucketKey {
            bucket: grand_bucket << 6,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftBucket {
    pub checksum: Checksum,
}

impl AccountNftBucket {
    pub fn new(checksum: Chksm) -> AccountNftBucket {
        AccountNftBucket {
            checksum: Checksum::Value(checksum),
        }
    }
}

impl TypedColumn for AccountNftBucket {
    type KeyType = AccountNftBucketKey;
    type ValueType = Self;
    const NAME: &'static str = "ACC_NFT_BUCKETS";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftGrandBucketKey {
    pub grand_bucket: u16,
}

impl AccountNftGrandBucketKey {
    pub fn new(grand_bucket: u16) -> AccountNftGrandBucketKey {
        AccountNftGrandBucketKey { grand_bucket }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccountNftGrandBucket {
    pub checksum: Checksum,
}

impl AccountNftGrandBucket {
    pub fn new(checksum: Chksm) -> AccountNftGrandBucket {
        AccountNftGrandBucket {
            checksum: Checksum::Value(checksum),
        }
    }
}

impl TypedColumn for AccountNftGrandBucket {
    type KeyType = AccountNftGrandBucketKey;
    type ValueType = Self;
    const NAME: &'static str = "ACC_NFT_GRAND_BUCKET";

    fn encode_key(key: Self::KeyType) -> Vec<u8> {
        bincode::serialize(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let key = bincode::deserialize(&bytes)?;
        Ok(key)
    }
}

impl Storage {
    /// Adds bubblegum change record to the bubblegum changes column family.
    /// Functionality for triggering checksum calculation/re-calculation is triggered separately,
    /// in ingester module.
    pub fn track_tree_change_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tree_update: &TreeUpdate,
    ) -> crate::Result<()> {
        let key = BubblegumChangeKey::new(tree_update.tree, tree_update.slot, tree_update.seq);
        let value = BubblegumChange {
            signature: tree_update.tx.clone(),
        };
        let _ = self.bubblegum_changes.put_with_batch(batch, key, &value);

        if epoch_of_slot(tree_update.slot) < current_estimated_epoch() {
            // We invalidate epoch checksum here, but trigger checksum recalculation in another place.
            // Possibly somthing might happen after that checksum is invalidate, and re-calculation
            // won't start.
            // It is acceptable, since it is better to have clearly invalidated checksum,
            // than cehcksum that doesn't reflect the current state.
            self.invalidate_bubblegum_epoch_with_batch(
                batch,
                tree_update.tree,
                epoch_of_slot(tree_update.slot),
            );
        }

        Ok(())
    }

    fn invalidate_bubblegum_epoch_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tree: Pubkey,
        epoch: u32,
    ) {
        let epock_key = BubblegumEpochKey {
            tree_pubkey: tree,
            epoch_num: epoch,
        };
        let _ =
            self.bubblegum_epochs
                .put_with_batch(batch, epock_key, &BUBBLEGUM_EPOCH_INVALIDATED);

        if grand_epoch_of_epoch(epoch) < grand_epoch_of_epoch(current_estimated_epoch()) {
            let grand_epock_key = BubblegumGrandEpochKey {
                tree_pubkey: tree,
                grand_epoch_num: grand_epoch_of_epoch(epoch),
            };
            let _ = self.bubblegum_grand_epochs.put_with_batch(
                batch,
                grand_epock_key,
                &BUBBLEGUM_GRAND_EPOCH_INVALIDATED,
            );
        }
    }
}

#[async_trait::async_trait]
pub trait DataConsistencyStorage {
    async fn drop_forked_bubblegum_changes(&self, chagens: &[BubblegumChangeKey]);
}

#[async_trait::async_trait]
impl DataConsistencyStorage for Storage {
    async fn drop_forked_bubblegum_changes(&self, chagens: &[BubblegumChangeKey]) {
        let _ = self.bubblegum_changes.delete_batch(chagens.to_vec()).await;

        let mut epochs_to_invalidate = HashSet::new();
        for change in chagens {
            epochs_to_invalidate.insert(BubblegumEpochKey {
                tree_pubkey: change.tree_pubkey,
                epoch_num: epoch_of_slot(change.slot),
            });
        }

        let mut grand_epochs_to_invalidate = HashSet::new();
        for epoch_to_invalidate in epochs_to_invalidate {
            let _ = self.bubblegum_epochs.put(
                epoch_to_invalidate.clone(),
                BUBBLEGUM_EPOCH_INVALIDATED.clone(),
            );
            grand_epochs_to_invalidate.insert(BubblegumGrandEpochKey {
                tree_pubkey: epoch_to_invalidate.tree_pubkey,
                grand_epoch_num: grand_epoch_of_epoch(epoch_to_invalidate.epoch_num),
            });
        }

        for grand_epoch_to_invalidate in grand_epochs_to_invalidate {
            let _ = self.bubblegum_grand_epochs.put(
                grand_epoch_to_invalidate,
                BUBBLEGUM_GRAND_EPOCH_INVALIDATED.clone(),
            );
        }
    }
}

// TODO: Replace with LazyLock after rustc update.
lazy_static::lazy_static! {
    pub static ref BUBBLEGUM_EPOCH_INVALIDATED_BYTES: Vec<u8> = bincode::serialize(&BUBBLEGUM_EPOCH_INVALIDATED).unwrap();
    pub static ref BUBBLEGUM_EPOCH_CALCULATING_BYTES: Vec<u8> = bincode::serialize(&BUBBLEGUM_EPOCH_CALCULATING).unwrap();
    pub static ref BUBBLEGUM_GRAND_EPOCH_INVALIDATED_BYTES: Vec<u8> = bincode::serialize(&BUBBLEGUM_GRAND_EPOCH_INVALIDATED).unwrap();
    pub static ref BUBBLEGUM_GRAND_EPOCH_CALCULATING_BYTES: Vec<u8> = bincode::serialize(&BUBBLEGUM_GRAND_EPOCH_CALCULATING).unwrap();
}

/// This merge should be used only for setting calculated checksum.
/// The thing is that while we calculate checksum for tree signatures in a given slot,
/// it is possible that in parallel we receive another update for this tree in this epoch.
/// To not miss this fact we calculate tree epoch checksum in following way:
/// 1) Set tree epoch as Calculating
/// 2) Calculate checksum
/// 3) Update tree epoch with calculated checksum, only if previous value is Calculating
/// This works in conjunction with bubblegum_updates_processor that sets tree epoch value
/// to Invalidated after each tree update.
/// That's why checksum calculator is able to specify checksum only, if no updates have been
/// received during the calculating (because otherwise the status will be Invalidated, not Calculating).
pub(crate) fn merge_bubblgum_epoch_checksum(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    if let Some(v) = existing_val {
        if v == BUBBLEGUM_EPOCH_CALCULATING_BYTES.as_slice() {
            if let Some(op) = operands.into_iter().next() {
                return Some(op.to_vec());
            }
        }
        Some(v.to_vec())
    } else {
        None
    }
}

pub(crate) fn merge_bubblgum_grand_epoch_checksum(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    if let Some(v) = existing_val {
        if v == BUBBLEGUM_GRAND_EPOCH_CALCULATING_BYTES.as_slice() {
            if let Some(op) = operands.into_iter().next() {
                return Some(op.to_vec());
            }
        }
        Some(v.to_vec())
    } else {
        None
    }
}

#[allow(clippy::while_let_on_iterator)]
#[async_trait]
impl BbgmChecksumServiceApi for Storage {
    async fn get_earliest_grand_epoch(&self) -> anyhow::Result<Option<u16>> {
        let Some(first_record) = self.bubblegum_grand_epochs.iter_start().next() else {
            return Ok(None);
        };
        let (k, _v) = first_record?;
        let first_key = BubblegumGrandEpoch::decode_key(k.to_vec())?;
        Ok(Some(first_key.grand_epoch_num))
    }

    async fn list_bbgm_grand_epoch_for_tree(
        &self,
        tree_pubkey: Pubkey,
    ) -> anyhow::Result<Vec<BbgmGrandEpochCksmWithNumber>> {
        let mut result: Vec<BbgmGrandEpochCksmWithNumber> = Vec::new();

        let Some(earliest_grand_epoch) = self.get_earliest_grand_epoch().await? else {
            return Ok(result);
        };
        let latest_grand_epoch = grand_epoch_of_epoch(current_estimated_epoch());

        for i in earliest_grand_epoch..=latest_grand_epoch {
            let pontial_key = BubblegumGrandEpochKey {
                grand_epoch_num: i,
                tree_pubkey,
            };
            if let Some(value) = self.bubblegum_grand_epochs.get_async(pontial_key).await? {
                result.push(BbgmGrandEpochCksmWithNumber {
                    grand_epoch: i,
                    checksum: value.checksum.ok(),
                });
            };
        }

        Ok(result)
    }

    async fn list_grand_epoch_checksums(
        &self,
        grand_epoch: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<BbgmGrandEpochCksm>> {
        let max_result = limit.unwrap_or(u64::MAX) as usize;
        let mut it = if let Some(after) = after {
            let mut it = self
                .bubblegum_grand_epochs
                .iter(BubblegumGrandEpochKey::new(after, grand_epoch));
            let _ = it.next();
            it
        } else {
            self.bubblegum_grand_epochs.iter_start()
        };
        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let pair = next?;
            let k = BubblegumGrandEpoch::decode_key(pair.0.to_vec())?;
            let v = bincode::deserialize::<BubblegumGrandEpoch>(&pair.1)?;
            if k.grand_epoch_num != grand_epoch {
                break;
            }
            result.push(BbgmGrandEpochCksm {
                tree_pubkey: k.tree_pubkey,
                checksum: v.checksum.ok(),
            });
            if result.len() >= max_result {
                break;
            }
        }
        Ok(result)
    }

    async fn list_epoch_checksums(
        &self,
        grand_epoch: u16,
        tree_pubkey: Pubkey,
    ) -> anyhow::Result<Vec<BbgmEpochCksm>> {
        let first_epoch = first_epoch_in_grand_epoch(grand_epoch);
        let mut it = self
            .bubblegum_epochs
            .iter(BubblegumEpochKey::new(tree_pubkey, first_epoch));
        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let pair = next?;
            let k = BubblegumEpoch::decode_key(pair.0.to_vec())?;
            let v = bincode::deserialize::<BubblegumEpoch>(&pair.1)?;
            if grand_epoch_of_epoch(k.epoch_num) != grand_epoch || k.tree_pubkey != tree_pubkey {
                break;
            }
            result.push(BbgmEpochCksm {
                tree_pubkey,
                epoch: k.epoch_num,
                checksum: v.checksum.ok(),
            });
        }
        Ok(result)
    }

    async fn list_epoch_changes(
        &self,
        epoch: u32,
        tree_pubkey: Pubkey,
        limit: Option<u64>,
        after: Option<BbgmChangePos>,
    ) -> anyhow::Result<Vec<BbgmChangeRecord>> {
        let max_result = limit.unwrap_or(u64::MAX) as usize;
        let BbgmChangePos { slot, seq } = after.unwrap_or_default();

        let mut it = self.bubblegum_changes.iter(BubblegumChangeKey {
            epoch,
            tree_pubkey,
            slot,
            seq,
        });

        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let pair = next?;
            let k = BubblegumChange::decode_key(pair.0.to_vec())?;
            let v = bincode::deserialize::<BubblegumChange>(&pair.1)?;
            if k.tree_pubkey != tree_pubkey || epoch_of_slot(k.slot) != epoch {
                break;
            }
            result.push(BbgmChangeRecord {
                tree_pubkey,
                slot: k.slot,
                seq: k.seq,
                signature: v.signature,
            });
            if result.len() >= max_result {
                break;
            }
        }

        Ok(result)
    }

    async fn propose_missing_changes(&self, _changes: &[BbgmChangeRecord]) {
        // TODO: how handle?
    }
}

#[allow(clippy::while_let_on_iterator)]
#[async_trait]
impl AccChecksumServiceApi for Storage {
    async fn list_grand_buckets(&self) -> anyhow::Result<Vec<AccGrandBucketCksm>> {
        let mut it = self.acc_nft_grand_buckets.iter_start();
        let mut result = Vec::new();
        while let Some(rec) = it.next() {
            if let Ok((Ok(k), Ok(v))) = rec.map(|(k, v)| {
                (
                    AccountNftGrandBucket::decode_key(k.to_vec()),
                    bincode::deserialize::<AccountNftGrandBucket>(&v),
                )
            }) {
                result.push(AccGrandBucketCksm {
                    grand_bucket: k.grand_bucket,
                    checksum: v.checksum.ok(),
                });
            }
        }
        Ok(result)
    }

    async fn list_bucket_checksums(&self, grand_bucket: u16) -> anyhow::Result<Vec<AccBucketCksm>> {
        let mut it = self
            .acc_nft_buckets
            .iter(AccountNftBucketKey::grand_bucket_start_key(grand_bucket));
        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let pair = next?;
            let k = AccountNftBucket::decode_key(pair.0.to_vec())?;
            let v = bincode::deserialize::<AccountNftBucket>(&pair.1)?;
            if grand_bucket_for_bucket(k.bucket) != grand_bucket {
                break;
            }
            result.push(AccBucketCksm {
                bucket: k.bucket,
                checksum: v.checksum.ok(),
            });
        }
        Ok(result)
    }

    async fn list_accounts(
        &self,
        bucket: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<AccLastChange>> {
        let max_result = limit.unwrap_or(u64::MAX) as usize;
        let start_key = after
            .map(|account_pubkey| AccountNftKey { account_pubkey })
            .unwrap_or(AccountNftKey::bucket_start_key(bucket));
        let mut it = self.acc_nft_last.iter(start_key);
        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let pair = next?;
            let k = AccountNft::decode_key(pair.0.to_vec())?;
            let v = bincode::deserialize::<AccountNft>(&pair.1)?;
            if bucket_for_acc(k.account_pubkey) != bucket {
                break;
            }
            result.push(AccLastChange {
                account_pubkey: k.account_pubkey,
                slot: v.last_slot,
                write_version: v.last_write_version,
                data_hash: v.last_data_hash,
            });
            if result.len() >= max_result {
                break;
            }
        }
        Ok(result)
    }

    async fn propose_missing_changes(
        &self,
        _changes: Vec<interface::checksums_storage::AccLastChange>,
    ) {
        // Do nothing
    }
}
