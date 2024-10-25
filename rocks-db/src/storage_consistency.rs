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
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{column::TypedColumn, transaction::TreeUpdate, Storage};

use std::{cell::Cell, collections::HashSet};

/// Holds the number for an epoch that is happening at the moment
/// It should be super presize, i.e. it is OK (and even preferable)
/// to have a small lag between real epoch change (current solana slot)
/// and update of this variable.
const CURRENT_ESTIMATED_EPOCH: Cell<u32> = Cell::new(0);

pub fn current_estimated_epoch() -> u32 {
    CURRENT_ESTIMATED_EPOCH.get()
}

pub fn update_estimated_epoch(new_value: u32) {
    CURRENT_ESTIMATED_EPOCH.set(new_value);
}

pub fn epoch_of_slot(slot: u64) -> u32 {
    (slot / 10_000) as u32
}

pub fn grand_epoch_of_slot(slot: u64) -> u16 {
    (slot / 100_000) as u16
}

pub fn grand_epoch_of_epoch(slot: u32) -> u16 {
    (slot / 10) as u16
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

/// We use 2 leading bytes of an account pubkey as a bucket number,
/// which means we have 65536 buckets.
/// This allows to have records in "account NFT changes" collumn family
/// "grouped" by the bucket number.
pub fn bucket_for_acc(accout_pubkey: Pubkey) -> u16 {
    let bytes = accout_pubkey.to_bytes();
    let mut b = <[u8; 2]>::default();
    b.clone_from_slice(&bytes[0..2]);

    u16::from_be_bytes(b)
}

/// We use first 10 bits of an account pubkey as a grand bucket number,
/// i.e. we have 1024 grand buckets.
pub fn grand_bucket_for_bucket(bucket: u16) -> u16 {
    bucket >> 6
}

pub fn grand_bucket_for_acc(accout_pubkey: Pubkey) -> u16 {
    grand_bucket_for_bucket(bucket_for_acc(accout_pubkey))
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

pub const ACC_GRAND_BUCKET_INVALIDATE: AccountNftGrandBucket = AccountNftGrandBucket {
    checksum: Checksum::Invalidated,
};

/// Checksum value for bubblegum epoch/account bucket.
/// Since the arrival of Solana data is asynchronous and has no strict order guarantees,
/// we can easily fall into a situation when we are in the process of calculation
/// of a checksum for an epoch, and a new update came befor the checksum has been written.
/// ```norun
///
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
    Value([u8; 32]),
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
            tree_pubkey: tree_pubkey,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct BubblegumEpoch {
    pub checksum: Checksum,
}

impl From<[u8; 32]> for BubblegumEpoch {
    fn from(value: [u8; 32]) -> Self {
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
    pub tree_pubkey: Pubkey,
    pub grand_epoch_num: u16,
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

impl From<[u8; 32]> for BubblegumGrandEpoch {
    fn from(value: [u8; 32]) -> Self {
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
}

impl AccountNft {
    pub fn new(last_slot: u64, last_write_version: u64) -> AccountNft {
        AccountNft {
            last_slot,
            last_write_version,
        }
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
    pub fn new(checksum: [u8; 32]) -> AccountNftBucket {
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
    pub fn new(checksum: [u8; 32]) -> AccountNftGrandBucket {
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
        let key =
            BubblegumChangeKey::new(tree_update.tree.clone(), tree_update.slot, tree_update.seq);
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
    pub static ref BUBBLEGUM_EPOCH_CALCULATING_BYTES: Vec<u8> = bincode::serialize(&BUBBLEGUM_EPOCH_CALCULATING).unwrap();
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
