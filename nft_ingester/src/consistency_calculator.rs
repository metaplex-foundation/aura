//! The module contains functionality for calculating transaction-based
//! and account-based NFT checsums that are used in peer-to-peer
//! Aura nodes communication to identify missing data on a node.

use interface::slots_dumper::SlotGetter;
use metrics_utils::ChecksumCalculationMetricsConfig;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::storage_consistency::first_slot_in_epoch;
use rocks_db::{
    column::TypedColumn,
    storage_consistency::{
        self, bucket_for_acc, epoch_of_slot, grand_bucket_for_bucket, grand_epoch_of_epoch,
        AccountNft, AccountNftBucket, AccountNftBucketKey, AccountNftChange, AccountNftChangeKey,
        AccountNftGrandBucket, AccountNftGrandBucketKey, AccountNftKey, BubblegumChange,
        BubblegumChangeKey, BubblegumEpoch, BubblegumEpochKey, BubblegumGrandEpoch,
        BubblegumGrandEpochKey, ACC_BUCKET_INVALIDATE, ACC_GRAND_BUCKET_INVALIDATED,
        BUBBLEGUM_GRAND_EPOCH_INVALIDATED,
    },
    Storage,
};
use solana_sdk::{hash::Hasher, pubkey::Pubkey};
use std::sync::atomic::AtomicI32;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};
use storage_consistency::{BUBBLEGUM_EPOCH_CALCULATING, BUBBLEGUM_GRAND_EPOCH_CALCULATING};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tokio::time::Instant;

use crate::fork_cleaner::last_fork_cleaned_slot;

/// This flag is set to true before bubblegum epoch calculation is started,
/// and set to false after the calculation is finished.
static IS_CALCULATING_BBGM_EPOCH: AtomicI32 = AtomicI32::new(-1);
static IS_CALCULATING_ACC_EPOCH: AtomicI32 = AtomicI32::new(-1);

fn set_currently_calculated_bbgm_epoch(epoch: u32) {
    IS_CALCULATING_BBGM_EPOCH.store(epoch as i32, std::sync::atomic::Ordering::Relaxed);
}

fn finish_currently_calculated_bbgm_epoch() {
    IS_CALCULATING_BBGM_EPOCH.store(-1, std::sync::atomic::Ordering::Relaxed);
}

fn set_currently_calculated_acc_epoch(epoch: u32) {
    IS_CALCULATING_ACC_EPOCH.store(epoch as i32, std::sync::atomic::Ordering::Relaxed);
}

fn finish_currently_calculated_acc_epoch() {
    IS_CALCULATING_ACC_EPOCH.store(-1, std::sync::atomic::Ordering::Relaxed);
}

pub fn get_calculating_bbgm_epoch() -> Option<u32> {
    let epoch = IS_CALCULATING_BBGM_EPOCH.load(std::sync::atomic::Ordering::Relaxed);
    if epoch > -1 {
        Some(epoch as u32)
    } else {
        None
    }
}

pub fn get_calculating_acc_epoch() -> Option<u32> {
    let epoch = IS_CALCULATING_ACC_EPOCH.load(std::sync::atomic::Ordering::Relaxed);
    if epoch > -1 {
        Some(epoch as u32)
    } else {
        None
    }
}

pub const NTF_CHANGES_NOTIFICATION_QUEUE_SIZE: usize = 1000;

/// Wait this amount of seconds for late data before starting to calculate the epoch
const EPOCH_CALC_LAG_SEC: u64 = 300;

/// This message is used to send notifications abount changes from:
/// - bubblegum processor
/// - account processor
/// - fork cleaner
#[derive(Debug, PartialEq, Eq)]
pub enum ConsistencyCalcMsg {
    StartingBackfilling,
    FinishedBackfilling,
    EpochChanged { new_epoch: u32 },
    BubblegumUpdated { tree: Pubkey, slot: u64 },
    AccUpdated { account: Pubkey, slot: u64 },
}

/// Component for convenient storing of account NFT changes,
/// and notifying checksum calculator when a whole epoch,
/// or an individual bubblegum three/account checksum should be calculated.
pub struct NftChangesTracker {
    sender: Option<Sender<ConsistencyCalcMsg>>,
}

impl NftChangesTracker {
    pub fn new(sender: Option<Sender<ConsistencyCalcMsg>>) -> NftChangesTracker {
        NftChangesTracker { sender }
    }

    /// Persists given account NFT change into the storage, and, if the change is from the epoch
    /// that is previous to the current epoch, then also notifies checksums calculator
    /// about late data.
    ///
    /// ## Args:
    /// * `batch_storage` - same batch storage that is used to save account data
    /// * `account_pubkey` - Pubkey of the NFT account
    /// * `slot` - the slot number that change is made in
    /// * `write_version` - write version of the change
    pub async fn track_account_change(
        &self,
        batch_storage: &mut BatchSaveStorage,
        account_pubkey: Pubkey,
        slot: u64,
        write_version: u64,
        data_hash: u64,
    ) {
        let epoch = epoch_of_slot(slot);
        let key = AccountNftChangeKey {
            epoch,
            account_pubkey,
            slot,
            write_version,
            data_hash,
        };
        let value = AccountNftChange {};

        let last_slot = storage_consistency::track_slot_counter(slot);
        let last_slot_epoch = epoch_of_slot(last_slot);

        if epoch < last_slot_epoch {
            let bucket = bucket_for_acc(account_pubkey);
            let grand_bucket = grand_bucket_for_bucket(bucket);
            let _ = batch_storage.put_acc_grand_bucket(
                AccountNftGrandBucketKey::new(grand_bucket),
                ACC_GRAND_BUCKET_INVALIDATED,
            );
            let _ = batch_storage
                .put_acc_bucket(AccountNftBucketKey::new(bucket), ACC_BUCKET_INVALIDATE);
        }

        let _ = batch_storage.put_account_change(key, value);

        if let Some(sender) = self.sender.as_ref() {
            if epoch < last_slot_epoch {
                let _ = sender
                    .send(ConsistencyCalcMsg::AccUpdated {
                        account: account_pubkey,
                        slot,
                    })
                    .await;
            } else if epoch > last_slot_epoch && last_slot != 0 {
                let _ = sender
                    .send(ConsistencyCalcMsg::EpochChanged { new_epoch: epoch })
                    .await;
            }
        }
    }

    /// Checks bubble tree slot, and if the slot number is from an epoch previous to the current,
    /// emits notification to the checksums calculator.
    ///
    /// In contrast to account notification tracking method, for bubblegum we don't
    /// store tree change here, since it is stored inside of [rocks_db::transaction_client]
    /// in scope of the same batch that persists Bubblegum tree change details.
    pub async fn watch_bubblegum_change(&self, tree: Pubkey, slot: u64) {
        let epoch = epoch_of_slot(slot);
        let last_slot = storage_consistency::track_slot_counter(slot);
        let last_slot_epoch = epoch_of_slot(last_slot);
        if let Some(sender) = self.sender.as_ref() {
            if epoch < last_slot_epoch {
                let _ = sender
                    .send(ConsistencyCalcMsg::BubblegumUpdated { tree, slot })
                    .await;
            } else if epoch > last_slot_epoch && last_slot != 0 {
                let _ = sender
                    .send(ConsistencyCalcMsg::EpochChanged { new_epoch: epoch })
                    .await;
            }
        }
    }

    /// Iterates over bubblegum changes, and for each of them check if the change from an epoch
    /// previous to the current. If the change is from the previous epoch,
    /// it sends a notification for the checksums calculator.
    /// This method is called from the fork cleaner.
    pub async fn watch_remove_forked_bubblegum_changes(&self, keys: &[BubblegumChangeKey]) {
        let last_slot = storage_consistency::last_tracked_slot();
        let last_slot_epoch = epoch_of_slot(last_slot);
        if let Some(sender) = self.sender.as_ref() {
            for key in keys {
                if key.epoch < last_slot_epoch {
                    let _ = sender
                        .send(ConsistencyCalcMsg::BubblegumUpdated {
                            tree: key.tree_pubkey,
                            slot: key.slot,
                        })
                        .await;
                }
            }
        }
    }
}

/// An entry point for checksums calculation component.
/// Should be called from "main".
/// Accepts notifications about epoch change, or changes in specific bubblegum tree or account,
/// and schedules checksum calculation.
pub fn run_bg_consistency_calculator(
    mut rcv: Receiver<ConsistencyCalcMsg>,
    storage: Arc<Storage>,
    force_reingestable_slot_getter: Arc<impl SlotGetter + Send + Sync + 'static>,
    mut shutdown_signal: tokio::sync::broadcast::Receiver<()>,
    metrics: Arc<ChecksumCalculationMetricsConfig>,
) {
    tokio::spawn(async move {
        let bbgm_tasks: Arc<Mutex<BTreeSet<BbgmTask>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let acc_tasks: Arc<Mutex<BTreeSet<AccTask>>> = Arc::new(Mutex::new(BTreeSet::new()));

        // Taks that calculates bubblegum checksums
        let _bbgm_bg = {
            let storage = storage.clone();
            let bbgm_tasks = bbgm_tasks.clone();
            let force_reingestable_slot_getter = force_reingestable_slot_getter.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                process_bbgm_tasks(storage, bbgm_tasks, force_reingestable_slot_getter, metrics)
                    .await;
            })
        };
        // Taks that calculates account NFT checksums
        let _acc_bg = {
            let storage = storage.clone();
            let acc_tasks = acc_tasks.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                process_acc_tasks(storage, acc_tasks, metrics).await;
            })
        };

        loop {
            let calc_msg = tokio::select! {
                msg = rcv.recv() => msg,
                _ = shutdown_signal.recv() => {
                    tracing::info!("Received stop signal, stopping consistency calculator");
                    break;
                }
            };

            match calc_msg {
                Some(msg) => match msg {
                    ConsistencyCalcMsg::EpochChanged { new_epoch } => {
                        let prev_epoch = new_epoch.saturating_sub(1);
                        {
                            // We don't wait for gaps filles (sequnce_consistent.rs) to process
                            // slots up to the last in the epoch, just will recalculate then if needed.
                            let mut guard = bbgm_tasks.lock().await;
                            guard.insert(BbgmTask::CalcEpoch(prev_epoch));
                        }
                        {
                            let mut guard = acc_tasks.lock().await;
                            guard.insert(AccTask::CalcEpoch(prev_epoch));
                        }
                    }
                    ConsistencyCalcMsg::BubblegumUpdated { tree, slot } => {
                        let mut guard = bbgm_tasks.lock().await;
                        guard.insert(BbgmTask::CalcTree(epoch_of_slot(slot), tree));
                    }
                    ConsistencyCalcMsg::AccUpdated { account: _, slot } => {
                        // It's actually more reasonable to just process all late changes
                        let mut guard = acc_tasks.lock().await;
                        guard.insert(AccTask::CalcEpoch(epoch_of_slot(slot)));
                    }
                    ConsistencyCalcMsg::StartingBackfilling => {
                        {
                            let mut guard = bbgm_tasks.lock().await;
                            guard.insert(BbgmTask::Suspend);
                        }
                        {
                            let mut guard = acc_tasks.lock().await;
                            guard.insert(AccTask::Suspend);
                        }
                    }
                    ConsistencyCalcMsg::FinishedBackfilling => {
                        {
                            let mut guard = bbgm_tasks.lock().await;
                            guard.insert(BbgmTask::Resume);
                        }
                        {
                            let mut guard = acc_tasks.lock().await;
                            guard.insert(AccTask::Resume);
                        }
                    }
                },
                None => break,
            }
        }
    });
}

/// Type for messages that are used to send commands to bubblegum epochs checksums calculator.
///
/// Fields order matters, because we use sorted set to pass commands to the calculator.
/// We want whole epochs to be calculates before individual tree epochs from late changes.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BbgmTask {
    /// Suspend checksum calculation, e.g. before backfilling
    Suspend,
    /// Resume checksum calculation (backfilling is finished)
    Resume,
    /// Calculate checksums for all bubblegum trees in the given epoch
    CalcEpoch(u32),
    /// Calculate checksums only for the given bubblegum tree in the given epoch
    CalcTree(u32, Pubkey),
}

/// Type for messages that are used to send commands to account buckets checksums calculator.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AccTask {
    Suspend,
    Resume,
    CalcEpoch(u32),
}

async fn process_bbgm_tasks(
    storage: Arc<Storage>,
    tasks: Arc<Mutex<BTreeSet<BbgmTask>>>,
    force_reingestable_slot_getter: Arc<impl SlotGetter>,
    metrics: Arc<ChecksumCalculationMetricsConfig>,
) {
    let mut is_suspended = false;
    loop {
        if is_suspended {
            let mut guard = tasks.lock().await;
            match guard.first() {
                Some(t) if *t != BbgmTask::Resume => (),
                Some(t) if *t != BbgmTask::Suspend => {
                    guard.pop_first();
                    continue;
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }
        }
        let maybe_task = {
            let mut guard = tasks.lock().await;
            guard.pop_first()
        };
        match maybe_task {
            Some(BbgmTask::CalcEpoch(epoch)) => {
                tracing::info!("Preparing for calculation if Bubblegum ckecksum epoch: {epoch}");
                set_currently_calculated_bbgm_epoch(epoch);
                tokio::time::sleep(Duration::from_secs(EPOCH_CALC_LAG_SEC)).await;
                // Calculate epoch only after fork cleaner and sequence consistency have finished their job
                let minimal_clarified_slot = first_slot_in_epoch(epoch);
                loop {
                    let fork_cleaner_not_finished =
                        last_fork_cleaned_slot() < minimal_clarified_slot;
                    let sequnce_consistency_not_finished = force_reingestable_slot_getter
                        .get_unprocessed_slots_iter()
                        .next()
                        .map(|slot| slot < minimal_clarified_slot)
                        .unwrap_or(false);
                    if fork_cleaner_not_finished || sequnce_consistency_not_finished {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        continue;
                    }
                    break;
                }
                tracing::info!("Calculating Bubblegum ckecksum epoch: {epoch}");
                let start = Instant::now();
                calc_bubblegum_checksums(&storage, epoch, None).await;
                metrics
                    .bubblegum_epoch_calculation_seconds
                    .set(start.elapsed().as_secs() as i64);
                finish_currently_calculated_bbgm_epoch();
                tracing::info!("Finished calculating Bubblegum ckecksum epoch: {epoch}");
            }
            Some(BbgmTask::CalcTree(epoch, tree)) => {
                calc_bubblegum_checksums(&storage, epoch, Some(tree)).await
            }
            Some(BbgmTask::Suspend) => is_suspended = true,
            Some(BbgmTask::Resume) => is_suspended = false,
            None => tokio::time::sleep(Duration::from_secs(10)).await,
        };
    }
}

async fn process_acc_tasks(
    storage: Arc<Storage>,
    tasks: Arc<Mutex<BTreeSet<AccTask>>>,
    metrics: Arc<ChecksumCalculationMetricsConfig>,
) {
    let mut is_suspended = false;
    loop {
        if is_suspended {
            let mut guard = tasks.lock().await;
            match guard.first() {
                Some(t) if *t != AccTask::Resume => (),
                Some(t) if *t != AccTask::Suspend => {
                    guard.pop_first();
                    continue;
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }
        }
        let maybe_task = {
            let mut guard = tasks.lock().await;
            guard.pop_first()
        };
        match maybe_task {
            Some(AccTask::CalcEpoch(epoch)) => {
                tracing::info!("Calculating accounts changes checksums: {epoch}");
                set_currently_calculated_acc_epoch(epoch);
                let start = Instant::now();
                calc_acc_nft_checksums(&storage, epoch).await;
                metrics
                    .account_epoch_calculation_seconds
                    .set(start.elapsed().as_secs() as i64);
                finish_currently_calculated_acc_epoch();
                tracing::info!("Finished calculating accounts ckecksum epoch: {epoch}");
            }
            Some(AccTask::Suspend) => is_suspended = true,
            Some(AccTask::Resume) => is_suspended = false,
            None => tokio::time::sleep(Duration::from_secs(10)).await,
        };
    }
}

/// Bubblegum checksums calculation start point.
/// Iterates over all the bubblegum trees changes in the given epoch, and calculates epochs
/// and grand epochs checksums.
pub async fn calc_bubblegum_checksums(storage: &Storage, epoch: u32, only_tree: Option<Pubkey>) {
    // For now let's just ignore trees that are update in the process of calculation,
    // anywhay we'll have a separate notification for each of tree late update.
    let trees_updated_in_the_process = calc_bubblegum_epoch(storage, epoch, only_tree).await;
    let invalidated_grand_epoch_trees =
        calc_bubblegum_grand_epoch(storage, grand_epoch_of_epoch(epoch), only_tree).await;
    if only_tree.is_none() {
        tracing::info!(
            "Calculated bubblegum epoch {epoch}. {} epoch and {} grand epoch trees were updated in the process",
            trees_updated_in_the_process.len(), invalidated_grand_epoch_trees.len(),
        );
    }
}

/// Calculates and stores bubblegum epoch checksums for bubblegum updates
/// received during the given epoch.
///
/// ## Args:
/// * `storage` - database
/// * `target_epoch` - the number of an epoch the checksum should be calculated for
async fn calc_bubblegum_epoch(
    storage: &Storage,
    target_epoch: u32,
    only_tree: Option<Pubkey>,
) -> Vec<Pubkey> {
    let mut to_recalc = Vec::new();
    let mut current_tree: Option<Pubkey> = None;

    let start_key = if let Some(tree) = only_tree {
        BubblegumChangeKey::tree_epoch_start_key(tree, target_epoch)
    } else {
        BubblegumChangeKey::epoch_start_key(target_epoch)
    };
    let mut it = storage.bubblegum_changes.iter(start_key);
    let mut hasher = Hasher::default();

    while let Some(Ok((k, v))) = it.next() {
        let Ok(change_key) = BubblegumChange::decode_key(k.to_vec()) else {
            continue;
        };
        if change_key.epoch > target_epoch {
            break;
        }
        if only_tree
            .map(|t| t != change_key.tree_pubkey)
            .unwrap_or(false)
        {
            break;
        }
        if current_tree != Some(change_key.tree_pubkey) {
            if current_tree.is_some() {
                // write checksum for previous tree
                let epoch_key = BubblegumEpochKey::new(current_tree.unwrap(), target_epoch);

                let epoch_val = BubblegumEpoch::from(hasher.result().to_bytes());
                let _ = storage
                    .bubblegum_epochs
                    .merge(epoch_key.clone(), epoch_val)
                    .await;

                if let Ok(Some(storage_consistency::BUBBLEGUM_EPOCH_INVALIDATED)) =
                    storage.bubblegum_epochs.get_async(epoch_key).await
                {
                    to_recalc.push(current_tree.unwrap());
                }
            }
            current_tree = Some(change_key.tree_pubkey);

            let new_epoch_key = BubblegumEpochKey::new(current_tree.unwrap(), target_epoch);
            let _ = storage
                .bubblegum_epochs
                .put_async(new_epoch_key, BUBBLEGUM_EPOCH_CALCULATING)
                .await;

            hasher = Hasher::default();
        }
        hasher.hash(&k);
        hasher.hash(&v);
    }

    if let Some(current_tree) = current_tree {
        let epoch_key = BubblegumEpochKey {
            tree_pubkey: current_tree,
            epoch_num: target_epoch,
        };
        let epoch_val = BubblegumEpoch::from(hasher.result().to_bytes());
        let _ = storage
            .bubblegum_epochs
            .merge(epoch_key.clone(), epoch_val)
            .await;
        if let Ok(Some(storage_consistency::BUBBLEGUM_EPOCH_INVALIDATED)) =
            storage.bubblegum_epochs.get_async(epoch_key).await
        {
            to_recalc.push(current_tree);
        }
    }

    to_recalc
}

async fn calc_bubblegum_grand_epoch(
    storage: &Storage,
    target_grand_epoch: u16,
    only_tree: Option<Pubkey>,
) -> Vec<Pubkey> {
    let mut to_recalc = Vec::new();
    let mut current_tree: Option<Pubkey> = None;
    let mut contains_invalidated_epoch = false;

    let start_key = if let Some(tree) = only_tree {
        BubblegumEpochKey::tree_grand_epoch_start_key(tree, target_grand_epoch)
    } else {
        BubblegumEpochKey::grand_epoch_start_key(target_grand_epoch)
    };
    let mut it = storage.bubblegum_epochs.iter(start_key);
    let mut hasher = Hasher::default();

    while let Some(Ok((k, v))) = it.next() {
        let Ok(epoch_key) = BubblegumEpoch::decode_key(k.to_vec()) else {
            continue;
        };
        let element_grand_epoch = grand_epoch_of_epoch(epoch_key.epoch_num);
        if element_grand_epoch > target_grand_epoch {
            break;
        }
        if only_tree
            .map(|t| t != epoch_key.tree_pubkey)
            .unwrap_or(false)
        {
            break;
        }
        if v.as_ref() == storage_consistency::BUBBLEGUM_EPOCH_INVALIDATED_BYTES.as_slice() {
            contains_invalidated_epoch = true;
            let new_grand_epoch_key =
                BubblegumGrandEpochKey::new(current_tree.unwrap(), target_grand_epoch);
            let _ = storage
                .bubblegum_grand_epochs
                .put_async(new_grand_epoch_key, BUBBLEGUM_GRAND_EPOCH_INVALIDATED)
                .await;
        }
        if current_tree != Some(epoch_key.tree_pubkey) {
            if current_tree.is_some() {
                if !contains_invalidated_epoch {
                    // write checksum for previous tree
                    let grand_epoch_key =
                        BubblegumGrandEpochKey::new(current_tree.unwrap(), target_grand_epoch);
                    let grand_epoch_val = BubblegumGrandEpoch::from(hasher.result().to_bytes());
                    let _ = storage
                        .bubblegum_grand_epochs
                        .merge(grand_epoch_key.clone(), grand_epoch_val)
                        .await;

                    if let Ok(Some(storage_consistency::BUBBLEGUM_GRAND_EPOCH_INVALIDATED)) =
                        storage
                            .bubblegum_grand_epochs
                            .get_async(grand_epoch_key)
                            .await
                    {
                        to_recalc.push(current_tree.unwrap());
                    }
                } else {
                    to_recalc.push(current_tree.unwrap());
                }
            }
            current_tree = Some(epoch_key.tree_pubkey);
            contains_invalidated_epoch = false;

            let new_grand_epoch_key =
                BubblegumGrandEpochKey::new(current_tree.unwrap(), target_grand_epoch);
            let _ = storage
                .bubblegum_grand_epochs
                .put_async(new_grand_epoch_key, BUBBLEGUM_GRAND_EPOCH_CALCULATING)
                .await;

            hasher = Hasher::default();
        } else if contains_invalidated_epoch {
            continue;
        }
        hasher.hash(&k);
        hasher.hash(&v);
    }

    if let Some(current_tree) = current_tree {
        let grand_epoch_key = BubblegumGrandEpochKey {
            tree_pubkey: current_tree,
            grand_epoch_num: target_grand_epoch,
        };
        let grand_epoch_val = BubblegumGrandEpoch::from(hasher.result().to_bytes());
        let _ = storage
            .bubblegum_grand_epochs
            .merge(grand_epoch_key.clone(), grand_epoch_val)
            .await;
        if let Ok(Some(storage_consistency::BUBBLEGUM_GRAND_EPOCH_INVALIDATED)) = storage
            .bubblegum_grand_epochs
            .get_async(grand_epoch_key)
            .await
        {
            to_recalc.push(current_tree);
        }
    }

    to_recalc
}

pub async fn calc_acc_nft_checksums(storage: &Storage, epoch: u32) {
    match calc_acc_latest_state(storage, epoch).await {
        Ok((invalidated_buckets, invalidated_grand_buckets)) => {
            calc_acc_buckets(storage, invalidated_buckets.iter()).await;
            calc_acc_grand_buckets(storage, invalidated_grand_buckets.iter()).await;
        }
        Err(e) => tracing::warn!("Error calculating accounts checksum: {e}"),
    };
}

async fn calc_acc_latest_state(
    storage: &Storage,
    target_epoch: u32,
) -> anyhow::Result<(HashSet<u16>, HashSet<u16>)> {
    let mut it = storage.acc_nft_changes.iter_start();
    let mut invalidated_buckets: HashSet<u16> = HashSet::new();
    let mut invalidated_grand_buckets: HashSet<u16> = HashSet::new();

    let Some(first_record) = it.next() else {
        return Ok((HashSet::new(), HashSet::new()));
    };
    let mut prev_change = AccountNftChange::decode_key(first_record?.0.to_vec())?;
    let mut changes_to_delete = Vec::new();

    while let Some(Ok((k, _v))) = it.next() {
        changes_to_delete.push(prev_change.clone());
        let next_change = AccountNftChange::decode_key(k.to_vec())?;
        if next_change.epoch > target_epoch {
            break;
        }

        if next_change.account_pubkey == prev_change.account_pubkey
            && next_change.epoch <= target_epoch
        {
            if next_change.slot > prev_change.slot
                || next_change.slot == prev_change.slot
                    && next_change.write_version > prev_change.write_version
            {
                prev_change = next_change.clone();
            }
        } else {
            update_acc_if_needed(
                storage,
                &prev_change,
                &mut invalidated_buckets,
                &mut invalidated_grand_buckets,
            )
            .await;

            let _ = storage
                .acc_nft_changes
                .delete_batch(changes_to_delete)
                .await;
            changes_to_delete = Vec::new();
            prev_change = next_change.clone();
        }
    }
    update_acc_if_needed(
        storage,
        &prev_change,
        &mut invalidated_buckets,
        &mut invalidated_grand_buckets,
    )
    .await;
    let _ = storage
        .acc_nft_changes
        .delete_batch(changes_to_delete)
        .await;

    Ok((invalidated_buckets, invalidated_grand_buckets))
}

async fn update_acc_if_needed(
    storage: &Storage,
    change: &AccountNftChangeKey,
    invalidated_buckets: &mut HashSet<u16>,
    invalidated_grand_buckets: &mut HashSet<u16>,
) {
    let acc_key = AccountNftKey::new(change.account_pubkey);

    let need_to_update = storage
        .acc_nft_last
        .get_async(acc_key.clone())
        .await
        .ok()
        .flatten()
        .map(|in_db| {
            change.data_hash != in_db.last_data_hash
                && (change.slot > in_db.last_slot
                    || change.slot == in_db.last_slot
                        && change.write_version > in_db.last_write_version)
        })
        .unwrap_or(true);

    if need_to_update {
        let _ = storage
            .acc_nft_last
            .put_async(
                acc_key,
                AccountNft::new(change.slot, change.write_version, change.data_hash),
            )
            .await;

        let bucket = bucket_for_acc(change.account_pubkey);
        let grand_bucket = grand_bucket_for_bucket(bucket);
        if !invalidated_grand_buckets.contains(&grand_bucket) {
            let _ = storage
                .acc_nft_grand_buckets
                .put_async(
                    AccountNftGrandBucketKey::new(grand_bucket),
                    ACC_GRAND_BUCKET_INVALIDATED,
                )
                .await;
            invalidated_grand_buckets.insert(grand_bucket);
        }

        if !invalidated_buckets.contains(&bucket) {
            let _ = storage
                .acc_nft_buckets
                .put_async(AccountNftBucketKey::new(bucket), ACC_BUCKET_INVALIDATE)
                .await;
            invalidated_buckets.insert(bucket);
        }
    }
}

async fn calc_acc_buckets<'a>(storage: &Storage, buckets: impl Iterator<Item = &'a u16>) {
    for bucket in buckets {
        let mut it = storage
            .acc_nft_last
            .iter(AccountNftKey::bucket_start_key(*bucket));
        let mut hasher = Hasher::default();
        while let Some(Ok((k, v))) = it.next() {
            if AccountNftKey::extract_bucket(&k) > *bucket {
                break;
            }
            hasher.hash(&k);
            // slot can vary because of forks and we are not interested in it
            hasher.hash(&AccountNft::clear_slot_bytes(&v));
        }
        // There is not need in merge operation that checks that the previous state was Calculating,
        // since we'll immediatelly detect a late update by finding a new change record.
        let _ = storage
            .acc_nft_buckets
            .put_async(
                AccountNftBucketKey::new(*bucket),
                AccountNftBucket::new(hasher.result().to_bytes()),
            )
            .await;
    }
}

async fn calc_acc_grand_buckets<'a>(
    storage: &Storage,
    grand_buckets: impl Iterator<Item = &'a u16>,
) {
    for grand_bucket in grand_buckets {
        let mut it = storage
            .acc_nft_buckets
            .iter(AccountNftBucketKey::grand_bucket_start_key(*grand_bucket));

        let mut hasher = Hasher::default();
        while let Some(Ok((k, v))) = it.next() {
            let is_for_next_grand_bucket = AccountNftBucket::decode_key(k.to_vec())
                .map(|bucket_key| grand_bucket_for_bucket(bucket_key.bucket) > *grand_bucket)
                .unwrap_or(false);
            if is_for_next_grand_bucket {
                break;
            }
            hasher.hash(&k);
            hasher.hash(&v);
        }
        let _ = storage
            .acc_nft_grand_buckets
            .put_async(
                AccountNftGrandBucketKey::new(*grand_bucket),
                AccountNftGrandBucket::new(hasher.result().to_bytes()),
            )
            .await;
    }
}

/// Calculates hash for solana account data.
/// This is used for account NFTs, to solve the duplicates problem
/// caused by solana forks and by fetching of same data from multiple
/// different sources.
pub fn calc_solana_account_data_hash(data: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(data)
}
