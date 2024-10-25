use rocks_db::{
    column::TypedColumn,
    storage_consistency::{
        self, bucket_for_acc, grand_bucket_for_acc, grand_bucket_for_bucket, grand_epoch_of_epoch,
        AccountNft, AccountNftBucket, AccountNftBucketKey, AccountNftChange, AccountNftChangeKey,
        AccountNftGrandBucket, AccountNftGrandBucketKey, AccountNftKey, BubblegumChange,
        BubblegumChangeKey, BubblegumEpoch, BubblegumEpochKey, BubblegumGrandEpoch,
        BubblegumGrandEpochKey, ACC_BUCKET_INVALIDATE, ACC_GRAND_BUCKET_INVALIDATE,
    },
    Storage,
};
use solana_sdk::{hash::Hasher, pubkey::Pubkey};
use std::{collections::HashSet, sync::Arc, time::Duration};
use storage_consistency::{BUBBLEGUM_EPOCH_CALCULATING, BUBBLEGUM_GRAND_EPOCH_CALCULATING};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};

pub enum ConsistencyCalcMsg {
    EpochChanged { epoch: u32 },
    BubblegumUpdated { tree: Pubkey, slot: u64 },
    AccUpdated { account: Pubkey, slot: u64 },
}

/// An entry point for checksums calculation component.
/// Should be called from "main".
pub async fn run_bg_consistency_calculator(
    mut rcv: UnboundedReceiver<ConsistencyCalcMsg>,
    storage: Arc<Storage>,
) {
    tokio::spawn(async move {
        let mut _bubblegum_task = None;

        // Don't look here too much for now, it is to be implemented
        loop {
            match rcv.recv().await {
                Some(msg) => match msg {
                    ConsistencyCalcMsg::EpochChanged { epoch } => {
                        // TODO: check sequnce_consistent.rs before calculating the checksums

                        // TODO: Scheduler epoch calc. Should calc immediately or wait, since more data can come?
                        let t = schedule_bublegum_calc(
                            Duration::from_secs(300),
                            storage.clone(),
                            epoch,
                        );
                        _bubblegum_task = Some(t);
                    }
                    ConsistencyCalcMsg::BubblegumUpdated { tree: _, slot: _ } => todo!(),
                    ConsistencyCalcMsg::AccUpdated {
                        account: _,
                        slot: _,
                    } => todo!(),
                },
                None => break,
            }
        }
    });
}

/// After a given lag, launches checksum calculation for the given epoch.
///
/// ## Args:
/// * `start_lag` - time to wait before actual calculation.
///   Required because we might want to wait for late updates.
/// * `storage` - database
/// * `epoch` - the epoch the calculation should be done for
fn schedule_bublegum_calc(
    start_lag: Duration,
    storage: Arc<Storage>,
    epoch: u32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(start_lag).await;
        calc_bubblegum_checksums(&storage, epoch).await;
    })
}

pub async fn calc_bubblegum_checksums(storage: &Storage, epoch: u32) {
    calc_bubblegum_epoch(&storage, epoch).await;
    calc_bubblegum_grand_epoch(&storage, grand_epoch_of_epoch(epoch)).await;
}

/// Calculates and stores bubblegum epoch checksums for bubblegum updates
/// received during the given epoch.
///
/// ## Args:
/// * `storage` - database
/// * `target_epoch` - the number of an epoch the checksum should be calculated for
async fn calc_bubblegum_epoch(storage: &Storage, target_epoch: u32) {
    // iterate over changes and calculate checksum per tree.

    let mut current_tree: Option<Pubkey> = None;

    let mut it = storage
        .bubblegum_changes
        .iter(BubblegumChangeKey::epoch_start_key(target_epoch));
    let mut hasher = Hasher::default();

    while let Some(Ok((k, v))) = it.next() {
        let Ok(change_key) = BubblegumChange::decode_key(k.to_vec()) else {
            continue;
        };
        if change_key.epoch > target_epoch {
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
                    storage.bubblegum_epochs.get(epoch_key)
                {
                    // TODO: Means more changes for the tree have come while the epoch chechsum calculation was in process.
                    //       How to handle?
                }
            }
            current_tree = Some(change_key.tree_pubkey);

            let new_epoch_key = BubblegumEpochKey::new(current_tree.unwrap(), target_epoch);
            storage
                .bubblegum_epochs
                .put(new_epoch_key, BUBBLEGUM_EPOCH_CALCULATING);

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
        let _ = storage.bubblegum_epochs.merge(epoch_key, epoch_val).await;
    }
}

async fn calc_bubblegum_grand_epoch(storage: &Storage, target_grand_epoch: u16) {
    let mut current_tree: Option<Pubkey> = None;

    let mut it = storage
        .bubblegum_epochs
        .iter(BubblegumEpochKey::grand_epoch_start_key(target_grand_epoch));
    let mut hasher = Hasher::default();

    while let Some(Ok((k, v))) = it.next() {
        let Ok(epoch_key) = BubblegumEpoch::decode_key(k.to_vec()) else {
            continue;
        };
        let element_grand_epoch = grand_epoch_of_epoch(epoch_key.epoch_num);
        if element_grand_epoch > target_grand_epoch {
            break;
        }
        if current_tree != Some(epoch_key.tree_pubkey) {
            if current_tree.is_some() {
                // write checksum for previous tree
                let grand_epoch_key =
                    BubblegumGrandEpochKey::new(current_tree.unwrap(), target_grand_epoch);
                let grand_epoch_val = BubblegumGrandEpoch::from(hasher.result().to_bytes());
                let _ = storage
                    .bubblegum_grand_epochs
                    .merge(grand_epoch_key.clone(), grand_epoch_val)
                    .await;

                if let Ok(Some(storage_consistency::BUBBLEGUM_GRAND_EPOCH_INVALIDATED)) =
                    storage.bubblegum_grand_epochs.get(grand_epoch_key)
                {
                    // TODO: ???
                }
            }
            current_tree = Some(epoch_key.tree_pubkey);

            let new_grand_epoch_key =
                BubblegumGrandEpochKey::new(current_tree.unwrap(), target_grand_epoch);
            let _ = storage
                .bubblegum_grand_epochs
                .put(new_grand_epoch_key, BUBBLEGUM_GRAND_EPOCH_CALCULATING);

            hasher = Hasher::default();
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
            .merge(grand_epoch_key, grand_epoch_val)
            .await;
    }
}

pub fn calc_acc_nft_checksums(storage: &Storage, epoch: u32) {
    let (invalidated_buckets, invalidated_grand_buckets) = calc_acc_latest_state(&storage, epoch);
    calc_acc_buckets(&storage, invalidated_buckets.iter());
    calc_acc_grand_buckets(&storage, invalidated_grand_buckets.iter())
}

fn calc_acc_latest_state(storage: &Storage, target_epoch: u32) -> (HashSet<u16>, HashSet<u16>) {
    let mut it = storage.acc_nft_changes.iter_start();
    let mut invalidated_buckets: HashSet<u16> = HashSet::new();
    let mut invalidated_grand_buckets: HashSet<u16> = HashSet::new();

    while let Some(Ok((k, _v))) = it.next() {
        let Ok(change_key) = AccountNftChange::decode_key(k.to_vec()) else {
            continue;
        };
        if change_key.epoch > target_epoch {
            break;
        }

        let acc_key = AccountNftKey::new(change_key.account_pubkey);

        let need_to_update = storage
            .acc_nft_last
            .get(acc_key.clone())
            .ok()
            .flatten()
            .map(|prev| {
                change_key.slot > prev.last_slot
                    || change_key.slot == prev.last_slot
                        && change_key.write_version > prev.last_write_version
            })
            .unwrap_or(true);

        if need_to_update {
            storage.acc_nft_last.put(
                acc_key,
                AccountNft::new(change_key.slot, change_key.write_version),
            );

            let bucket = bucket_for_acc(change_key.account_pubkey);
            let grand_bucket = grand_bucket_for_bucket(bucket);
            if !invalidated_grand_buckets.contains(&grand_bucket) {
                let _ = storage.acc_nft_grand_buckets.put(
                    AccountNftGrandBucketKey::new(grand_bucket),
                    ACC_GRAND_BUCKET_INVALIDATE,
                );
                invalidated_grand_buckets.insert(grand_bucket);
            }

            if !invalidated_buckets.contains(&bucket) {
                let _ = storage
                    .acc_nft_buckets
                    .put(AccountNftBucketKey::new(bucket), ACC_BUCKET_INVALIDATE);
                invalidated_buckets.insert(bucket);
            }
        }
        storage.acc_nft_changes.delete(change_key);
    }
    (invalidated_buckets, invalidated_grand_buckets)
}

fn calc_acc_buckets<'a>(storage: &Storage, buckets: impl Iterator<Item = &'a u16>) {
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
            hasher.hash(&v);
        }
        // There is not need in merge operation that checks that the previous state was Calculating,
        // since we'll immediatelly detect a late update by finding a new change record.
        storage.acc_nft_buckets.put(
            AccountNftBucketKey::new(*bucket),
            AccountNftBucket::new(hasher.result().to_bytes()),
        );
    }
}

fn calc_acc_grand_buckets<'a>(storage: &Storage, grand_buckets: impl Iterator<Item = &'a u16>) {
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
        storage.acc_nft_grand_buckets.put(
            AccountNftGrandBucketKey::new(*grand_bucket),
            AccountNftGrandBucket::new(hasher.result().to_bytes()),
        );
    }
}
