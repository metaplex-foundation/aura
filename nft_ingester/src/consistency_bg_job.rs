//! This module contains background job that after each epoch (10 000 slots) is finished,
//! calls Aura peers to get their epoch ckecksums, and searches for missing data.
//! When a missing data is found, corresponding blocks are requsted from
//! peers.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use grpc::consistencyapi_impl::{AccConsistencyApiClientImpl, BbgmConsistencyApiClientImpl};
use interface::signature_persistence::BlockConsumer;
use interface::signature_persistence::BlockProducer;
use interface::{
    aura_peers_provides::AuraPeersProvides,
    checksums_storage::{
        AccBucketCksm, AccChecksumServiceApi, AccGrandBucketCksm, AccLastChange, BbgmChangeRecord,
        BbgmChecksumServiceApi, BbgmEpochCksm, BbgmGrandEpochCksm, Chksm,
    },
};
use metrics_utils::Peer2PeerConsistencyMetricsConfig;
use rocks_db::{
    storage_consistency::{
        calc_exchange_slot_for_epoch, epoch_of_slot, grand_epoch_of_epoch, last_tracked_slot,
        slots_to_time,
    },
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tokio::time::Instant;
use url::Url;

use crate::{
    consistency_calculator::{get_calculating_acc_epoch, get_last_calculated_bbgm_epoch},
    gapfiller::process_asset_details_stream,
};

/// Read peer URLs from given file.
/// Each URL is expected to be on a separate line.
pub struct FileSrcAuraPeersProvides {
    pub file_path: String,
}

impl FileSrcAuraPeersProvides {
    pub fn new(file_path: String) -> FileSrcAuraPeersProvides {
        FileSrcAuraPeersProvides { file_path }
    }
}

#[async_trait::async_trait]
impl AuraPeersProvides for FileSrcAuraPeersProvides {
    async fn list_trusted_peers(&self) -> Vec<String> {
        match tokio::fs::read_to_string(&self.file_path).await {
            Ok(s) => {
                let mut result = Vec::new();
                for line in s.lines().filter(|s| !s.is_empty()).map(|s| s.trim()) {
                    if Url::parse(line).is_err() {
                        tracing::warn!("Invalid peer URL: {line}");
                    } else {
                        result.push(line.to_string());
                    }
                }
                result
            }
            Err(e) => {
                tracing::error!("Unable to read peers file: {e}");
                Vec::new()
            }
        }
    }
}

/// Spawns in background functionality for checksums and missing blocks fetching.
pub fn run_consistenct_bg_job(
    storage: Arc<Storage>,
    peers_provider: Arc<dyn AuraPeersProvides + Send + Sync>,
    force_reingestable_slot_processor: Arc<dyn BlockConsumer + Send + Sync>,
    metrics: Arc<Peer2PeerConsistencyMetricsConfig>,
) {
    tokio::spawn(async move {
        let _ = run_peers_checking_loop(
            storage,
            peers_provider,
            force_reingestable_slot_processor,
            metrics,
        )
        .await;
    });
}

/// Background task that waits for the current epoch to end, then waits a little
/// for late data to come, and the epoch checksums to be calculated,
/// and after that runs an exchange of bubblegum and account checksums
/// to identify changes that had been missing on our side.
/// After missing changes are found, it fetches corresponding blocks from the peer
/// and used gap filling mechanism to process them
///
/// ## Args:
/// * storage - rocksdb storage
/// * peers_provider - provider of trusted peers
async fn run_peers_checking_loop(
    storage: Arc<Storage>,
    peers_provider: Arc<dyn AuraPeersProvides + Send + Sync>,
    force_reingestable_slot_processor: Arc<dyn BlockConsumer + Send + Sync>,
    metrics: Arc<Peer2PeerConsistencyMetricsConfig>,
) {
    let mut last_processed_epoch = 0u32;

    loop {
        let next_processing_slot = calc_exchange_slot_for_epoch(last_processed_epoch + 1);
        let current_slot = last_tracked_slot();
        if current_slot < next_processing_slot {
            let duration = slots_to_time(next_processing_slot - current_slot);
            tokio::time::sleep(duration).await;
            continue;
        }

        let epoch_to_process = epoch_of_slot(next_processing_slot);

        let bbgm_task = {
            let storage = storage.clone();
            let peers_provider = peers_provider.clone();
            let force_reingestable_slot_processor = force_reingestable_slot_processor.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                exchange_bbgms_with_peers(
                    epoch_to_process,
                    storage,
                    peers_provider,
                    force_reingestable_slot_processor,
                    metrics,
                )
                .await
            })
        };
        let acc_task = {
            let storage = storage.clone();
            let peers_provider = peers_provider.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                exchange_account_with_peers(storage, peers_provider, metrics.as_ref()).await
            })
        };
        let _ = bbgm_task.await;
        let _ = acc_task.await;

        last_processed_epoch = epoch_to_process;
    }
}

/// Exchanges bubblegum checksum with peers to identify missing bubblegum change,
/// and requests these missing changes from peers.
///
/// ## Args:
/// * epoch - epoch we want the checksums to be exchanged
/// * storage - local database
/// * peers_provider - source of trusted peers
async fn exchange_bbgms_with_peers(
    epoch: u32,
    storage: Arc<Storage>,
    peers_provider: Arc<dyn AuraPeersProvides + Send + Sync>,
    force_reingestable_slot_processor: Arc<dyn BlockConsumer + Send + Sync>,
    metrics: Arc<Peer2PeerConsistencyMetricsConfig>,
) {
    tracing::info!("Starting bubblegum changes peer-to-peer exchange for epoch={epoch}");
    while get_last_calculated_bbgm_epoch()
        .map(|e| e <= epoch)
        .unwrap_or(false)
    {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let grand_epoch = grand_epoch_of_epoch(epoch);

    let mut missing_bbgm_changes: HashMap<BbgmChangeRecord, HashSet<usize>> = HashMap::new();
    let trusted_peers = peers_provider.list_trusted_peers().await;

    for (peer_ind, trusted_peer) in trusted_peers.iter().enumerate() {
        tracing::info!("Exchanging bubblegum changes for epoch={epoch} with peer={trusted_peer}");
        let Ok(client) = BbgmConsistencyApiClientImpl::new(
            trusted_peer,
            Some(metrics.bbgm_consistency_grpc_client.clone()),
        )
        .await
        .map(Arc::new) else {
            tracing::warn!("Cannot connect to peer={trusted_peer}");
            continue;
        };

        let changes_we_miss = compare_bbgm_with_peer(
            grand_epoch,
            storage.as_ref(),
            client.as_ref(),
            metrics.as_ref(),
        )
        .await;

        metrics
            .found_missing_bubblegums
            .inc_by(changes_we_miss.len() as i64);

        for change in changes_we_miss {
            match missing_bbgm_changes.get_mut(&change) {
                Some(peers_have_change) => {
                    peers_have_change.insert(peer_ind);
                }
                None => {
                    missing_bbgm_changes.insert(change, HashSet::from([peer_ind]));
                }
            };
        }
    }
    handle_missing_bbgm_changes(
        missing_bbgm_changes,
        trusted_peers,
        force_reingestable_slot_processor,
    )
    .await
}

/// For given grand epoch, compares our bubblegum changes
/// (by comparing, first, grand epoch checksums and then epoch checksums)
/// with corresponding bubblegum changes of peer,
/// and returns changes that are missing on our side.
///
/// ## Args:
/// * grand_epoch - a grand epoch the exchange is performed for
/// * we - local storage of bubblegum changes (rocksdb storage)
/// * peer - GRPC client for peer
pub async fn compare_bbgm_with_peer(
    grand_epoch: u16,
    we: &impl BbgmChecksumServiceApi,
    peer: &impl BbgmChecksumServiceApi,
    metrics: &Peer2PeerConsistencyMetricsConfig,
) -> Vec<BbgmChangeRecord> {
    let mut result = Vec::new();
    let Ok(peer_ge_chksms) = peer
        .list_grand_epoch_checksums(grand_epoch, None, None)
        .await
    else {
        return Vec::new();
    };

    let start = Instant::now();
    let my_ge_chksms = match we.list_grand_epoch_checksums(grand_epoch, None, None).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Error reading grand epochs from DB: {}", e.to_string());
            return Vec::new();
        }
    };
    metrics
        .db_bubblegum_get_grand_epochs_latency
        .observe(start.elapsed().as_secs_f64());

    let ge_cmp_res = cmp(&my_ge_chksms, &peer_ge_chksms);
    let ge_trees_to_check = ge_cmp_res
        .we_miss
        .iter()
        .chain(ge_cmp_res.different.iter())
        .map(|&a| a.tree_pubkey)
        .collect::<Vec<_>>();
    for tree_pk in ge_trees_to_check {
        let Ok(peer_e_chksms) = peer.list_epoch_checksums(grand_epoch, tree_pk).await else {
            continue;
        };

        let start = Instant::now();
        let my_e_chksms = match we.list_epoch_checksums(grand_epoch, tree_pk).await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Error reading epochs from DB: {}", e.to_string());
                return result;
            }
        };
        metrics
            .db_bubblegum_get_epochs_latency
            .observe(start.elapsed().as_secs_f64());

        let e_cmp_res = cmp(&my_e_chksms, &peer_e_chksms);
        let epochs_to_check = e_cmp_res
            .we_miss
            .iter()
            .chain(e_cmp_res.different.iter())
            .map(|&a| (a.epoch, a.tree_pubkey))
            .collect::<Vec<_>>();
        for (epoch, tree_pubkey) in epochs_to_check {
            let Ok(peer_changes) = peer
                .list_epoch_changes(epoch, tree_pubkey, None, None)
                .await
            else {
                continue;
            };

            let start = Instant::now();
            let my_changes = match we.list_epoch_changes(epoch, tree_pubkey, None, None).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Error reading bubblegum changes from DB: {}", e.to_string());
                    return result;
                }
            };
            metrics
                .db_bubblegum_get_changes_latency
                .observe(start.elapsed().as_secs_f64());

            let changes_cmp_res = cmp(&my_changes, &peer_changes);
            result.extend(changes_cmp_res.we_miss.into_iter().map(|a| a.to_owned()));
            // TODO: track different
        }
    }

    result
}

#[allow(clippy::while_let_on_iterator)]
async fn handle_missing_bbgm_changes(
    missing_accounts: HashMap<BbgmChangeRecord, HashSet<usize>>,
    trusted_peers: Vec<String>,
    force_reingestable_slot_processor: Arc<dyn BlockConsumer + Send + Sync>,
) {
    let mut clients: HashMap<usize, grpc::client::Client> = HashMap::new();

    let mut already_fetched_slots = HashSet::<u64>::new();
    for (change, peers) in missing_accounts {
        if already_fetched_slots.contains(&change.slot) {
            continue;
        }
        let mut it = peers.iter();
        while let Some(peer_ind) = it.next() {
            let client = if let Some(client) = clients.get_mut(peer_ind) {
                client
            } else {
                let Ok(peer_client) =
                    grpc::client::Client::connect_to_url(&trusted_peers[*peer_ind]).await
                else {
                    continue;
                };
                clients.insert(*peer_ind, peer_client);
                clients.get_mut(peer_ind).unwrap()
            };
            if let Ok(block) = client
                .get_block(change.slot, Option::<Arc<grpc::client::Client>>::None)
                .await
            {
                let _ = force_reingestable_slot_processor
                    .consume_block(change.slot, block)
                    .await;
            }
            break;
        }
        already_fetched_slots.insert(change.slot);
    }
}

/// Exchanges account NFTs checksum with peers to identify missing account NFT changes,
/// and requests these missing changes from peers.
///
/// ## Args:
/// * storage - local database
/// * peers_provider - source of trusted peers
async fn exchange_account_with_peers(
    storage: Arc<Storage>,
    peers_provider: Arc<dyn AuraPeersProvides + Send + Sync>,
    metrics: &Peer2PeerConsistencyMetricsConfig,
) {
    tracing::info!("Starting account NFT peer-to-peer exchange");
    while get_calculating_acc_epoch().is_some() {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let mut missing_accounts: HashMap<AccLastChange, HashSet<usize>> = HashMap::new();
    let trusted_peers = peers_provider.list_trusted_peers().await;

    for (peer_ind, trusted_peer) in trusted_peers.iter().enumerate() {
        tracing::info!("Exchanging account NFT with peer={trusted_peer}");
        let Ok(client) = AccConsistencyApiClientImpl::new(
            trusted_peer,
            Some(metrics.acc_consistency_grpc_client.clone()),
        )
        .await
        .map(Arc::new) else {
            tracing::warn!("Cannot connect to peer={trusted_peer}");
            continue;
        };

        let accs_we_miss: Vec<AccLastChange> =
            compare_acc_with_peer(storage.as_ref(), client.as_ref(), metrics).await;

        metrics
            .found_missing_accounts
            .set(accs_we_miss.len() as i64);

        for change in accs_we_miss {
            match missing_accounts.get_mut(&change) {
                Some(peers_have_change) => {
                    peers_have_change.insert(peer_ind);
                }
                None => {
                    missing_accounts.insert(change, HashSet::from([peer_ind]));
                }
            };
        }
    }
    handle_missing_accs(missing_accounts, trusted_peers, storage).await
}

/// Compares our account NFT info with corresponding account records on peer,
/// and returns records that are missing on our side.
///
/// ## Args:
/// * we - local storage of account NFTs (rocksdb storage)
/// * peer - GRPC client for peer
pub async fn compare_acc_with_peer(
    storage: &impl AccChecksumServiceApi,
    client: &impl AccChecksumServiceApi,
    metrics: &Peer2PeerConsistencyMetricsConfig,
) -> Vec<AccLastChange> {
    let mut result = Vec::new();

    let Ok(peer_grand_buckets) = client.list_grand_buckets().await else {
        return result;
    };

    let start = Instant::now();
    let my_grand_buckets = match storage.list_grand_buckets().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Error reading grand buckets from DB: {}", e.to_string());
            return result;
        }
    };
    metrics
        .db_account_get_grand_buckets_latency
        .observe(start.elapsed().as_secs_f64());

    let gb_cmp_res = cmp(&peer_grand_buckets, &my_grand_buckets);

    let grand_buckets_to_check = gb_cmp_res
        .we_miss
        .iter()
        .chain(gb_cmp_res.different.iter())
        .map(|&a| a.grand_bucket)
        .collect::<Vec<_>>();

    for grand_bucket in grand_buckets_to_check {
        let Ok(peer_buckets) = client.list_bucket_checksums(grand_bucket).await else {
            continue;
        };

        let start = Instant::now();
        let my_buckets = match storage.list_bucket_checksums(grand_bucket).await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Error reading buckets from DB: {}", e.to_string());
                return result;
            }
        };
        metrics
            .db_account_get_buckets_latency
            .observe(start.elapsed().as_secs_f64());

        let b_cmp_res = cmp(&peer_buckets, &my_buckets);

        let buckets_to_check = b_cmp_res
            .we_miss
            .iter()
            .chain(b_cmp_res.different.iter())
            .map(|&a| a.bucket)
            .collect::<Vec<_>>();

        for bucket in buckets_to_check {
            let Ok(peer_accounts) = client.list_accounts(bucket, None, None).await else {
                continue;
            };

            let start = Instant::now();
            let my_accounts = match storage.list_accounts(bucket, None, None).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Error reading accounts from DB: {}", e.to_string());
                    return result;
                }
            };
            metrics
                .db_account_get_latests_latency
                .observe(start.elapsed().as_secs_f64());

            let acc_cmp_res = cmp(&peer_accounts, &my_accounts);
            result.extend(acc_cmp_res.we_miss.into_iter().map(|a| a.to_owned()));
        }
    }
    result
}

#[allow(clippy::while_let_on_iterator)]
async fn handle_missing_accs(
    missing_accounts: HashMap<AccLastChange, HashSet<usize>>,
    trusted_peers: Vec<String>,
    storage: Arc<Storage>,
) {
    let mut clients: HashMap<usize, grpc::client::Client> = HashMap::new();
    let mut already_fetched_slots = HashSet::<u64>::new();

    for (change, peers) in missing_accounts {
        if already_fetched_slots.contains(&change.slot) {
            continue;
        }
        let mut it = peers.iter();
        while let Some(peer_ind) = it.next() {
            let client = if let Some(client) = clients.get_mut(peer_ind) {
                client
            } else {
                let Ok(peer_client) =
                    grpc::client::Client::connect_to_url(&trusted_peers[*peer_ind]).await
                else {
                    continue;
                };
                clients.insert(*peer_ind, peer_client);
                clients.get_mut(peer_ind).unwrap()
            };

            let (_snd, rx) = tokio::sync::broadcast::channel(1);
            process_asset_details_stream(
                rx,
                storage.clone(),
                change.slot,
                change.slot,
                client.clone(),
            )
            .await;
            break;
        }
        already_fetched_slots.insert(change.slot);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CmpRes<'a, T> {
    pub we_miss: Vec<&'a T>,
    pub they_miss: Vec<&'a T>,
    pub different: Vec<&'a T>,
}

#[allow(
    clippy::collapsible_else_if,
    clippy::comparison_chain,
    clippy::needless_range_loop
)]
fn cmp<'a, T>(we: &'a [T], they: &'a [T]) -> CmpRes<'a, T>
where
    T: AsKeyVal,
    T::Key: PartialEq + Ord,
    T::Val: PartialEq,
{
    let mut we_ind = 0usize;
    let mut they_ind = 0usize;

    let mut we_miss = Vec::new();
    let mut they_miss = Vec::new();
    let mut different = Vec::new();

    while we_ind < we.len() && they_ind < they.len() {
        let we_key = we[we_ind].key();
        let they_key = they[they_ind].key();

        if we_key < they_key {
            if we[we_ind].val().is_some() {
                they_miss.push(&we[we_ind]);
            }
            we_ind += 1;
        } else if we_key > they_key {
            if they[they_ind].val().is_some() {
                we_miss.push(&they[they_ind]);
            }
            they_ind += 1;
        } else {
            if we[we_ind].val() == they[they_ind].val()
                || we[we_ind].val().is_none()
                || they[they_ind].val().is_none()
            {
                we_ind += 1;
                they_ind += 1;
            } else {
                different.push(&we[we_ind]);
                we_ind += 1;
                they_ind += 1;
            }
        }
    }
    for i in we_ind..we.len() {
        if we[i].val().is_some() {
            they_miss.push(&we[i]);
        }
    }
    for i in they_ind..they.len() {
        if they[i].val().is_some() {
            we_miss.push(&they[i]);
        }
    }
    CmpRes {
        we_miss,
        they_miss,
        different,
    }
}

pub trait AsKeyVal {
    type Key;
    type Val;
    fn key(&self) -> Self::Key;
    fn val(&self) -> Option<Self::Val>;
}

impl AsKeyVal for BbgmGrandEpochCksm {
    type Key = Pubkey;
    type Val = [u8; 32];
    fn key(&self) -> Self::Key {
        self.tree_pubkey
    }
    fn val<'a>(&self) -> Option<Self::Val> {
        self.checksum
    }
}

impl AsKeyVal for BbgmEpochCksm {
    type Key = (u32, Pubkey);
    type Val = [u8; 32];
    fn key(&self) -> Self::Key {
        (self.epoch, self.tree_pubkey)
    }
    fn val(&self) -> Option<Self::Val> {
        self.checksum
    }
}

impl AsKeyVal for BbgmChangeRecord {
    type Key = Self;
    type Val = ();
    fn key(&self) -> Self::Key {
        self.to_owned()
    }
    fn val(&self) -> Option<Self::Val> {
        Some(())
    }
}

impl AsKeyVal for AccGrandBucketCksm {
    type Key = u16;
    type Val = Chksm;
    fn key(&self) -> Self::Key {
        self.grand_bucket
    }
    fn val(&self) -> Option<Self::Val> {
        self.checksum
    }
}

impl AsKeyVal for AccBucketCksm {
    type Key = u16;
    type Val = Chksm;
    fn key(&self) -> Self::Key {
        self.bucket
    }
    fn val(&self) -> Option<Self::Val> {
        self.checksum
    }
}

impl AsKeyVal for AccLastChange {
    type Key = Self;
    type Val = ();
    fn key(&self) -> Self::Key {
        self.to_owned()
    }
    fn val(&self) -> Option<Self::Val> {
        Some(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cmp() {
        {
            let list_1 = [
                (1, Some('a')),
                (2, Some('b')),
                (3, Some('c')),
                (5, Some('e')),
            ];
            let list_2 = [(1, Some('a')), (2, Some('z')), (4, Some('d')), (5, None)];

            let r = cmp(&list_1, &list_2);
            assert_eq!(
                r,
                CmpRes {
                    we_miss: vec![&(4, Some('d'))],
                    they_miss: vec![&(3, Some('c'))],
                    different: vec![&(2, Some('b'))]
                }
            );
        }

        {
            let list_1 = [
                (0, Some('y')),
                (1, Some('a')),
                (2, Some('b')),
                (3, Some('c')),
                (5, Some('e')),
            ];
            let list_2 = [(1, Some('a')), (2, Some('z')), (4, Some('d')), (5, None)];

            let result = cmp(&list_1, &list_2);
            assert_eq!(
                result,
                CmpRes {
                    we_miss: vec![&(4, Some('d'))],
                    they_miss: vec![&(0, Some('y')), &(3, Some('c'))],
                    different: vec![&(2, Some('b'))]
                }
            );
        }
    }

    impl AsKeyVal for (i32, Option<char>) {
        type Key = i32;

        type Val = char;

        fn key(&self) -> Self::Key {
            self.0
        }

        fn val(&self) -> Option<Self::Val> {
            self.1
        }
    }
}
