use std::{collections::HashSet, fs::File, sync::Arc};

use entities::enums::AssetType;
use metrics_utils::SynchronizerMetricsConfig;
use num_bigint::BigUint;
use postgre_client::storage_traits::{AssetIndexStorage, NFTSemaphores};
use rocks_db::{
    key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey},
    storage_traits::{AssetIndexStorage as AssetIndexSourceStorage, AssetUpdatedKey},
};
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinSet;
use tracing::warn;

use crate::error::IngesterError;

const BUF_CAPACITY: usize = 1024 * 1024 * 32;
const NFT_SHARDS: u64 = 32;
const FUNGIBLE_SHARDS: u64 = 16;
#[derive(Debug)]
pub struct SyncState {
    last_indexed_key: Option<AssetUpdatedKey>,
    last_known_key: AssetUpdatedKey,
}
#[derive(Debug)]
pub enum SyncStatus {
    FullSyncRequired(SyncState),
    RegularSyncRequired(SyncState),
    NoSyncRequired,
}

pub struct Synchronizer<T, U>
where
    T: AssetIndexSourceStorage,
    U: AssetIndexStorage,
{
    rocks_primary_storage: Arc<T>,
    pg_index_storage: Arc<U>,
    dump_synchronizer_batch_size: usize,
    dump_path: String,
    metrics: Arc<SynchronizerMetricsConfig>,
    parallel_tasks: usize,
}

impl<T, U> Synchronizer<T, U>
where
    T: AssetIndexSourceStorage + Send + Sync + 'static,
    U: AssetIndexStorage + Clone + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rocks_primary_storage: Arc<T>,
        pg_index_storage: Arc<U>,
        dump_synchronizer_batch_size: usize,
        dump_path: String,
        metrics: Arc<SynchronizerMetricsConfig>,
        parallel_tasks: usize,
    ) -> Self {
        Synchronizer {
            rocks_primary_storage,
            pg_index_storage,
            dump_synchronizer_batch_size,
            dump_path,
            metrics,
            parallel_tasks,
        }
    }

    pub async fn nft_run(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
        timeout_duration: tokio::time::Duration,
    ) {
        while rx.is_empty() {
            if let Err(e) = self.synchronize_nft_asset_indexes(rx, run_full_sync_threshold).await {
                tracing::error!("Non fungible synchronization failed: {:?}", e);
            } else {
                tracing::info!("Non fungible synchronization finished successfully");
            }
            if rx.is_empty() {
                tokio::time::sleep(timeout_duration).await;
            }
        }
    }

    pub async fn fungible_run(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
        timeout_duration: tokio::time::Duration,
    ) {
        while rx.is_empty() {
            if let Err(e) =
                self.synchronize_fungible_asset_indexes(rx, run_full_sync_threshold).await
            {
                tracing::error!("Fungible synchronization failed: {:?}", e);
            } else {
                tracing::info!("Fungible synchronization finished successfully");
            }
            if rx.is_empty() {
                tokio::time::sleep(timeout_duration).await;
            }
        }
    }

    pub async fn get_sync_state(
        &self,
        run_full_sync_threshold: i64,
        asset_type: AssetType,
    ) -> Result<SyncStatus, IngesterError> {
        let last_indexed_key = self.pg_index_storage.fetch_last_synced_id(asset_type).await?;
        let last_indexed_key = last_indexed_key.map(decode_u64x2_pubkey).transpose()?;

        // Fetch the last known key from the primary storage
        let (last_key, prefix) = match asset_type {
            AssetType::NonFungible => {
                (self.rocks_primary_storage.last_known_nft_asset_updated_key()?, "nft")
            },
            AssetType::Fungible => {
                (self.rocks_primary_storage.last_known_fungible_asset_updated_key()?, "fungible")
            },
        };
        let Some(last_key) = last_key else {
            return Ok(SyncStatus::NoSyncRequired);
        };

        if last_indexed_key.is_none() {
            return Ok(SyncStatus::FullSyncRequired(SyncState {
                last_indexed_key: None,
                last_known_key: last_key,
            }));
        }
        let last_known_seq = last_key.seq as i64;
        self.metrics.set_last_synchronized_slot(
            &format!("last_known_updated_{}_seq", prefix),
            last_known_seq,
        );
        self.metrics.set_last_synchronized_slot(
            &format!("last_known_updated_{}_slot", prefix),
            last_key.slot as i64,
        );

        self.metrics.set_last_synchronized_slot(
            &format!("last_synchronized_{}_slot", prefix),
            last_indexed_key.as_ref().map(|k| k.slot).unwrap_or_default() as i64,
        );
        self.metrics.set_last_synchronized_slot(
            &format!("last_synchronized_{}_seq", prefix),
            last_indexed_key.as_ref().map(|k| k.seq).unwrap_or_default() as i64,
        );
        if let Some(last_indexed_key) = &last_indexed_key {
            if last_indexed_key.seq >= last_key.seq {
                return Ok(SyncStatus::NoSyncRequired);
            }
            let last_indexed_seq = last_indexed_key.seq as i64;
            if run_full_sync_threshold > 0
                && last_known_seq - last_indexed_seq > run_full_sync_threshold
            {
                return Ok(SyncStatus::FullSyncRequired(SyncState {
                    last_indexed_key: Some(last_indexed_key.clone()),
                    last_known_key: last_key,
                }));
            }
        }
        Ok(SyncStatus::RegularSyncRequired(SyncState {
            last_indexed_key,
            last_known_key: last_key,
        }))
    }

    pub async fn synchronize_asset_indexes(
        &self,
        asset_type: AssetType,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
    ) -> Result<(), IngesterError> {
        match asset_type {
            AssetType::NonFungible => {
                self.synchronize_nft_asset_indexes(rx, run_full_sync_threshold).await
            },
            AssetType::Fungible => {
                self.synchronize_fungible_asset_indexes(rx, run_full_sync_threshold).await
            },
        }
    }

    pub async fn synchronize_nft_asset_indexes(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
    ) -> Result<(), IngesterError> {
        let asset_type = AssetType::NonFungible;

        let state = self.get_sync_state(run_full_sync_threshold, asset_type).await?;
        match state {
            SyncStatus::FullSyncRequired(state) => {
                tracing::debug!("Should run dump synchronizer as the difference between last indexed and last known sequence is greater than the threshold. Last indexed: {:?}, Last known: {}", state.last_indexed_key.clone().map(|k|k.seq), state.last_known_key.seq);
                self.regular_nft_syncronize(rx, state.last_indexed_key, state.last_known_key).await
            },
            SyncStatus::RegularSyncRequired(state) => {
                tracing::debug!("Regular sync required for nft asset");
                self.regular_nft_syncronize(rx, state.last_indexed_key, state.last_known_key).await
            },
            SyncStatus::NoSyncRequired => {
                tracing::debug!("No sync required for nft asset");
                Ok(())
            },
        }
    }

    pub async fn synchronize_fungible_asset_indexes(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
    ) -> Result<(), IngesterError> {
        let asset_type = AssetType::Fungible;

        let state = self.get_sync_state(run_full_sync_threshold, asset_type).await?;

        match state {
            SyncStatus::FullSyncRequired(state) => {
                tracing::debug!("Should run dump synchronizer as the difference between last indexed and last known sequence is greater than the threshold. Last indexed: {:?}, Last known: {}", state.last_indexed_key.clone().map(|k|k.seq), state.last_known_key.seq);
                self.regular_fungible_syncronize(rx, state.last_indexed_key, state.last_known_key)
                    .await
            },
            SyncStatus::RegularSyncRequired(state) => {
                tracing::debug!("Regular sync required for fungible asset");
                self.regular_fungible_syncronize(rx, state.last_indexed_key, state.last_known_key)
                    .await
            },
            SyncStatus::NoSyncRequired => {
                tracing::debug!("No sync required for fungible asset");
                Ok(())
            },
        }
    }

    pub async fn full_syncronize(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        asset_type: AssetType,
    ) -> Result<(), IngesterError> {
        let last_known_key = match asset_type {
            AssetType::NonFungible => {
                self.rocks_primary_storage.last_known_nft_asset_updated_key()?
            },
            AssetType::Fungible => {
                self.rocks_primary_storage.last_known_fungible_asset_updated_key()?
            },
        };
        let Some(last_known_key) = last_known_key else {
            return Ok(());
        };
        let last_included_rocks_key =
            encode_u64x2_pubkey(last_known_key.seq, last_known_key.slot, last_known_key.pubkey);
        self.dump_sync(last_included_rocks_key.as_slice(), rx, asset_type).await
    }

    async fn dump_sync(
        &self,
        last_included_rocks_key: &[u8],
        rx: &tokio::sync::broadcast::Receiver<()>,
        asset_type: AssetType,
    ) -> Result<(), IngesterError> {
        tracing::info!("Dumping {:?} from the primary storage to {}", asset_type, self.dump_path);

        match asset_type {
            AssetType::NonFungible => {
                self.dump_sync_nft(rx, last_included_rocks_key, NFT_SHARDS).await?;
            },
            AssetType::Fungible => {
                self.dump_sync_fungibles(rx, last_included_rocks_key, FUNGIBLE_SHARDS).await?;
            },
        }

        tracing::info!("{:?} Dump is complete and loaded", asset_type);
        Ok(())
    }

    async fn dump_sync_nft(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        last_included_rocks_key: &[u8],
        num_shards: u64,
    ) -> Result<(), IngesterError> {
        let base_path = std::path::Path::new(self.dump_path.as_str());
        self.pg_index_storage.destructive_prep_to_batch_nft_load().await?;

        let shards = shard_pubkeys(num_shards);
        type ResultWithPaths = Result<(usize, String, String, String, String), String>;
        let mut tasks: JoinSet<ResultWithPaths> = JoinSet::new();
        for (start, end) in shards.iter() {
            let name_postfix =
                if num_shards > 1 { format!("_shard_{}_{}", start, end) } else { "".to_string() };
            let creators_path = base_path
                .join(format!("creators{}.csv", name_postfix))
                .to_str()
                .map(str::to_owned)
                .unwrap();
            let assets_path = base_path
                .join(format!("assets{}.csv", name_postfix))
                .to_str()
                .map(str::to_owned)
                .unwrap();
            let authorities_path = base_path
                .join(format!("assets_authorities{}.csv", name_postfix))
                .to_str()
                .map(str::to_owned)
                .unwrap();
            let metadata_path = base_path
                .join(format!("metadata{}.csv", name_postfix))
                .to_str()
                .map(str::to_owned)
                .unwrap();
            tracing::info!(
                "Dumping to creators: {:?}, assets: {:?}, authorities: {:?}, metadata: {:?}",
                creators_path,
                assets_path,
                authorities_path,
                metadata_path,
            );

            let assets_file = File::create(assets_path.clone())
                .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
            let creators_file = File::create(creators_path.clone())
                .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
            let authority_file = File::create(authorities_path.clone())
                .map_err(|e| format!("Could not create file for authority dump: {}", e))?;
            let metadata_file = File::create(metadata_path.clone())
                .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;

            let start = *start;
            let end = *end;
            let shutdown_rx = rx.resubscribe();
            let metrics = self.metrics.clone();
            let rocks_storage = self.rocks_primary_storage.clone();
            tasks.spawn_blocking(move || {
                let res = rocks_storage.dump_nft_csv(
                    assets_file,
                    creators_file,
                    authority_file,
                    metadata_file,
                    BUF_CAPACITY,
                    None,
                    Some(start),
                    Some(end),
                    &shutdown_rx,
                    metrics,
                )?;
                Ok((
                    res,
                    assets_path.clone(),
                    creators_path.clone(),
                    authorities_path.clone(),
                    metadata_path.clone(),
                ))
            });
        }
        let mut index_tasks = JoinSet::new();
        let semaphore = Arc::new(NFTSemaphores::new());
        while let Some(task) = tasks.join_next().await {
            let (_cnt, assets_path, creators_path, authorities_path, metadata_path) =
                task.map_err(|e| e.to_string())??;
            let index_storage = self.pg_index_storage.clone();
            let semaphore = semaphore.clone();
            index_tasks.spawn(async move {
                index_storage
                    .load_from_dump_nfts(
                        assets_path.as_str(),
                        creators_path.as_str(),
                        authorities_path.as_str(),
                        metadata_path.as_str(),
                        semaphore,
                    )
                    .await
            });
        }
        while let Some(task) = index_tasks.join_next().await {
            task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
        }
        tracing::info!("All NFT assets loads complete. Finalizing the batch load");
        self.pg_index_storage.finalize_batch_nft_load().await?;
        tracing::info!("Batch load finalized for NFTs");
        self.pg_index_storage
            .update_last_synced_key(last_included_rocks_key, AssetType::NonFungible)
            .await?;
        Ok(())
    }

    async fn dump_sync_fungibles(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        last_included_rocks_key: &[u8],
        num_shards: u64,
    ) -> Result<(), IngesterError> {
        let base_path = std::path::Path::new(self.dump_path.as_str());
        self.pg_index_storage.destructive_prep_to_batch_fungible_load().await?;

        let shards = shard_pubkeys(num_shards);
        let mut tasks: JoinSet<Result<(usize, String), String>> = JoinSet::new();
        for (start, end) in shards.iter() {
            let name_postfix =
                if num_shards > 1 { format!("_shard_{}_{}", start, end) } else { "".to_string() };

            let fungible_tokens_path = base_path
                .join(format!("fungible_tokens{}.csv", name_postfix))
                .to_str()
                .map(str::to_owned)
                .unwrap();
            tracing::info!("Dumping to fungible_tokens: {:?}", fungible_tokens_path);

            let fungible_tokens_file = File::create(fungible_tokens_path.clone())
                .map_err(|e| format!("Could not create file for fungible tokens dump: {}", e))?;

            let start = *start;
            let end = *end;
            let shutdown_rx = rx.resubscribe();
            let metrics = self.metrics.clone();
            let rocks_storage = self.rocks_primary_storage.clone();

            tasks.spawn_blocking(move || {
                let res = rocks_storage.dump_fungible_csv(
                    (fungible_tokens_file, fungible_tokens_path.clone()),
                    BUF_CAPACITY,
                    Some(start),
                    Some(end),
                    &shutdown_rx,
                    metrics,
                )?;
                Ok((res, fungible_tokens_path))
            });
        }
        let mut index_tasks = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        while let Some(task) = tasks.join_next().await {
            let (_cnt, fungible_tokens_path) = task.map_err(|e| e.to_string())??;
            let index_storage = self.pg_index_storage.clone();
            let semaphore = semaphore.clone();
            index_tasks.spawn(async move {
                index_storage
                    .load_from_dump_fungibles(fungible_tokens_path.as_str(), semaphore)
                    .await
            });
        }
        while let Some(task) = index_tasks.join_next().await {
            task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
        }
        tracing::info!("All token accounts/fungibles loads complete. Finalizing the batch load");
        self.pg_index_storage.finalize_batch_fungible_load().await?;
        tracing::info!("Batch load finalized for fungibles");
        self.pg_index_storage
            .update_last_synced_key(last_included_rocks_key, AssetType::Fungible)
            .await?;
        Ok(())
    }

    async fn regular_fungible_syncronize(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        last_indexed_key: Option<AssetUpdatedKey>,
        last_key: AssetUpdatedKey,
    ) -> Result<(), IngesterError> {
        let mut starting_key = last_indexed_key;
        let mut processed_keys = HashSet::<Pubkey>::new();
        // Loop until no more new keys are returned
        while rx.is_empty() {
            let mut tasks = JoinSet::new();
            let mut last_included_rocks_key = None;
            let mut end_reached = false;
            for _ in 0..self.parallel_tasks {
                if !rx.is_empty() {
                    break;
                }
                let (updated_keys, last_included_key) =
                    self.rocks_primary_storage.fetch_fungible_asset_updated_keys(
                        starting_key.clone(),
                        Some(last_key.clone()),
                        self.dump_synchronizer_batch_size,
                        Some(processed_keys.clone()),
                    )?;
                if updated_keys.is_empty() || last_included_key.is_none() {
                    end_reached = true;
                    break;
                }
                // add the processed keys to the set
                processed_keys.extend(updated_keys.clone());

                starting_key = last_included_key.clone();
                let last_included_key = last_included_key.unwrap();
                // fetch the asset indexes from the primary storage
                let updated_keys_refs: Vec<Pubkey> = updated_keys.iter().copied().collect();

                // Update the asset indexes in the index storage
                // let last_included_key = AssetsUpdateIdx::encode_key(last_included_key);
                last_included_rocks_key = Some(last_included_key);
                let primary_storage = self.rocks_primary_storage.clone();
                let index_storage = self.pg_index_storage.clone();
                let metrics = self.metrics.clone();
                tasks.spawn(async move {
                    Self::syncronize_fungible_batch(
                        primary_storage.clone(),
                        index_storage.clone(),
                        updated_keys_refs.as_slice(),
                        metrics,
                    )
                    .await
                });
                if updated_keys.len() < self.dump_synchronizer_batch_size {
                    end_reached = true;
                    break;
                }
            }

            while let Some(task) = tasks.join_next().await {
                task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
            }
            if let Some(last_included_rocks_key) = last_included_rocks_key {
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_fungible_slot",
                    last_included_rocks_key.slot as i64,
                );
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_fungible_seq",
                    last_included_rocks_key.seq as i64,
                );

                let last_included_rocks_key = encode_u64x2_pubkey(
                    last_included_rocks_key.seq,
                    last_included_rocks_key.slot,
                    last_included_rocks_key.pubkey,
                );
                self.pg_index_storage
                    .update_last_synced_key(&last_included_rocks_key, AssetType::Fungible)
                    .await?;
            } else {
                break;
            }
            if end_reached {
                break;
            }
        }
        self.metrics.inc_number_of_records_synchronized("synchronization_runs", 1);
        Ok(())
    }

    async fn regular_nft_syncronize(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        last_indexed_key: Option<AssetUpdatedKey>,
        last_key: AssetUpdatedKey,
    ) -> Result<(), IngesterError> {
        let mut starting_key = last_indexed_key;
        let mut processed_keys = HashSet::<Pubkey>::new();
        // Loop until no more new keys are returned
        while rx.is_empty() {
            let mut tasks = JoinSet::new();
            let mut last_included_rocks_key = None;
            let mut end_reached = false;
            for _ in 0..self.parallel_tasks {
                if !rx.is_empty() {
                    break;
                }
                let (updated_keys, last_included_key) =
                    self.rocks_primary_storage.fetch_nft_asset_updated_keys(
                        starting_key.clone(),
                        Some(last_key.clone()),
                        self.dump_synchronizer_batch_size,
                        Some(processed_keys.clone()),
                    )?;
                if updated_keys.is_empty() || last_included_key.is_none() {
                    end_reached = true;
                    break;
                }
                // add the processed keys to the set
                processed_keys.extend(updated_keys.clone());

                starting_key = last_included_key.clone();
                let last_included_key = last_included_key.unwrap();
                // fetch the asset indexes from the primary storage
                let updated_keys_refs: Vec<Pubkey> = updated_keys.iter().copied().collect();

                // Update the asset indexes in the index storage
                // let last_included_key = AssetsUpdateIdx::encode_key(last_included_key);
                last_included_rocks_key = Some(last_included_key);
                let primary_storage = self.rocks_primary_storage.clone();
                let index_storage = self.pg_index_storage.clone();
                let metrics = self.metrics.clone();
                tasks.spawn(async move {
                    Self::syncronize_nft_batch(
                        primary_storage.clone(),
                        index_storage.clone(),
                        updated_keys_refs.as_slice(),
                        metrics,
                    )
                    .await
                });
                if updated_keys.len() < self.dump_synchronizer_batch_size {
                    end_reached = true;
                    break;
                }
            }

            while let Some(task) = tasks.join_next().await {
                task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
            }
            if let Some(last_included_rocks_key) = last_included_rocks_key {
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_nft_slot",
                    last_included_rocks_key.slot as i64,
                );
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_nft_seq",
                    last_included_rocks_key.seq as i64,
                );

                let last_included_rocks_key = encode_u64x2_pubkey(
                    last_included_rocks_key.seq,
                    last_included_rocks_key.slot,
                    last_included_rocks_key.pubkey,
                );
                self.pg_index_storage
                    .update_last_synced_key(&last_included_rocks_key, AssetType::NonFungible)
                    .await?;
            } else {
                break;
            }
            if end_reached {
                break;
            }
        }
        self.metrics.inc_number_of_records_synchronized("synchronization_runs", 1);
        Ok(())
    }

    pub async fn syncronize_nft_batch(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        updated_keys_refs: &[Pubkey],
        metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), IngesterError> {
        let asset_indexes = primary_storage.get_nft_asset_indexes(updated_keys_refs).await?;

        if asset_indexes.is_empty() {
            warn!("No asset indexes found for keys: {:?}", updated_keys_refs);
            return Ok(());
        }

        index_storage.update_nft_asset_indexes_batch(asset_indexes.as_slice()).await?;
        metrics.inc_number_of_records_synchronized(
            "synchronized_records",
            updated_keys_refs.len() as u64,
        );
        Ok(())
    }

    pub async fn syncronize_fungible_batch(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        updated_keys_refs: &[Pubkey],
        metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), IngesterError> {
        let asset_indexes = primary_storage.get_fungible_assets_indexes(updated_keys_refs).await?;

        if asset_indexes.is_empty() {
            warn!("No asset indexes found for keys: {:?}", updated_keys_refs);
            return Ok(());
        }

        index_storage.update_fungible_asset_indexes_batch(asset_indexes.as_slice()).await?;
        metrics.inc_number_of_records_synchronized(
            "synchronized_records",
            updated_keys_refs.len() as u64,
        );
        Ok(())
    }
}

/// Generate the first and last Pubkey for each shard.
/// Returns a vector of tuples (start_pubkey, end_pubkey) for each shard.
pub fn shard_pubkeys(num_shards: u64) -> Vec<(Pubkey, Pubkey)> {
    // Total keyspace as BigUint
    let total_keyspace = BigUint::from_bytes_be([0xffu8; 32].as_slice());
    let shard_size = &total_keyspace / num_shards;

    let mut shards = Vec::new();
    for i in 0..num_shards {
        // Calculate the start of the shard
        let shard_start = &shard_size * i;
        let shard_start_bytes = shard_start.to_bytes_be();

        // Calculate the end of the shard
        let shard_end = if i == num_shards - 1 {
            total_keyspace.clone() // Last shard ends at the max value
        } else {
            &shard_size * (i + 1) - 1u64
        };
        let shard_end_bytes = shard_end.to_bytes_be();

        // Pad the bytes to fit [u8; 32]
        let start_pubkey = pad_to_32_bytes(&shard_start_bytes);
        let end_pubkey = pad_to_32_bytes(&shard_end_bytes);

        shards.push((Pubkey::new_from_array(start_pubkey), Pubkey::new_from_array(end_pubkey)));
    }

    shards
}

/// Pad a byte slice to fit into a [u8; 32] array.
fn pad_to_32_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut array = [0u8; 32];
    let offset = 32 - bytes.len();
    array[offset..].copy_from_slice(bytes); // Copy the bytes into the rightmost part of the array
    array
}

#[cfg(test)]
mod tests {
    use entities::{
        enums::ASSET_TYPES,
        models::{AssetIndex, UrlWithStatus},
    };
    use metrics_utils::{MetricState, MetricsTrait};
    use mockall;
    use postgre_client::storage_traits::MockAssetIndexStorageMock;
    use rocks_db::storage_traits::MockAssetIndexStorage as MockPrimaryStorage;
    use tokio;

    use super::*;

    fn create_test_asset_index(pubkey: &Pubkey) -> AssetIndex {
        AssetIndex {
            pubkey: pubkey.clone(),
            specification_version: entities::enums::SpecificationVersions::V1,
            specification_asset_class: entities::enums::SpecificationAssetClass::Nft,
            royalty_target_type: entities::enums::RoyaltyTargetType::Creators,
            slot_created: 123456,
            owner: Some(pubkey.clone()),
            owner_type: Some(entities::enums::OwnerType::Single),
            delegate: Some(pubkey.clone()),
            authority: Some(pubkey.clone()),
            collection: Some(Pubkey::new_unique()),
            is_collection_verified: Some(true),
            creators: vec![entities::models::Creator {
                creator: Pubkey::new_unique(),
                creator_verified: true,
                creator_share: 100,
            }],
            royalty_amount: 123,
            is_burnt: false,
            is_compressible: false,
            is_compressed: false,
            is_frozen: false,
            supply: Some(1),
            metadata_url: Some(UrlWithStatus {
                metadata_url: "https://www.google.com".to_string(),
                is_downloaded: true,
            }),
            update_authority: None,
            slot_updated: 123456,
            fungible_asset_mint: None,
            fungible_asset_balance: None,
        }
    }

    #[tokio::test]
    async fn test_synchronizer_over_2_empty_storages() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();

        index_storage.expect_fetch_last_synced_id().once().return_once(|_| Ok(None));

        primary_storage
            .mock_update_index_storage
            .expect_last_known_fungible_asset_updated_key()
            .once()
            .return_once(|| Ok(None));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(synchronizer);

        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();

            match asset_type {
                AssetType::Fungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
                AssetType::NonFungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
            }
        }
    }

    #[tokio::test]
    async fn test_synchronizer_with_records_in_primary_storage() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();
        ASSET_TYPES.iter().for_each(|_e| {
            index_storage.expect_fetch_last_synced_id().once().return_once(|_| Ok(None));
        });

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = AssetUpdatedKey::new(100, 2, key.clone());
        let binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());
        // Primary storage has some records
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_nft_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_clone)));

        let updated_keys = HashSet::from([key.clone()]);
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_nft_asset_updated_keys()
            .once()
            .return_once(move |_, _, _, _| Ok((updated_keys.clone(), Some(index_clone))));

        let mut expected_indexes = Vec::<AssetIndex>::new();
        expected_indexes.push(create_test_asset_index(&key));
        let indexes_vec = expected_indexes.clone();
        primary_storage
            .mock_asset_index_reader
            .expect_get_nft_asset_indexes()
            .once()
            .return_once(move |_| Ok(indexes_vec));
        let indexes_vec = expected_indexes.clone();
        index_storage
            .expect_update_nft_asset_indexes_batch()
            .with(mockall::predicate::eq(indexes_vec))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key), mockall::predicate::eq(AssetType::Fungible))
            .once()
            .return_once(|_, _| Ok(()));
        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(synchronizer);
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();

            match asset_type {
                AssetType::Fungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
                AssetType::NonFungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
            }
        }
    }

    #[tokio::test]
    async fn test_synchronizer_with_small_batch_size() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();

        // Index storage starts empty
        ASSET_TYPES.iter().for_each(|_| {
            index_storage.expect_fetch_last_synced_id().once().return_once(|_| Ok(None));
        });

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = AssetUpdatedKey::new(100, 2, key.clone());
        let binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_nft_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_clone)));

        let updated_keys = HashSet::from([key.clone()]);
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_nft_asset_updated_keys()
            .times(2)
            .returning(move |_, _, _, _| {
                static mut CALL_COUNT: usize = 0;
                unsafe {
                    CALL_COUNT += 1;
                    if CALL_COUNT == 1 {
                        Ok((updated_keys.clone(), Some(index_clone.clone())))
                    } else {
                        Ok((HashSet::new(), Some(index_clone.clone())))
                    }
                }
            });

        let mut expected_indexes = Vec::<AssetIndex>::new();
        expected_indexes.push(create_test_asset_index(&key));
        let indexes_vec = expected_indexes.clone();
        primary_storage
            .mock_asset_index_reader
            .expect_get_nft_asset_indexes()
            .once()
            .return_once(move |_| Ok(indexes_vec));

        index_storage
            .expect_update_nft_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key), mockall::predicate::eq(AssetType::Fungible))
            .once()
            .return_once(|_, _| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            1,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        ); // Small batch size
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(synchronizer);
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();

            match asset_type {
                AssetType::Fungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
                AssetType::NonFungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
            }
        }
    }

    #[tokio::test]
    async fn test_synchronizer_with_existing_index_data() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();

        let index_key = AssetUpdatedKey::new(95, 2, Pubkey::new_unique());
        let last_synced_binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());

        ASSET_TYPES.iter().for_each(|_| {
            let last_synced_binary_key = last_synced_binary_key.clone();
            index_storage
                .expect_fetch_last_synced_id()
                .once()
                .return_once(|_| Ok(Some(last_synced_binary_key)));
        });

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key_first_batch = AssetUpdatedKey::new(100, 2, key.clone());
        let index_key_second_batch = AssetUpdatedKey::new(120, 3, key.clone());
        let binary_key_first_batch = encode_u64x2_pubkey(
            index_key_first_batch.seq,
            index_key_first_batch.slot,
            index_key_first_batch.pubkey.clone(),
        );
        let binary_key_second_batch = encode_u64x2_pubkey(
            index_key_second_batch.seq,
            index_key_second_batch.slot,
            index_key_second_batch.pubkey.clone(),
        );
        let index_key_second_batch_clone = index_key_second_batch.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_nft_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key_second_batch_clone)));

        let mut call_count = 0;
        let updated_keys_first_call = HashSet::from([key.clone(), Pubkey::new_unique()]);
        let updated_keys_second_call = HashSet::from([Pubkey::new_unique()]);
        let index_key_second_batch_clone = index_key_second_batch.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_nft_asset_updated_keys()
            .times(2)
            .returning(move |_, _, _, _| {
                call_count += 1;
                if call_count == 1 {
                    Ok((updated_keys_first_call.clone(), Some(index_key_first_batch.clone())))
                } else {
                    Ok((
                        updated_keys_second_call.clone(),
                        Some(index_key_second_batch_clone.clone()),
                    ))
                }
            });

        let mut expected_indexes_first_batch = Vec::<AssetIndex>::new();
        expected_indexes_first_batch.push(create_test_asset_index(&key));
        let indexes_first_batch_vec = expected_indexes_first_batch.clone();
        let indexes_second_batch_vec = expected_indexes_first_batch.clone();
        let expected_indexes_second_batch: Vec<AssetIndex> = expected_indexes_first_batch.clone();
        let mut call_count2 = 0;
        primary_storage.mock_asset_index_reader.expect_get_nft_asset_indexes().times(2).returning(
            move |_| {
                call_count2 += 1;
                if call_count2 == 1 {
                    Ok(indexes_first_batch_vec.clone())
                } else {
                    Ok(indexes_second_batch_vec.clone())
                }
            },
        );

        index_storage
            .expect_update_nft_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes_first_batch.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(
                mockall::predicate::eq(binary_key_first_batch),
                mockall::predicate::eq(AssetType::Fungible),
            )
            .once()
            .return_once(|_, _| Ok(()));

        index_storage
            .expect_update_nft_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes_second_batch.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(
                mockall::predicate::eq(binary_key_second_batch),
                mockall::predicate::eq(AssetType::Fungible),
            )
            .once()
            .return_once(|_, _| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            2,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(synchronizer);
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();

            match asset_type {
                AssetType::Fungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
                AssetType::NonFungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
            }
        }
    }

    #[tokio::test]
    async fn test_synchronizer_with_synced_databases() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();

        let key = Pubkey::new_unique();
        let index_key = AssetUpdatedKey::new(100, 2, key);
        let index_key_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_nft_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key_clone)));

        ASSET_TYPES.iter().for_each(|_| {
            let index_key_clone = index_key.clone();
            index_storage.expect_fetch_last_synced_id().once().return_once(move |_| {
                Ok(Some(encode_u64x2_pubkey(
                    index_key_clone.seq,
                    index_key_clone.slot,
                    index_key_clone.pubkey.clone(),
                )))
            });
        });

        // Expect no calls to fetch_asset_updated_keys since databases are synced
        primary_storage.mock_update_index_storage.expect_fetch_nft_asset_updated_keys().never();

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(synchronizer);
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();

            match asset_type {
                AssetType::Fungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
                AssetType::NonFungible => {
                    tokio::spawn(async move {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap();
                    });
                },
            }
        }
    }

    #[test]
    fn test_shard_pubkeys_1() {
        let shards = shard_pubkeys(1);
        assert_eq!(shards.len(), 1);
        assert_eq!(
            shards[0],
            (Pubkey::new_from_array([0; 32]), Pubkey::new_from_array([0xff; 32]))
        );
    }

    #[test]
    fn test_shard_pubkeys_2() {
        let shards = shard_pubkeys(2);
        assert_eq!(shards.len(), 2);
        let first_key = [0x0; 32];
        let mut last_key = [0xff; 32];
        last_key[0] = 0x7f;
        last_key[31] = 0xfe;
        assert_eq!(shards[0].0.to_bytes(), first_key);
        assert_eq!(shards[0].1.to_bytes(), last_key);

        let mut first_key = last_key;
        first_key[31] = 0xff;
        let last_key = [0xff; 32];
        assert_eq!(shards[1].0.to_bytes(), first_key);
        assert_eq!(shards[1].1.to_bytes(), last_key);
    }
}
