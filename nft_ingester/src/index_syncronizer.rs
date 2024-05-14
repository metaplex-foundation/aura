use entities::models::AssetIndex;
use log::warn;
use metrics_utils::SynchronizerMetricsConfig;
use postgre_client::storage_traits::{AssetIndexStorage, TempClientProvider};
use rocks_db::{
    key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey},
    storage_traits::{AssetIndexStorage as AssetIndexSourceStorage, AssetUpdatedKey},
};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::Arc};
use tokio::task::JoinSet;

use crate::error::IngesterError;

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

pub struct Synchronizer<T, U, P>
where
    T: AssetIndexSourceStorage,
    U: AssetIndexStorage,
    P: TempClientProvider + Send + Sync + 'static + Clone,
{
    primary_storage: Arc<T>,
    index_storage: Arc<U>,
    temp_client_provider: P,
    dump_synchronizer_batch_size: usize,
    dump_path: String,
    metrics: Arc<SynchronizerMetricsConfig>,
    parallel_tasks: usize,
    run_temp_sync_during_dump: bool,
}

impl<T, U, P> Synchronizer<T, U, P>
where
    T: AssetIndexSourceStorage + Send + Sync + 'static,
    U: AssetIndexStorage + Clone + Send + Sync + 'static,
    P: TempClientProvider + Send + Sync + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        temp_client_provider: P,
        dump_synchronizer_batch_size: usize,
        dump_path: String,
        metrics: Arc<SynchronizerMetricsConfig>,
        parallel_tasks: usize,
        run_temp_sync_during_dump: bool,
    ) -> Self {
        Synchronizer {
            primary_storage,
            index_storage,
            temp_client_provider,
            dump_synchronizer_batch_size,
            dump_path,
            metrics,
            parallel_tasks,
            run_temp_sync_during_dump,
        }
    }

    pub async fn maybe_run_full_sync(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
    ) {
        if let Ok(SyncStatus::FullSyncRequired(_)) =
            self.get_sync_state(run_full_sync_threshold).await
        {
            let res = self.full_syncronize(rx).await;
            match res {
                Ok(_) => {
                    tracing::info!("Full synchronization finished successfully");
                }
                Err(e) => {
                    tracing::error!("Full synchronization failed: {:?}", e);
                }
            }
        }
    }

    pub async fn run(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
        timeout_duration: tokio::time::Duration,
    ) {
        while rx.is_empty() {
            let res = self
                .synchronize_asset_indexes(rx, run_full_sync_threshold)
                .await;
            match res {
                Ok(_) => {
                    tracing::info!("Synchronization finished successfully");
                }
                Err(e) => {
                    tracing::error!("Synchronization failed: {:?}", e);
                }
            }
            if rx.is_empty() {
                tokio::time::sleep(timeout_duration).await;
            }
        }
    }

    async fn get_sync_state(
        &self,
        run_full_sync_threshold: i64,
    ) -> Result<SyncStatus, IngesterError> {
        let last_indexed_key = self.index_storage.fetch_last_synced_id().await?;
        let last_indexed_key = match last_indexed_key {
            Some(bytes) => {
                let decoded_key = decode_u64x2_pubkey(bytes)?;
                Some(decoded_key)
            }
            None => None,
        };

        // Fetch the last known key from the primary storage
        let Some(last_key) = self.primary_storage.last_known_asset_updated_key()? else {
            return Ok(SyncStatus::NoSyncRequired);
        };
        if last_indexed_key.is_none() {
            return Ok(SyncStatus::FullSyncRequired(SyncState {
                last_indexed_key: None,
                last_known_key: last_key,
            }));
        }
        let last_known_seq = last_key.seq as i64;
        self.metrics
            .set_last_synchronized_slot("last_known_updated_seq", last_known_seq);
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
        rx: &tokio::sync::broadcast::Receiver<()>,
        run_full_sync_threshold: i64,
    ) -> Result<(), IngesterError> {
        let state = self.get_sync_state(run_full_sync_threshold).await?;
        match state {
            SyncStatus::FullSyncRequired(state) => {
                tracing::info!("Should run dump synchronizer as the difference between last indexed and last known sequence is greater than the threshold. Last indexed: {:?}, Last known: {}", state.last_indexed_key.clone().map(|k|k.seq), state.last_known_key.seq);
                self.regular_syncronize(rx, state.last_indexed_key, state.last_known_key)
                    .await
            }
            SyncStatus::RegularSyncRequired(state) => {
                self.regular_syncronize(rx, state.last_indexed_key, state.last_known_key)
                    .await
            }
            SyncStatus::NoSyncRequired => Ok(()),
        }
    }

    pub async fn full_syncronize(
        &self,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), IngesterError> {
        let Some(last_known_key) = self.primary_storage.last_known_asset_updated_key()? else {
            return Ok(());
        };
        let last_included_rocks_key = encode_u64x2_pubkey(
            last_known_key.seq,
            last_known_key.slot,
            last_known_key.pubkey,
        );
        if !self.run_temp_sync_during_dump {
            return self.dump_sync(last_included_rocks_key.as_slice(), rx).await;
        }
        // start a regular synchronization into a temporary storage to catch up on it while the dump is being created and loaded, as it takes a loooong time
        let (tx, local_rx) = tokio::sync::broadcast::channel::<()>(1);
        let temp_storage = Arc::new(self.temp_client_provider.create_temp_client().await?);
        temp_storage
            .initialize(last_included_rocks_key.as_slice())
            .await?;
        let temp_syncronizer = Arc::new(Synchronizer::new(
            self.primary_storage.clone(),
            temp_storage.clone(),
            self.temp_client_provider.clone(),
            self.dump_synchronizer_batch_size,
            "not used".to_string(),
            self.metrics.clone(),
            1,
            false,
        ));
        let task = tokio::spawn(async move {
            temp_syncronizer
                .run(&local_rx, -1, tokio::time::Duration::from_millis(100))
                .await;
        });

        self.dump_sync(last_included_rocks_key.as_slice(), rx)
            .await?;

        tx.send(()).map_err(|e| e.to_string())?;
        task.await.map_err(|e| e.to_string())?;
        // now we can copy temp storage to the main storage
        temp_storage.copy_to_main().await?;
        Ok(())
    }

    async fn dump_sync(
        &self,
        last_included_rocks_key: &[u8],
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), IngesterError> {
        let path = std::path::Path::new(self.dump_path.as_str());
        tracing::info!("Dumping the primary storage to {}", self.dump_path);
        self.primary_storage
            .dump_db(path, self.dump_synchronizer_batch_size, rx)
            .await?;
        tracing::info!("Dump is complete. Loading the dump into the index storage");

        self.index_storage
            .load_from_dump(path, last_included_rocks_key)
            .await?;
        tracing::info!("Dump is loaded into the index storage");
        Ok(())
    }

    async fn regular_syncronize(
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
                    self.primary_storage.fetch_asset_updated_keys(
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
                let primary_storage = self.primary_storage.clone();
                let index_storage = self.index_storage.clone();
                let metrics = self.metrics.clone();
                tasks.spawn(tokio::spawn(async move {
                    Self::syncronize_batch(
                        primary_storage.clone(),
                        index_storage.clone(),
                        updated_keys_refs.as_slice(),
                        metrics,
                    )
                    .await
                }));
                if updated_keys.len() < self.dump_synchronizer_batch_size {
                    end_reached = true;
                    break;
                }
            }

            while let Some(task) = tasks.join_next().await {
                task.map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())??;
            }
            if let Some(last_included_rocks_key) = last_included_rocks_key {
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_slot",
                    last_included_rocks_key.slot as i64,
                );
                self.metrics.set_last_synchronized_slot(
                    "last_synchronized_seq",
                    last_included_rocks_key.seq as i64,
                );

                let last_included_rocks_key = encode_u64x2_pubkey(
                    last_included_rocks_key.seq,
                    last_included_rocks_key.slot,
                    last_included_rocks_key.pubkey,
                );
                self.index_storage
                    .update_last_synced_key(&last_included_rocks_key)
                    .await?;
            } else {
                break;
            }
            if end_reached {
                break;
            }
        }
        self.metrics
            .inc_number_of_records_synchronized("synchronization_runs", 1);
        Ok(())
    }

    pub async fn syncronize_batch(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        updated_keys_refs: &[Pubkey],
        metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), IngesterError> {
        let asset_indexes = primary_storage.get_asset_indexes(updated_keys_refs).await?;

        if asset_indexes.is_empty() {
            warn!("No asset indexes found for keys: {:?}", updated_keys_refs);
            return Ok(());
        }

        index_storage
            .update_asset_indexes_batch(
                asset_indexes
                    .values()
                    .cloned()
                    .collect::<Vec<AssetIndex>>()
                    .as_slice(),
            )
            .await?;
        metrics.inc_number_of_records_synchronized(
            "synchronized_records",
            updated_keys_refs.len() as u64,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use entities::models::{AssetIndex, UrlWithStatus};
    use metrics_utils::{MetricState, MetricsTrait};
    use mockall;
    use postgre_client::storage_traits::{MockAssetIndexStorageMock, MockTempClientProviderMock};
    use rocks_db::storage_traits::MockAssetIndexStorage as MockPrimaryStorage;
    use std::collections::HashMap;
    use tokio;

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
        }
    }

    #[tokio::test]
    async fn test_synchronizer_over_2_empty_storages() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        let temp_client_provider = MockTempClientProviderMock::new();
        metrics_state.register_metrics();

        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(None));
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(|| Ok(None));
        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            temp_client_provider,
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
            false,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        synchronizer
            .synchronize_asset_indexes(&rx, 0)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_records_in_primary_storage() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        let temp_client_provider = MockTempClientProviderMock::new();
        metrics_state.register_metrics();

        // Index storage starts empty
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(None));

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = AssetUpdatedKey::new(100, 2, key.clone());
        let binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());
        // Primary storage has some records
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_clone)));

        let updated_keys = HashSet::from([key.clone()]);
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
            .once()
            .return_once(move |_, _, _, _| Ok((updated_keys.clone(), Some(index_clone))));

        let mut map_of_asset_indexes = HashMap::<Pubkey, AssetIndex>::new();
        map_of_asset_indexes.insert(key.clone(), create_test_asset_index(&key));
        let expected_indexes: Vec<AssetIndex> = map_of_asset_indexes.values().cloned().collect();
        primary_storage
            .mock_asset_index_reader
            .expect_get_asset_indexes()
            .once()
            .return_once(move |_| Ok(map_of_asset_indexes));

        index_storage
            .expect_update_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key))
            .once()
            .return_once(|_| Ok(()));
        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            temp_client_provider,
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
            false,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        synchronizer
            .synchronize_asset_indexes(&rx, 0)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_small_batch_size() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        let temp_client_provider = MockTempClientProviderMock::new();
        metrics_state.register_metrics();

        // Index storage starts empty
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(None));

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = AssetUpdatedKey::new(100, 2, key.clone());
        let binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_clone)));

        let updated_keys = HashSet::from([key.clone()]);
        let index_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
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

        let mut map_of_asset_indexes = HashMap::<Pubkey, AssetIndex>::new();
        map_of_asset_indexes.insert(key.clone(), create_test_asset_index(&key));
        let expected_indexes: Vec<AssetIndex> = map_of_asset_indexes.values().cloned().collect();
        primary_storage
            .mock_asset_index_reader
            .expect_get_asset_indexes()
            .once()
            .return_once(move |_| Ok(map_of_asset_indexes));

        index_storage
            .expect_update_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key))
            .once()
            .return_once(|_| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            temp_client_provider,
            1,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
            false,
        ); // Small batch size
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        synchronizer
            .synchronize_asset_indexes(&rx, 0)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_existing_index_data() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        let temp_client_provider = MockTempClientProviderMock::new();
        metrics_state.register_metrics();

        let index_key = AssetUpdatedKey::new(95, 2, Pubkey::new_unique());
        let last_synced_binary_key =
            encode_u64x2_pubkey(index_key.seq, index_key.slot, index_key.pubkey.clone());

        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(Some(last_synced_binary_key)));

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
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key_second_batch_clone)));

        let mut call_count = 0;
        let updated_keys_first_call = HashSet::from([key.clone(), Pubkey::new_unique()]);
        let updated_keys_second_call = HashSet::from([Pubkey::new_unique()]);
        let index_key_second_batch_clone = index_key_second_batch.clone();
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
            .times(2)
            .returning(move |_, _, _, _| {
                call_count += 1;
                if call_count == 1 {
                    Ok((
                        updated_keys_first_call.clone(),
                        Some(index_key_first_batch.clone()),
                    ))
                } else {
                    Ok((
                        updated_keys_second_call.clone(),
                        Some(index_key_second_batch_clone.clone()),
                    ))
                }
            });

        let mut map_of_asset_indexes = HashMap::<Pubkey, AssetIndex>::new();
        map_of_asset_indexes.insert(key.clone(), create_test_asset_index(&key));
        let expected_indexes_first_batch: Vec<AssetIndex> =
            map_of_asset_indexes.values().cloned().collect();

        let expected_indexes_second_batch: Vec<AssetIndex> =
            map_of_asset_indexes.values().cloned().collect();
        let second_call_map = map_of_asset_indexes.clone();
        let mut call_count2 = 0;
        primary_storage
            .mock_asset_index_reader
            .expect_get_asset_indexes()
            .times(2)
            .returning(move |_| {
                call_count2 += 1;
                if call_count2 == 1 {
                    Ok(map_of_asset_indexes.clone())
                } else {
                    Ok(second_call_map.clone())
                }
            });

        index_storage
            .expect_update_asset_indexes_batch()
            .with(mockall::predicate::eq(expected_indexes_first_batch.clone()))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key_first_batch))
            .once()
            .return_once(|_| Ok(()));

        index_storage
            .expect_update_asset_indexes_batch()
            .with(mockall::predicate::eq(
                expected_indexes_second_batch.clone(),
            ))
            .once()
            .return_once(|_| Ok(()));
        index_storage
            .expect_update_last_synced_key()
            .with(mockall::predicate::eq(binary_key_second_batch))
            .once()
            .return_once(|_| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            temp_client_provider,
            2,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
            false,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        synchronizer
            .synchronize_asset_indexes(&rx, 0)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_synced_databases() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockAssetIndexStorageMock::new();
        let mut metrics_state = MetricState::new();
        let temp_client_provider = MockTempClientProviderMock::new();
        metrics_state.register_metrics();

        let key = Pubkey::new_unique();
        let index_key = AssetUpdatedKey::new(100, 2, key);
        let index_key_clone = index_key.clone();
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key_clone)));
        let index_key_clone = index_key.clone();
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(move || {
                Ok(Some(encode_u64x2_pubkey(
                    index_key_clone.seq,
                    index_key_clone.slot,
                    index_key_clone.pubkey.clone(),
                )))
            });

        // Expect no calls to fetch_asset_updated_keys since databases are synced
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
            .never();

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            temp_client_provider,
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
            false,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        synchronizer
            .synchronize_asset_indexes(&rx, 0)
            .await
            .unwrap();
    }
}
