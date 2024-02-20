use entities::models::AssetIndex;
use log::warn;
use metrics_utils::SynchronizerMetricsConfig;
use postgre_client::storage_traits::AssetIndexStorage;
use rocks_db::{
    key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey},
    storage_traits::AssetIndexStorage as AssetIndexSourceStorage,
};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashSet, sync::Arc};

use crate::error::IngesterError;

pub struct Synchronizer<T, U>
where
    T: AssetIndexSourceStorage,
    U: AssetIndexStorage,
{
    primary_storage: Arc<T>,
    index_storage: Arc<U>,
    batch_size: usize,
    dump_synchronizer_batch_size: usize,
    dump_path: String,
    metrics: Arc<SynchronizerMetricsConfig>,
}

impl<T, U> Synchronizer<T, U>
where
    T: AssetIndexSourceStorage,
    U: AssetIndexStorage,
{
    pub fn new(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        batch_size: usize,
        dump_synchronizer_batch_size: usize,
        dump_path: String,
        metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Self {
        Synchronizer {
            primary_storage,
            index_storage,
            batch_size,
            dump_synchronizer_batch_size,
            dump_path,
            metrics,
        }
    }

    pub async fn synchronize_asset_indexes(
        &self,
        keep_running: Arc<AtomicBool>,
    ) -> Result<(), IngesterError> {
        // Retrieve the last synced ID from the secondary storage
        let last_indexed_key = self.index_storage.fetch_last_synced_id().await?;
        let last_indexed_key = match last_indexed_key {
            Some(bytes) => {
                // let decoded_key = AssetsUpdateIdx::decode_key(bytes)?;
                let decoded_key = decode_u64x2_pubkey(bytes).unwrap();
                Some(decoded_key)
            }
            None => None,
        };

        // Fetch the last known key from the primary storage
        let last_key = self.primary_storage.last_known_asset_updated_key()?;
        if last_key.is_none() {
            return Ok(());
        }
        if last_indexed_key.is_some() && last_indexed_key.unwrap() >= last_key.unwrap() {
            return Ok(());
        }
        self.metrics
            .set_last_synchronized_slot("last_known_updated_seq", last_key.unwrap().0 as i64);
        self.regular_syncronize(keep_running, last_indexed_key, last_key)
            .await
    }

    pub async fn full_syncronize(
        &self,
        rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), IngesterError> {
        let last_key = self.primary_storage.last_known_asset_updated_key()?;
        if last_key.is_none() {
            return Ok(());
        }
        let metadata_keys = self.index_storage.get_existing_metadata_keys().await?;
        let path = std::path::Path::new(self.dump_path.as_str());
        self.primary_storage
            .dump_db(path, metadata_keys, self.dump_synchronizer_batch_size, rx)
            .await?;
        let last_included_rocks_key = last_key
            .map(|(seq, slot, pubkey)| encode_u64x2_pubkey(seq, slot, pubkey))
            .unwrap();
        self.index_storage
            .load_from_dump(path, last_included_rocks_key.as_slice())
            .await?;
        Ok(())
    }

    async fn regular_syncronize(
        &self,
        keep_running: Arc<AtomicBool>,
        last_indexed_key: Option<(u64, u64, Pubkey)>,
        last_key: Option<(u64, u64, Pubkey)>,
    ) -> Result<(), IngesterError> {
        let mut starting_key = last_indexed_key;
        let mut processed_keys = HashSet::<Pubkey>::new();
        // Loop until no more new keys are returned
        while keep_running.load(Ordering::SeqCst) {
            let (updated_keys, last_included_key) = self.primary_storage.fetch_asset_updated_keys(
                starting_key,
                last_key,
                self.batch_size,
                Some(processed_keys.clone()),
            )?;
            if updated_keys.is_empty() || last_included_key.is_none() {
                break;
            }

            starting_key = last_included_key;
            let last_included_key = last_included_key.unwrap();
            // fetch the asset indexes from the primary storage
            let updated_keys_refs: Vec<Pubkey> = updated_keys.iter().copied().collect();

            // Update the asset indexes in the index storage
            // let last_included_key = AssetsUpdateIdx::encode_key(last_included_key);
            let last_included_rocks_key = encode_u64x2_pubkey(
                last_included_key.0,
                last_included_key.1,
                last_included_key.2,
            );
            Self::syncronize_batch(
                self.primary_storage.clone(),
                self.index_storage.clone(),
                updated_keys_refs.as_slice(),
                last_included_rocks_key,
            )
            .await?;

            self.metrics
                .set_last_synchronized_slot("last_synchronized_slot", last_included_key.1 as i64);
            self.metrics
                .set_last_synchronized_slot("last_synchronized_seq", last_included_key.0 as i64);

            self.metrics.inc_number_of_records_synchronized(
                "synchronized_records",
                updated_keys_refs.len() as u64,
            );

            if updated_keys.len() < self.batch_size {
                break;
            }
            // add the processed keys to the set
            processed_keys.extend(updated_keys);
        }
        self.metrics
            .inc_number_of_records_synchronized("synchronization_runs", 1);
        Ok(())
    }

    pub async fn syncronize_batch(
        primary_storage: Arc<T>,
        index_storage: Arc<U>,
        updated_keys_refs: &[Pubkey],
        last_included_rocks_key: Vec<u8>,
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
                last_included_rocks_key.as_slice(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use entities::models::{AssetIndex, UrlWithStatus};
    use metrics_utils::{
        red::RequestErrorDurationMetrics, ApiMetricsConfig, BackfillerMetricsConfig,
        IngesterMetricsConfig, JsonDownloaderMetricsConfig, JsonMigratorMetricsConfig, MetricState,
        MetricsTrait, RpcBackfillerMetricsConfig, SequenceConsistentGapfillMetricsConfig,
        SynchronizerMetricsConfig,
    };
    use mockall;
    use postgre_client::storage_traits::MockAssetIndexStorage as MockIndexStorage;
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
            slot_updated: 123456,
        }
    }

    #[tokio::test]
    async fn test_synchronizer_over_2_empty_storages() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockIndexStorage::new();
        let mut metrics_state = MetricState::new(
            IngesterMetricsConfig::new(),
            ApiMetricsConfig::new(),
            JsonDownloaderMetricsConfig::new(),
            BackfillerMetricsConfig::new(),
            RpcBackfillerMetricsConfig::new(),
            SynchronizerMetricsConfig::new(),
            JsonMigratorMetricsConfig::new(),
            SequenceConsistentGapfillMetricsConfig::new(),
            RequestErrorDurationMetrics::new(),
        );
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
            1000,
            200_000,
            "".to_string(),
            // 1000,
            metrics_state.synchronizer_metrics.clone(),
        );
        let keep_running = Arc::new(AtomicBool::new(true));
        synchronizer
            .synchronize_asset_indexes(keep_running)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_records_in_primary_storage() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockIndexStorage::new();
        let mut metrics_state = MetricState::new(
            IngesterMetricsConfig::new(),
            ApiMetricsConfig::new(),
            JsonDownloaderMetricsConfig::new(),
            BackfillerMetricsConfig::new(),
            RpcBackfillerMetricsConfig::new(),
            SynchronizerMetricsConfig::new(),
            JsonMigratorMetricsConfig::new(),
            SequenceConsistentGapfillMetricsConfig::new(),
            RequestErrorDurationMetrics::new(),
        );
        metrics_state.register_metrics();

        // Index storage starts empty
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(None));

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = (100, 2, key.clone());
        let binary_key = encode_u64x2_pubkey(index_key.0, index_key.1, index_key.2.clone());
        // Primary storage has some records
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key.clone())));

        let updated_keys = HashSet::from([key.clone()]);
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
            .once()
            .return_once(move |_, _, _, _| Ok((updated_keys.clone(), Some(index_key.clone()))));

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
            .with(
                mockall::predicate::eq(expected_indexes.clone()),
                mockall::predicate::eq(binary_key),
            )
            .once()
            .return_once(|_, _| Ok(()));
        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            1000,
            200_000,
            "".to_string(),
            // 1000,
            metrics_state.synchronizer_metrics.clone(),
        );
        let keep_running = Arc::new(AtomicBool::new(true));
        synchronizer
            .synchronize_asset_indexes(keep_running)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_small_batch_size() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockIndexStorage::new();
        let mut metrics_state = MetricState::new(
            IngesterMetricsConfig::new(),
            ApiMetricsConfig::new(),
            JsonDownloaderMetricsConfig::new(),
            BackfillerMetricsConfig::new(),
            RpcBackfillerMetricsConfig::new(),
            SynchronizerMetricsConfig::new(),
            JsonMigratorMetricsConfig::new(),
            SequenceConsistentGapfillMetricsConfig::new(),
            RequestErrorDurationMetrics::new(),
        );
        metrics_state.register_metrics();

        // Index storage starts empty
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(None));

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key = (100, 2, key.clone());
        let binary_key = encode_u64x2_pubkey(index_key.0, index_key.1, index_key.2.clone());
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key.clone())));

        let updated_keys = HashSet::from([key.clone()]);
        primary_storage
            .mock_update_index_storage
            .expect_fetch_asset_updated_keys()
            .times(2)
            .returning(move |_, _, _, _| {
                static mut CALL_COUNT: usize = 0;
                unsafe {
                    CALL_COUNT += 1;
                    if CALL_COUNT == 1 {
                        Ok((updated_keys.clone(), Some(index_key.clone())))
                    } else {
                        Ok((HashSet::new(), Some(index_key.clone())))
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
            .with(
                mockall::predicate::eq(expected_indexes.clone()),
                mockall::predicate::eq(binary_key),
            )
            .once()
            .return_once(|_, _| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            1,
            200_000,
            "".to_string(),
            // 1000,
            metrics_state.synchronizer_metrics.clone(),
        ); // Small batch size
        let keep_running = Arc::new(AtomicBool::new(true));
        synchronizer
            .synchronize_asset_indexes(keep_running)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_existing_index_data() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockIndexStorage::new();
        let mut metrics_state = MetricState::new(
            IngesterMetricsConfig::new(),
            ApiMetricsConfig::new(),
            JsonDownloaderMetricsConfig::new(),
            BackfillerMetricsConfig::new(),
            RpcBackfillerMetricsConfig::new(),
            SynchronizerMetricsConfig::new(),
            JsonMigratorMetricsConfig::new(),
            SequenceConsistentGapfillMetricsConfig::new(),
            RequestErrorDurationMetrics::new(),
        );
        metrics_state.register_metrics();

        let index_key = (95, 2, Pubkey::new_unique());
        let last_synced_binary_key =
            encode_u64x2_pubkey(index_key.0, index_key.1, index_key.2.clone());

        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(|| Ok(Some(last_synced_binary_key)));

        let key = Pubkey::new_from_array([1u8; 32]);
        let index_key_first_batch = (100, 2, key.clone());
        let index_key_second_batch = (120, 3, key.clone());
        let binary_key_first_batch = encode_u64x2_pubkey(
            index_key_first_batch.0,
            index_key_first_batch.1,
            index_key_first_batch.2.clone(),
        );
        let binary_key_second_batch = encode_u64x2_pubkey(
            index_key_second_batch.0,
            index_key_second_batch.1,
            index_key_second_batch.2.clone(),
        );
        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key_second_batch.clone())));

        let mut call_count = 0;
        let updated_keys_first_call = HashSet::from([key.clone(), Pubkey::new_unique()]);
        let updated_keys_second_call = HashSet::from([Pubkey::new_unique()]);
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
                        Some(index_key_second_batch.clone()),
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
            .with(
                mockall::predicate::eq(expected_indexes_first_batch.clone()),
                mockall::predicate::eq(binary_key_first_batch),
            )
            .once()
            .return_once(|_, _| Ok(()));
        index_storage
            .expect_update_asset_indexes_batch()
            .with(
                mockall::predicate::eq(expected_indexes_second_batch.clone()),
                mockall::predicate::eq(binary_key_second_batch),
            )
            .once()
            .return_once(|_, _| Ok(()));

        let synchronizer = Synchronizer::new(
            Arc::new(primary_storage),
            Arc::new(index_storage),
            2,
            200_000,
            "".to_string(),
            // 1000,
            metrics_state.synchronizer_metrics.clone(),
        );
        let keep_running = Arc::new(AtomicBool::new(true));
        synchronizer
            .synchronize_asset_indexes(keep_running)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_with_synced_databases() {
        let mut primary_storage = MockPrimaryStorage::new();
        let mut index_storage = MockIndexStorage::new();
        let mut metrics_state = MetricState::new(
            IngesterMetricsConfig::new(),
            ApiMetricsConfig::new(),
            JsonDownloaderMetricsConfig::new(),
            BackfillerMetricsConfig::new(),
            RpcBackfillerMetricsConfig::new(),
            SynchronizerMetricsConfig::new(),
            JsonMigratorMetricsConfig::new(),
            SequenceConsistentGapfillMetricsConfig::new(),
            RequestErrorDurationMetrics::new(),
        );
        metrics_state.register_metrics();

        let key = Pubkey::new_unique();
        let index_key = (100, 2, key);

        primary_storage
            .mock_update_index_storage
            .expect_last_known_asset_updated_key()
            .once()
            .return_once(move || Ok(Some(index_key)));
        index_storage
            .expect_fetch_last_synced_id()
            .once()
            .return_once(move || {
                Ok(Some(encode_u64x2_pubkey(
                    index_key.0,
                    index_key.1,
                    index_key.2.clone(),
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
            1000,
            200_000,
            "".to_string(),
            // 1000,
            metrics_state.synchronizer_metrics.clone(),
        );
        let keep_running = Arc::new(AtomicBool::new(true));
        synchronizer
            .synchronize_asset_indexes(keep_running)
            .await
            .unwrap();
    }
}
