#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use interface::slot_getter::MockFinalizedSlotGetter;
    use metrics_utils::red::RequestErrorDurationMetrics;
    use metrics_utils::utils::start_metrics;
    use metrics_utils::{
        ApiMetricsConfig, BackfillerMetricsConfig, IngesterMetricsConfig,
        JsonDownloaderMetricsConfig, JsonMigratorMetricsConfig, MetricState, MetricsTrait,
        RpcBackfillerMetricsConfig, SequenceConsistentGapfillMetricsConfig,
        SynchronizerMetricsConfig,
    };
    use nft_ingester::sequence_consistent::SequenceConsistentGapfiller;
    use rocks_db::bubblegum_slots::bubblegum_slots_key_to_value;
    use rocks_db::key_encoders::{decode_pubkey, decode_pubkey_u64, decode_string};
    use rocks_db::tree_seq::TreeSeqIdx;
    use setup::rocks::RocksTestEnvironment;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use usecase::slots_collector::{MockRowKeysGetter, SlotsCollector};

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_range_delete() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let first_tree_key = solana_program::pubkey::Pubkey::from_str(
            "5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61",
        )
        .unwrap();
        let second_tree_key = solana_program::pubkey::Pubkey::from_str(
            "94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe",
        )
        .unwrap();

        storage
            .tree_seq_idx
            .put_async((first_tree_key, 100), TreeSeqIdx { slot: 200 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 101), TreeSeqIdx { slot: 201 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 102), TreeSeqIdx { slot: 202 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((second_tree_key, 103), TreeSeqIdx { slot: 203 })
            .await
            .unwrap();

        storage
            .tree_seq_idx
            .delete_range((first_tree_key, 100), (first_tree_key, 102))
            .await
            .unwrap();

        let mut tree_iterator = storage.tree_seq_idx.iter_start();
        let (key, _) = tree_iterator.next().unwrap().unwrap();
        let key = decode_pubkey_u64(key.to_vec()).unwrap();
        assert_eq!(key, (first_tree_key, 102));
        let (key, _) = tree_iterator.next().unwrap().unwrap();
        let key = decode_pubkey_u64(key.to_vec()).unwrap();
        assert_eq!(key, (second_tree_key, 103));
        assert_eq!(tree_iterator.next(), None);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_find_gap() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let first_tree_key = solana_program::pubkey::Pubkey::from_str(
            "5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61",
        )
        .unwrap();
        let second_tree_key = solana_program::pubkey::Pubkey::from_str(
            "94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe",
        )
        .unwrap();

        storage
            .tree_seq_idx
            .put_async((first_tree_key, 100), TreeSeqIdx { slot: 200 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 101), TreeSeqIdx { slot: 201 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 102), TreeSeqIdx { slot: 202 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 103), TreeSeqIdx { slot: 203 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 107), TreeSeqIdx { slot: 207 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 108), TreeSeqIdx { slot: 208 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 111), TreeSeqIdx { slot: 211 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((first_tree_key, 112), TreeSeqIdx { slot: 212 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((second_tree_key, 10), TreeSeqIdx { slot: 20 })
            .await
            .unwrap();
        storage
            .tree_seq_idx
            .put_async((second_tree_key, 11), TreeSeqIdx { slot: 21 })
            .await
            .unwrap();

        let mut row_keys_getter = MockRowKeysGetter::new();
        row_keys_getter
            .expect_get_row_keys()
            .times(1)
            .return_once(move |_, _, _, _| {
                Ok(vec![
                    format!("{}/{:016x}", first_tree_key, !206u64),
                    format!("{}/{:016x}", first_tree_key, !204u64),
                    format!("{}/{:016x}", first_tree_key, !203u64),
                ])
            });
        row_keys_getter
            .expect_get_row_keys()
            .times(1)
            .return_once(move |_, _, _, _| {
                Ok(vec![
                    format!("{}/{:016x}", first_tree_key, !209u64),
                    format!("{}/{:016x}", first_tree_key, !208u64),
                ])
            });
        let row_keys_getter_arc = Arc::new(row_keys_getter);
        let mut metrics_state = MetricState::new();
        metrics_state.register_metrics();
        start_metrics(metrics_state.registry, Some(4444)).await;

        let slots_collector = SlotsCollector::new(
            storage.clone(),
            row_keys_getter_arc.clone(),
            metrics_state.backfiller_metrics.clone(),
        );

        let mut finalized_slot_getter = MockFinalizedSlotGetter::new();

        finalized_slot_getter
            .expect_get_finalized_slot()
            .times(1)
            .return_once(move || Ok(212));

        let arc_finalized_slot_getter = Arc::new(finalized_slot_getter);

        let sequence_consistent_gapfiller = SequenceConsistentGapfiller::new(
            storage.clone(),
            slots_collector,
            metrics_state.sequence_consistent_gapfill_metrics.clone(),
            arc_finalized_slot_getter.clone(),
        );
        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
        sequence_consistent_gapfiller
            .collect_sequences_gaps(shutdown_rx.resubscribe())
            .await;

        let mut gaps_iter = storage.trees_gaps.iter_start();
        let (key, _) = gaps_iter.next().unwrap().unwrap();
        let key = decode_pubkey(key.to_vec()).unwrap();

        assert_eq!(first_tree_key, key);
        assert_eq!(gaps_iter.next(), None);

        let tree_iter = storage.tree_seq_idx.iter_start();
        assert_eq!(10, tree_iter.count());

        let mut tree_iter = storage.tree_seq_idx.iter_start();
        let (key, _) = tree_iter.next().unwrap().unwrap();
        let key = decode_pubkey_u64(key.to_vec()).unwrap();
        assert_eq!((first_tree_key, 100), key);

        let slot_iter = storage.bubblegum_slots.iter_start();
        assert_eq!(5, slot_iter.count());

        let mut slot_iter = storage.bubblegum_slots.iter_start();
        let (key, _) = slot_iter.next().unwrap().unwrap();
        let key = decode_string(key.to_vec()).unwrap();
        assert_eq!(bubblegum_slots_key_to_value(key), 203);
    }
}
