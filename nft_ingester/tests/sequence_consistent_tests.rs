#[cfg(test)]
mod tests {
    #[cfg(any(feature = "integration_tests", feature = "rpc_tests"))]
    use {rocks_db::tree_seq::TreeSeqIdx, setup::rocks::RocksTestEnvironment, std::str::FromStr};

    #[cfg(feature = "integration_tests")]
    #[tracing_test::traced_test]
    #[tokio::test(flavor = "multi_thread")]
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
        let key = rocks_db::key_encoders::decode_pubkey_u64(key.to_vec()).unwrap();
        assert_eq!(key, (first_tree_key, 102));
        let (key, _) = tree_iterator.next().unwrap().unwrap();
        let key = rocks_db::key_encoders::decode_pubkey_u64(key.to_vec()).unwrap();
        assert_eq!(key, (second_tree_key, 103));
        assert_eq!(tree_iterator.next(), None);
    }

    #[cfg(feature = "rpc_tests")]
    #[tracing_test::traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fill_gap() {
        use std::sync::Arc;

        use backfill_rpc::rpc::BackfillRPC;
        use metrics_utils::MetricState;
        use nft_ingester::{
            backfiller::DirectBlockParser,
            processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
            sequence_consistent::collect_sequences_gaps,
            transaction_ingester::BackfillTransactionIngester,
        };
        use tokio::sync::broadcast;
        use usecase::bigtable::BigTableClient;
        // Tests the following gap is filled: Gap found for MRKt4uPZY5ytQzxvAYEkeGAd3A8ir12khRUNfZvNb5U tree. Sequences: [39739, 39742], slots: [305441204, 305441218]
        // slot 305441204 also contains seq 39738, which will be in the result set as well

        let storage = RocksTestEnvironment::new(&[]).storage;
        let tree_key =
            solana_program::pubkey::Pubkey::from_str("MRKt4uPZY5ytQzxvAYEkeGAd3A8ir12khRUNfZvNb5U")
                .unwrap();
        storage.tree_seq_idx.put((tree_key, 39739), TreeSeqIdx { slot: 305441204 }).unwrap();
        storage.tree_seq_idx.put((tree_key, 39742), TreeSeqIdx { slot: 305441218 }).unwrap();

        let metrics_state = MetricState::new();
        let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            storage.clone(),
            metrics_state.ingester_metrics.clone(),
        ));

        let tx_ingester = Arc::new(BackfillTransactionIngester::new(
            backfill_bubblegum_updates_processor.clone(),
        ));
        let direct_block_parser = Arc::new(DirectBlockParser::new(
            tx_ingester.clone(),
            storage.clone(),
            metrics_state.backfiller_metrics.clone(),
        ));
        let backfiller_source = Arc::new(
            BigTableClient::connect_new_with("../creds.json".to_string(), 1000)
                .await
                .expect("should create bigtable client"),
        );
        let rpc_backfiller =
            Arc::new(BackfillRPC::connect("https://api.mainnet-beta.solana.com".to_string()));
        let (_tx, rx) = broadcast::channel::<()>(1);
        collect_sequences_gaps(
            rpc_backfiller.clone(),
            storage.clone(),
            backfiller_source.big_table_inner_client.clone(),
            metrics_state.backfiller_metrics.clone(),
            metrics_state.sequence_consistent_gapfill_metrics.clone(),
            backfiller_source.clone(),
            direct_block_parser,
            rx,
        )
        .await;

        let it = storage.tree_seq_idx.pairs_iterator(storage.tree_seq_idx.iter_start());
        let slots: Vec<_> =
            it.filter(|((k, _), _)| *k == tree_key).map(|((_, seq), _)| seq).collect();
        assert_eq!(slots, vec![39738, 39739, 39740, 39741, 39742]);
    }
}
