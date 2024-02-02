// this test requires credentials for bigtable to be present in the root of the project
#[cfg(test)]
// #[cfg(feature = "big_table_tests")]
mod tests {
    use std::{collections::VecDeque, str::FromStr, sync::Arc};

    use entities::models::BufferedTransaction;
    use flatbuffers::FlatBufferBuilder;
    use interface::{
        signature_persistence::{BlockConsumer, BlockProducer},
        slots_dumper::{SlotGetter, SlotsDumper},
    };
    use metrics_utils::{BackfillerMetricsConfig, IngesterMetricsConfig};
    use nft_ingester::{
        backfiller::{is_bubblegum_transaction_encoded, TransactionsParser},
        bubblegum_updates_processor::BubblegumTxProcessor,
        db_v2::Task,
        transaction_ingester,
    };
    use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
    use rocks_db::{bubblegum_slots::BubblegumSlotGetter, transaction::TransactionProcessor};
    use setup::rocks::RocksTestEnvironment;
    use solana_program::pubkey::Pubkey;
    use solana_transaction_status::{
        EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta,
    };
    use tokio::sync::Mutex;
    use usecase::{bigtable::BigTableClient, slots_collector::SlotsCollector};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_consume_a_block_and_check_if_processed() {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        let storage = RocksTestEnvironment::new(&[]).storage;
        let big_table_client = Arc::new(
            BigTableClient::connect_new_with("../../creds.json".to_string(), 1000)
                .await
                .unwrap(),
        );

        let metrics = Arc::new(BackfillerMetricsConfig::new());
        let slot = 242596740;
        let response = storage.already_processed_slot(slot).await.unwrap();
        assert!(response == false);
        TransactionsParser::<rocks_db::Storage, BigTableClient, BubblegumSlotGetter>::parse_slots(
            storage.clone(),
            big_table_client.clone(),
            metrics,
            1,
            [slot].to_vec(),
            rx,
        )
        .await
        .unwrap();
        let response = storage.already_processed_slot(slot).await.unwrap();
        assert!(response == true);
        let original_block = big_table_client.get_block(slot).await.unwrap();
        let persisted = storage.get_block(slot).await.unwrap();
        assert_eq!(persisted, original_block);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_collect_slots() {
        let (_tx, mut rx) = tokio::sync::broadcast::channel(1);

        let storage = RocksTestEnvironment::new(&[]).storage;
        let big_table_client = Arc::new(
            BigTableClient::connect_new_with("../../creds.json".to_string(), 1000)
                .await
                .unwrap(),
        );
        let metrics = Arc::new(BackfillerMetricsConfig::new());

        let slots_collector = SlotsCollector::new(
            storage.clone(),
            big_table_client.big_table_inner_client.clone(),
            metrics.clone(),
        );
        const BBG_PREFIX: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY/";
        slots_collector
            .collect_slots(BBG_PREFIX, 160_000_000, 130_000_000, &mut rx)
            .await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_max_u64_request() {
        let (_tx, mut rx) = tokio::sync::broadcast::channel(1);
        let storage = RocksTestEnvironment::new(&[]).storage;
        let big_table_client = Arc::new(
            BigTableClient::connect_new_with("../../creds.json".to_string(), 1000)
                .await
                .unwrap(),
        );
        let metrics = Arc::new(BackfillerMetricsConfig::new());
        let slots_collector = SlotsCollector::new(
            storage.clone(),
            big_table_client.big_table_inner_client.clone(),
            metrics.clone(),
        );

        const BBG_PREFIX: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY/";
        let some_unreached_slot = 300_000_000_000;
        slots_collector
            .collect_slots(BBG_PREFIX, u64::MAX, some_unreached_slot, &mut rx)
            .await;
        let slot_getter = BubblegumSlotGetter::new(storage.clone());
        let mut iter = slot_getter.get_unprocessed_slots_iter();
        let one_and_only_slot = iter.next();
        assert!(one_and_only_slot.is_some());
        let one_and_only_slot = one_and_only_slot.unwrap();
        assert!(one_and_only_slot < some_unreached_slot);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_consume_a_block_and_check_if_processed2() {
        let big_table_client = Arc::new(
            BigTableClient::connect_new_with("../../creds.json".to_string(), 1000)
                .await
                .unwrap(),
        );
        let storage = RocksTestEnvironment::new(&[]).storage;

        let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
            storage.clone(),
            Arc::new(IngesterMetricsConfig::new()),
            Arc::new(Mutex::new(VecDeque::<Task>::new())),
        ));

        let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
            bubblegum_updates_processor.clone(),
        ));

        let slot = 245464459;
        let block = big_table_client.get_block(slot).await.unwrap();
        let txs: Vec<EncodedTransactionWithStatusMeta> = block.transactions.unwrap();
        let mut results = Vec::new();
        for tx in txs.iter() {
            if !is_bubblegum_transaction_encoded(tx) {
                continue;
            }
            let builder = FlatBufferBuilder::new();
            let encoded_tx = tx.clone();
            let tx_wrap = EncodedConfirmedTransactionWithStatusMeta {
                transaction: encoded_tx,
                slot,
                block_time: block.block_time,
            };

            let builder = seralize_encoded_transaction_with_status(builder, tx_wrap).unwrap();

            let tx = builder.finished_data().to_vec();
            let tx = BufferedTransaction {
                transaction: tx,
                map_flatbuffer: false,
            };

            results.push(
                tx_ingester
                    .get_ingest_transaction_results(tx.clone())
                    .map_err(|e| e.to_string())
                    .unwrap(),
            );
        }
        tracing::info!("Results: {:?}", results);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_consume_a_block_and_check_if_processed3() {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);

        let big_table_client = Arc::new(
            BigTableClient::connect_new_with("../../creds.json".to_string(), 1000)
                .await
                .unwrap(),
        );
        let metrics = Arc::new(BackfillerMetricsConfig::new());
        let dumper = Arc::new(ScreenDumper {});
        let slots_collector = SlotsCollector::new(
            dumper,
            big_table_client.big_table_inner_client.clone(),
            metrics.clone(),
        );
        let tree = Pubkey::from_str("7RD2TjLhgTLTftqjUK3irotGbemYvaMrjSLffj73fX1U").unwrap();
        let prefix = &format!("{}/", tree);
        slots_collector
            .collect_slots(prefix, 245553329, 245296450, &rx)
            .await;
    }

    struct ScreenDumper {}
    #[async_trait::async_trait]
    impl SlotsDumper for ScreenDumper {
        async fn dump_slots(&self, slots: &[u64]) {
            tracing::warn!("Dumping slots: {:?}", slots);
        }
    }
}
