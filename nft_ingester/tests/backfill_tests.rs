// this test requires credentials for bigtable to be present in the root of the project
#[cfg(test)]
#[cfg(feature = "big_table_tests")]
mod tests {
    use solana_program::pubkey::Pubkey;
    use std::str::FromStr;
    use std::sync::Arc;

    use interface::{
        signature_persistence::{BlockConsumer, BlockProducer},
        slots_dumper::SlotGetter,
    };
    use metrics_utils::BackfillerMetricsConfig;
    use nft_ingester::backfiller::TransactionsParser;
    use rocks_db::bubblegum_slots::BubblegumSlotGetter;
    use setup::rocks::RocksTestEnvironment;
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
        slots_collector
            .collect_slots(
                &blockbuster::programs::bubblegum::ID,
                160_000_000,
                130_000_000,
                &mut rx,
            )
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

        let some_unreached_slot = 300_000_000_000;
        slots_collector
            .collect_slots(
                &blockbuster::programs::bubblegum::ID,
                u64::MAX,
                some_unreached_slot,
                &mut rx,
            )
            .await;
        let slot_getter = BubblegumSlotGetter::new(storage.clone());
        let mut iter = slot_getter.get_unprocessed_slots_iter();
        let one_and_only_slot = iter.next();
        assert!(one_and_only_slot.is_some());
        let one_and_only_slot = one_and_only_slot.unwrap();
        assert!(one_and_only_slot < some_unreached_slot);
        assert!(iter.next().is_none());
    }
}
