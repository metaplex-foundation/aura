// this test requires credentials for bigtable to be present in the root of the project
#[cfg(test)]
#[cfg(feature = "big_table_tests")]
mod tests {
    use std::sync::Arc;

    use interface::signature_persistence::{BlockConsumer, BlockProducer};
    use metrics_utils::BackfillerMetricsConfig;
    use nft_ingester::backfiller::{BubblegumSlotGetter, TransactionsParser};
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
        slots_collector
            .collect_slots(BBG_PREFIX, u64::MAX, 300_000_000_000, &mut rx)
            .await;
        assert!(storage.bubblegum_slots.iter_end().next().is_some());
    }
}
