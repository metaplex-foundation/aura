// this test requires credentials for bigtable to be present in the root of the project
#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicBool, Arc};

    use interface::signature_persistence::{BlockConsumer, BlockProducer};
    use metrics_utils::BackfillerMetricsConfig;
    use nft_ingester::backfiller::{BigTableClient, SlotsCollector, TransactionsParser};
    use setup::rocks::RocksTestEnvironment;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_consume_a_block_and_check_if_processed() {
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
        TransactionsParser::parse_slots(
            storage.clone(),
            big_table_client.clone(),
            metrics,
            1,
            [slot].to_vec(),
            Arc::new(AtomicBool::new(true)),
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
    async fn test_search_assets_filter_from_search_assets_query_conversion_error() {
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
            160_000_000,
            130_000_000,
            metrics.clone(),
        );
        slots_collector
            .collect_slots(Arc::new(AtomicBool::new(true)))
            .await;
    }
}
