// this test requires credentials for bigtable to be present in the root of the project
#[cfg(test)]
#[cfg(feature = "big_table_tests")]
mod tests {
    use std::sync::Arc;

    use interface::signature_persistence::{BlockConsumer, BlockProducer};
    use metrics_utils::BackfillerMetricsConfig;
    use nft_ingester::backfiller::{BigTableClient, TransactionsParser};
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
        let tp = TransactionsParser::new(
            storage.clone(),
            storage.clone(),
            big_table_client.clone(),
            metrics,
            1,
            1,
        );
        let slot = 242596740;
        let response = storage.already_processed_slot(slot).await.unwrap();
        assert!(response == false);
        tp.parse_slots([slot].to_vec()).await.unwrap();
        let response = storage.already_processed_slot(slot).await.unwrap();
        assert!(response == true);
        let original_block = big_table_client.get_block(slot).await.unwrap();
        let persisted = storage.get_block(slot).await.unwrap();
        assert_eq!(persisted, original_block);
    }
}
