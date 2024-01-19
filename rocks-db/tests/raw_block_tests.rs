#[cfg(test)]
mod tests {

    use interface::signature_persistence::BlockConsumer;

    use setup::rocks::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_raw_block_on_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let response = storage.already_processed_slot(137827927).await.unwrap();
        assert!(response == false);
    }
}
