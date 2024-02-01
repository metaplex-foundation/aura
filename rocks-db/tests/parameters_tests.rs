#[cfg(test)]
mod tests {
    use rocks_db::parameters::Parameter;
    use setup::rocks::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_raw_block_on_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let response = storage
            .get_parameter::<u64>(Parameter::LastBackfilledSlot)
            .await
            .unwrap();
        assert!(response == None);
        let response = storage
            .get_parameter::<u64>(Parameter::LastFetchedSlot)
            .await
            .unwrap();
        assert!(response == None);
        let last_backfilled_slot = 137827927u64;
        let last_fetched_slot = 242827927u64;
        storage
            .put_parameter(Parameter::LastBackfilledSlot, last_backfilled_slot)
            .await
            .unwrap();
        storage
            .put_parameter(Parameter::LastFetchedSlot, last_fetched_slot)
            .await
            .unwrap();
        let response = storage
            .get_parameter::<u64>(Parameter::LastBackfilledSlot)
            .await
            .unwrap();
        assert!(response == Some(last_backfilled_slot));
        let response = storage
            .get_parameter::<u64>(Parameter::LastFetchedSlot)
            .await
            .unwrap();
        assert!(response == Some(last_fetched_slot));
    }
}
