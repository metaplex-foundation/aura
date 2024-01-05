#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use interface::asset_streaming_and_discovery::AssetDetailsStreamer;
    use solana_sdk::pubkey::Pubkey;
    use tokio_stream::StreamExt;

    use setup::rocks::*;

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;

        // Call get_asset_details_stream_in_range on an empty database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream is empty
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_data_only_before_target() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let pk = Pubkey::new_unique();

        storage.asset_updated(10, pk.clone()).unwrap();
        // Call get_asset_details_stream_in_range on a database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream is empty
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_data_only_after_target() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let pk = Pubkey::new_unique();

        storage.asset_updated(1000, pk.clone()).unwrap();
        // Call get_asset_details_stream_in_range on a database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream is empty
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_data_missing_data() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let pk = Pubkey::new_unique();

        storage.asset_updated(100, pk.clone()).unwrap();
        // Call get_asset_details_stream_in_range on a database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream contains an error
        let first_resp = stream.next().await;
        assert!(first_resp.is_some());
        let first_resp = first_resp.unwrap();
        assert!(first_resp.is_err());

        // Check that the stream is closed
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_data() {
        let cnt = 1000;
        let env = RocksTestEnvironment::new(&[]);
        let storage = &env.storage;
        let slot = 100;
        let pks = env.generate_assets(cnt, slot);
        // Call get_asset_details_stream_in_range on a database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream contains all the data
        let mut pk_set = HashSet::new();
        while let Some(resp) = stream.next().await {
            let resp = resp.unwrap();
            pk_set.insert(resp.pubkey);
        }
        assert_eq!(pk_set.len(), cnt);
        assert_eq!(pk_set, pks.0.into_iter().collect::<HashSet<_>>());
    }
}
