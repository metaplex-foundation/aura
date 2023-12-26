mod setup;
#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use interface::AssetDetailsStreamer;
    use solana_sdk::pubkey::Pubkey;
    use tempfile::TempDir;

    use rocks_db::Storage;
    use tokio::{sync::Mutex, task::JoinSet};
    use tokio_stream::StreamExt;

    use crate::setup::setup::*;

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_empty_db() {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
        )
        .expect("Failed to create a database");

        // Call get_asset_details_stream_in_range on an empty database
        let response = storage.get_asset_details_stream_in_range(100, 200).await;

        assert!(response.is_ok());
        let mut stream = response.unwrap();

        // Check that the stream is empty
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_data_only_before_target() {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
        )
        .expect("Failed to create a database");
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
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
        )
        .expect("Failed to create a database");
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
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
        )
        .expect("Failed to create a database");
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
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
        )
        .expect("Failed to create a database");
        let pks = (0..cnt).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let mut slot = 100;
        for pk in pks.iter() {
            storage.asset_updated(slot, pk.clone()).unwrap();
        }
        // generate 1000 units of data using generate_test_static_data,generate_test_authority,generate_test_owner and create_test_dynamic_data for a 1000 unique pubkeys
        let static_data = pks
            .iter()
            .map(|pk| generate_test_static_data(pk.clone(), slot))
            .collect::<Vec<_>>();
        let authority_data = pks
            .iter()
            .map(|pk| generate_test_authority(pk.clone()))
            .collect::<Vec<_>>();
        let owner_data = pks
            .iter()
            .map(|pk| generate_test_owner(pk.clone()))
            .collect::<Vec<_>>();
        let dynamic_data = pks
            .iter()
            .map(|pk| create_test_dynamic_data(pk.clone(), slot))
            .collect::<Vec<_>>();
        // put everything in the database
        for ((((pk, static_data), authority_data), owner_data), dynamic_data) in pks
            .iter()
            .zip(static_data.iter())
            .zip(authority_data.iter())
            .zip(owner_data.iter())
            .zip(dynamic_data.iter())
        {
            storage
                .asset_authority_data
                .put(*pk, authority_data)
                .unwrap();
            storage.asset_owner_data.put(*pk, owner_data).unwrap();
            storage.asset_static_data.put(*pk, static_data).unwrap();
            storage.asset_owner_data.put(*pk, owner_data).unwrap();
            storage.asset_dynamic_data.put(*pk, dynamic_data).unwrap();
        }
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
        assert_eq!(pk_set, pks.into_iter().collect::<HashSet<_>>());
    }
}
