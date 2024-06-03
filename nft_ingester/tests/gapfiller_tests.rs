use entities::models::{CompleteAssetDetails, Updated};
use futures::stream;
use interface::asset_streaming_and_discovery::{
    AsyncError, MockAssetDetailsConsumer, MockRawBlocksConsumer,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::gapfiller::{process_asset_details_stream, process_raw_blocks_stream};
use rocks_db::migrator::MigrationState;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiConfirmedBlock;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::{sync::Mutex, task::JoinSet};

use rocks_db::Storage;

fn create_test_complete_asset_details(pubkey: Pubkey) -> CompleteAssetDetails {
    CompleteAssetDetails {
        pubkey,
        supply: Some(Updated::new(1, None, 10)),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_process_asset_details_stream() {
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let storage = Arc::new(
        Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to create a database"),
    );

    let first_key = Pubkey::new_unique();
    let second_key = Pubkey::new_unique();

    let details1 = create_test_complete_asset_details(first_key.clone());
    let details2 = create_test_complete_asset_details(second_key.clone());

    let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
    let mut mock = MockAssetDetailsConsumer::new();
    mock.expect_get_asset_details_consumable_stream_in_range()
        .returning(move |_, _| {
            Ok(Box::pin(stream::iter(vec![
                Ok(details1.clone()),
                Ok(details2.clone()),
                Err(AsyncError::from("test error")),
            ])))
        });
    process_asset_details_stream(rx, storage.clone(), 100, 200, mock).await;

    let selected_data = storage
        .asset_dynamic_data
        .get(first_key.clone())
        .unwrap()
        .unwrap();
    assert_eq!(selected_data.supply, Some(Updated::new(1, None, 10)));

    let selected_data = storage
        .asset_dynamic_data
        .get(second_key.clone())
        .unwrap()
        .unwrap();
    assert_eq!(selected_data.supply, Some(Updated::new(1, None, 10)));
}

#[tokio::test]
async fn test_process_raw_blocks_stream() {
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let storage = Arc::new(
        Storage::open(
            temp_dir.path().to_str().unwrap(),
            Arc::new(Mutex::new(JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to create a database"),
    );
    let slot = 153;
    let blockhash = "blockhash";
    let block = entities::models::RawBlock {
        slot,
        block: UiConfirmedBlock {
            previous_blockhash: "".to_string(),
            blockhash: blockhash.to_string(),
            parent_slot: 0,
            transactions: None,
            signatures: None,
            rewards: None,
            block_time: None,
            block_height: None,
        },
    };
    let mut mock = MockRawBlocksConsumer::new();
    mock.expect_get_raw_blocks_consumable_stream_in_range()
        .returning(move |_, _| Ok(Box::pin(stream::iter(vec![Ok(block.clone())]))));
    let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
    process_raw_blocks_stream(rx, storage.clone(), 100, 200, mock).await;

    let selected_data = storage
        .raw_blocks_cbor
        .get_cbor_encoded(slot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected_data.block.blockhash, blockhash.to_string());
}
