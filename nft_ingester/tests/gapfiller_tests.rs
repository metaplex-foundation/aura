use std::sync::Arc;

use entities::models::{AssetCompleteDetailsGrpc, RawBlockWithTransactions, Updated};
use futures::stream;
use interface::asset_streaming_and_discovery::{
    AsyncError, MockAssetDetailsConsumer, MockRawBlocksConsumer,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::gapfiller::process_asset_details_stream;
use rocks_db::{
    column::TypedColumn, columns::asset::AssetCompleteDetails,
    generated::asset_generated::asset as fb, migrator::MigrationState, SlotStorage, Storage,
};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn create_test_complete_asset_details(pubkey: Pubkey) -> AssetCompleteDetailsGrpc {
    AssetCompleteDetailsGrpc {
        pubkey,
        supply: Some(Updated::new(1, None, 10)),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_asset_details_stream() {
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let storage = Arc::new(
        Storage::open(temp_dir.path().to_str().unwrap(), red_metrics.clone(), MigrationState::Last)
            .expect("Failed to create a database"),
    );

    let first_key = Pubkey::new_unique();
    let second_key = Pubkey::new_unique();

    let details1 = create_test_complete_asset_details(first_key.clone());
    let details2 = create_test_complete_asset_details(second_key.clone());

    let mut mock = MockAssetDetailsConsumer::new();
    mock.expect_get_asset_details_consumable_stream_in_range().returning(move |_, _| {
        Ok(Box::pin(stream::iter(vec![
            Ok(details1.clone()),
            Ok(details2.clone()),
            Err(AsyncError::from("test error")),
        ])))
    });
    process_asset_details_stream(CancellationToken::new(), storage.clone(), 100, 200, mock).await;
    let selected_data = storage
        .db
        .get_pinned_cf(
            &storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            first_key.clone(),
        )
        .unwrap()
        .unwrap();
    let selected_data = fb::root_as_asset_complete_details(&selected_data).unwrap();
    let selected_data = AssetCompleteDetails::from(selected_data);
    assert_eq!(selected_data.dynamic_details.unwrap().supply, Some(Updated::new(1, None, 10)));

    let selected_data = storage
        .db
        .get_pinned_cf(
            &storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            second_key.clone(),
        )
        .unwrap()
        .unwrap();
    let selected_data = fb::root_as_asset_complete_details(&selected_data).unwrap();
    let selected_data = AssetCompleteDetails::from(selected_data);
    assert_eq!(selected_data.dynamic_details.unwrap().supply, Some(Updated::new(1, None, 10)));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "TODO: unignore when process_raw_blocks_stream is fixed"]
async fn test_process_raw_blocks_stream() {
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let storage = Arc::new(
        SlotStorage::open(temp_dir.path().to_str().unwrap(), red_metrics.clone())
            .expect("Failed to create a database"),
    );
    let slot = 153;
    let blockhash = "blockhash";
    let block = entities::models::RawBlock {
        slot,
        block: RawBlockWithTransactions {
            previous_blockhash: "".to_string(),
            blockhash: blockhash.to_string(),
            parent_slot: 0,
            transactions: Default::default(),
            block_time: None,
        },
    };
    let mut mock = MockRawBlocksConsumer::new();
    mock.expect_get_raw_blocks_consumable_stream_in_range()
        .returning(move |_, _| Ok(Box::pin(stream::iter(vec![Ok(block.clone())]))));
    let (_, _rx) = tokio::sync::broadcast::channel::<()>(1);

    // TODO: this method currently does nothing. uncomment once fixed

    // process_raw_blocks_stream(rx, storage.clone(), 100, 200, mock).await;

    let selected_data = storage.raw_blocks.get_async(slot).await.unwrap().unwrap();
    assert_eq!(selected_data.block.blockhash, blockhash.to_string());
}
