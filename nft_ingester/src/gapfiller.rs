use std::{sync::Arc, time::Duration};

use backfill_rpc::rpc::BackfillRPC;
use futures::StreamExt;
use grpc::client::Client;
use interface::{
    asset_streaming_and_discovery::{AssetDetailsConsumer, RawBlocksConsumer},
    signature_persistence::{BlockConsumer, BlockProducer},
};
use metrics_utils::{BackfillerMetricsConfig, SequenceConsistentGapfillMetricsConfig};
use rocks_db::Storage;
use tokio::{
    task::JoinError,
    time::{sleep as tokio_sleep, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, log::info};
use usecase::slots_collector::SlotsGetter;

pub async fn process_asset_details_stream_wrapper(
    cancellation_token: CancellationToken,
    cloned_rocks_storage: Arc<Storage>,
    last_saved_slot: u64,
    first_processed_slot_value: u64,
    gaped_data_client_clone: Client,
    raw_blocks: bool,
) -> Result<(), JoinError> {
    if raw_blocks {
        let processed_raw_blocks = process_raw_blocks_stream(
            cancellation_token,
            cloned_rocks_storage,
            last_saved_slot,
            first_processed_slot_value,
            gaped_data_client_clone,
        )
        .await;

        info!("Processed raw blocks: {}", processed_raw_blocks);
    } else {
        let processed_assets = process_asset_details_stream(
            cancellation_token,
            cloned_rocks_storage.clone(),
            last_saved_slot,
            first_processed_slot_value,
            gaped_data_client_clone,
        )
        .await;

        info!("Processed gaped assets: {}", processed_assets);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_sequence_consistent_gapfiller<R, BP, BC>(
    rocks_storage: Arc<Storage>,
    backfiller_source: Arc<R>,
    backfiller_metrics: Arc<BackfillerMetricsConfig>,
    sequence_consistent_gapfill_metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
    bp: Arc<BP>,
    bc: Arc<BC>,
    cancellation_token: CancellationToken,
    rpc_backfiller: Arc<BackfillRPC>,
    sequence_consistent_checker_wait_period_sec: u64,
) where
    R: SlotsGetter + Sync + Send + 'static,
    BP: BlockProducer,
    BC: BlockConsumer,
{
    let metrics = sequence_consistent_gapfill_metrics.clone();

    usecase::executor::spawn(async move {
        tracing::info!("Start collecting sequences gaps...");
        loop {
            let start = Instant::now();
            crate::sequence_consistent::collect_sequences_gaps(
                rpc_backfiller.clone(),
                rocks_storage.clone(),
                backfiller_source.clone(),
                backfiller_metrics.clone(),
                sequence_consistent_gapfill_metrics.clone(),
                bp.clone(),
                bc.clone(),
                cancellation_token.child_token(),
            )
            .await;
            metrics.set_scans_latency(start.elapsed().as_secs_f64());
            metrics.inc_total_scans();

            tokio::select! {
                _ = tokio_sleep(Duration::from_secs(sequence_consistent_checker_wait_period_sec)) => {},
                _ = cancellation_token.cancelled() => {
                    info!("Received stop signal, stopping collecting sequences gaps");
                    break;
                }
            }
        }
    });
}

/// Method returns the number of successfully processed assets
#[allow(clippy::let_and_return)]
pub async fn process_raw_blocks_stream(
    _cancellation_token: CancellationToken,
    _storage: Arc<Storage>,
    _start_slot: u64,
    _end_slot: u64,
    _raw_blocks_consumer: impl RawBlocksConsumer,
) -> u64 {
    // TODO: move to slot persister
    // let mut raw_blocks_streamer = match raw_blocks_consumer
    //     .get_raw_blocks_consumable_stream_in_range(start_slot, end_slot)
    //     .await
    // {
    //     Ok(stream) => stream,
    //     Err(e) => {
    //         error!("Error consume raw blocks stream in range: {e}");
    //         return 0;
    //     }
    // };

    let processed_slots = 0;

    // while rx.is_empty() {
    //     match raw_blocks_streamer.next().await {
    //         Some(Ok(block)) => {
    //             if let Some(e) = storage
    //                 .raw_blocks_cbor
    //                 .put(block.slot, block)
    //                 .await
    //                 .err()
    //             {
    //                 error!("Error processing raw block: {e}")
    //             } else {
    //                 processed_slots += 1;
    //             }
    //         }
    //         Some(Err(e)) => {
    //             error!("Error processing raw block stream item: {e}");
    //         }
    //         None => return processed_slots,
    //     }
    // }

    processed_slots
}

/// Method returns the number of successfully processed slots
pub async fn process_asset_details_stream(
    cancellation_token: CancellationToken,
    storage: Arc<Storage>,
    start_slot: u64,
    end_slot: u64,
    mut asset_details_consumer: impl AssetDetailsConsumer,
) -> u64 {
    let mut asset_details_stream = match asset_details_consumer
        .get_asset_details_consumable_stream_in_range(start_slot, end_slot)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            error!("Error consume asset details stream in range: {e}");
            return 0;
        },
    };

    let mut processed_assets = 0;

    while !cancellation_token.is_cancelled() {
        match asset_details_stream.next().await {
            Some(Ok(details)) => {
                if let Some(e) = storage.insert_gaped_data(details).await.err() {
                    error!("Error processing gaped data: {e}")
                } else {
                    processed_assets += 1;
                }
            },
            Some(Err(e)) => {
                error!("Error processing asset details stream item: {e}");
            },
            None => return processed_assets,
        }
    }

    processed_assets
}
