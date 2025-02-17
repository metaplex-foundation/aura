use std::sync::Arc;

use async_trait::async_trait;
use entities::models::RawBlock;
use interface::asset_streaming_and_discovery::{
    AsyncError, RawBlockGetter, RawBlocksStream, RawBlocksStreamer,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::DB;
use tokio_stream::wrappers::ReceiverStream;

use crate::{column::TypedColumn, errors::StorageError, SlotStorage, Storage};

#[async_trait]
impl RawBlocksStreamer for SlotStorage {
    async fn get_raw_blocks_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStream, AsyncError> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let backend = self.db.clone();
        let metrics = self.red_metrics.clone();
        self.join_set.lock().await.spawn(tokio::spawn(async move {
            let _ = process_raw_blocks_range(
                backend,
                start_slot,
                end_slot,
                metrics.clone(),
                tx.clone(),
            )
            .await;
        }));

        Ok(Box::pin(ReceiverStream::new(rx)) as RawBlocksStream)
    }
}

async fn process_raw_blocks_range(
    backend: Arc<DB>,
    start_slot: u64,
    end_slot: u64,
    metrics: Arc<RequestErrorDurationMetrics>,
    tx: tokio::sync::mpsc::Sender<Result<RawBlock, AsyncError>>,
) -> Result<(), AsyncError> {
    let raw_blocks = Storage::column::<RawBlock>(backend.clone(), metrics.clone());
    let iterator = raw_blocks.iter(start_slot);

    for pair in iterator {
        let (key, value) = pair.map_err(|e| Box::new(e) as AsyncError)?;
        let block = RawBlock::decode(value.as_ref()).map_err(|e| Box::new(e) as AsyncError)?;
        let slot = RawBlock::decode_key(key.to_vec()).map_err(|e| Box::new(e) as AsyncError)?;
        if slot > end_slot {
            break;
        }

        if tx.send(Ok(block)).await.is_err() {
            break; // Receiver is dropped
        }
    }

    Ok(())
}

impl RawBlockGetter for Storage {
    fn get_raw_block(&self, slot: u64) -> Result<RawBlock, AsyncError> {
        self.db
            .get_cf(&self.db.cf_handle(RawBlock::NAME).unwrap(), RawBlock::encode_key(slot))
            .map_err(|e| Box::new(e) as AsyncError)
            .and_then(|res| {
                let err_msg = format!("Cannot get raw block with slot: '{slot}'!");
                res.and_then(|r| RawBlock::decode(r.as_slice()).ok())
                    .ok_or(Box::new(StorageError::NotFound(err_msg)) as AsyncError)
            })
    }
}
