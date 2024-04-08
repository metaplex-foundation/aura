use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::Storage;
use async_trait::async_trait;
use entities::models::{RawBlock, SerializedRawBlock};
use interface::asset_streaming_and_discovery::{
    AsyncError, RawBlockGetter, RawBlocksStream, RawBlocksStreamer,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::DB;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

#[async_trait]
impl RawBlocksStreamer for Storage {
    async fn get_raw_blocks_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStream, AsyncError> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let backend = self.raw_blocks_cbor.backend.clone();
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
    tx: tokio::sync::mpsc::Sender<Result<SerializedRawBlock, AsyncError>>,
) -> Result<(), AsyncError> {
    let raw_blocks = Storage::column::<RawBlock>(backend.clone(), metrics.clone());
    let iterator = raw_blocks.iter(start_slot);

    for pair in iterator {
        let (key, value) = pair.map_err(|e| Box::new(e) as AsyncError)?;
        let slot = RawBlock::decode_key(key.to_vec()).map_err(|e| Box::new(e) as AsyncError)?;
        if slot > end_slot {
            break;
        }

        if tx
            .send(Ok(SerializedRawBlock {
                block: value.to_vec(),
            }))
            .await
            .is_err()
        {
            break; // Receiver is dropped
        }
    }

    Ok(())
}

impl RawBlockGetter for Storage {
    fn get_raw_block(&self, slot: u64) -> Result<SerializedRawBlock, AsyncError> {
        self.db
            .get_cf(
                &self.db.cf_handle(RawBlock::NAME).unwrap(),
                RawBlock::encode_key(slot),
            )
            .map_err(|e| Box::new(e) as AsyncError)
            .and_then(|res| {
                res.map(|r| SerializedRawBlock { block: r })
                    .ok_or(Box::new(StorageError::NotFound) as AsyncError)
            })
    }
}
