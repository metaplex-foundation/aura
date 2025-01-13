use crate::config::{BackfillerSourceMode, BigTableConfig};
use crate::error::IngesterError;
use async_trait::async_trait;
use backfill_rpc::rpc::BackfillRPC;
use entities::models::{BufferedTransaction, RawBlock};
use flatbuffers::FlatBufferBuilder;
use interface::error::{BlockConsumeError, StorageError, UsecaseError};
use interface::signature_persistence::{BlockConsumer, BlockProducer};
use interface::slots_dumper::{SlotGetter, SlotsDumper};
use metrics_utils::BackfillerMetricsConfig;
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use rocks_db::column::TypedColumn;
use rocks_db::columns::bubblegum_slots::ForceReingestableSlots;
use rocks_db::transaction::{TransactionProcessor, TransactionResultPersister};
use rocks_db::{SlotStorage, Storage};
use solana_program::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta, UiConfirmedBlock,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time;

use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::bigtable::{is_bubblegum_transaction_encoded, BigTableClient};
use usecase::slots_collector::SlotsGetter;

pub const GET_SIGNATURES_LIMIT: usize = 2000;
pub const GET_SLOT_RETRIES: u32 = 3;
pub const SECONDS_TO_WAIT_NEW_SLOTS: u64 = 10;
pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;
pub const DELETE_SLOT_RETRIES: u32 = 5;

pub enum BackfillSource {
    Bigtable(Arc<BigTableClient>),
    Rpc(Arc<BackfillRPC>),
}

impl BackfillSource {
    pub async fn new(
        source_mode: &BackfillerSourceMode,
        solana_rpc_address: Option<String>,
        big_table_config: Option<&BigTableConfig>,
    ) -> Self {
        match source_mode {
            BackfillerSourceMode::Bigtable => Self::Bigtable(Arc::new(
                connect_new_bigtable_from_config(
                    big_table_config.expect("big_table_config is required for Bigtable mode"),
                )
                .await
                .unwrap(),
            )),
            BackfillerSourceMode::RPC => Self::Rpc(Arc::new(BackfillRPC::connect(
                solana_rpc_address.expect("solana_rpc_address is required for RPC mode"),
            ))),
        }
    }
}

#[async_trait]
impl SlotsGetter for BackfillSource {
    async fn get_slots_sorted_desc(
        &self,
        collected_key: &Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        match self {
            BackfillSource::Bigtable(bigtable) => {
                bigtable
                    .big_table_inner_client
                    .get_slots_sorted_desc(collected_key, start_at, rows_limit)
                    .await
            }
            BackfillSource::Rpc(rpc) => {
                rpc.get_slots_sorted_desc(collected_key, start_at, rows_limit)
                    .await
            }
        }
    }
}

#[async_trait]
impl BlockProducer for BackfillSource {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<UiConfirmedBlock, StorageError> {
        match self {
            BackfillSource::Bigtable(bigtable) => bigtable.get_block(slot, backup_provider).await,
            BackfillSource::Rpc(rpc) => rpc.get_block(slot, backup_provider).await,
        }
    }
}

#[derive(Clone)]
pub struct DirectBlockParser<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    ingester: Arc<T>,
    persister: Arc<P>,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl<T, P> DirectBlockParser<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    pub fn new(
        ingester: Arc<T>,
        persister: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> DirectBlockParser<T, P> {
        DirectBlockParser {
            ingester,
            persister,
            metrics,
        }
    }
}
pub async fn run_backfill_slots<C>(
    shutdown_token: CancellationToken,
    db: Arc<Storage>,
    slot_db: Arc<SlotStorage>,
    consumer: Arc<C>,
    metrics: Arc<BackfillerMetricsConfig>,
) where
    C: BlockConsumer,
{
    loop {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received, stopping run_backfill_slots");
            break;
        }
        let sleep = tokio::time::sleep(Duration::from_millis(400));
        if let Err(e) = backfill_slots(
            &shutdown_token,
            db.clone(),
            slot_db.clone(),
            consumer.clone(),
            metrics.clone(),
        )
        .await
        {
            error!("Error while backfilling slots: {}", e);
        }
        tokio::select! {
            _ = sleep => {}
            _ = shutdown_token.cancelled() => {
                info!("Shutdown signal received, stopping run_backfill_slots");
                break;
            }
        }
    }
}

pub async fn backfill_slots<C>(
    shutdown_token: &CancellationToken,
    db: Arc<Storage>,
    slot_db: Arc<SlotStorage>,
    consumer: Arc<C>,
    metrics: Arc<BackfillerMetricsConfig>,
) -> Result<(), IngesterError>
where
    C: BlockConsumer,
{
    let start_slot = db
        .get_parameter::<u64>(rocks_db::columns::parameters::Parameter::LastBackfilledSlot)
        .await?;
    slot_db
        .db
        .try_catch_up_with_primary()
        .map_err(|e| IngesterError::DatabaseError(e.to_string()))?;
    let mut it = slot_db
        .db
        .raw_iterator_cf(&slot_db.db.cf_handle(RawBlock::NAME).unwrap());
    if let Some(start_slot) = start_slot {
        it.seek(&RawBlock::encode_key(start_slot));
    } else {
        it.seek_to_first();
    }
    while it.valid() {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received, stopping backfill_slots");
            break;
        }
        if let Some((key, raw_block_data)) = it.item() {
            let slot = RawBlock::decode_key(key.to_vec())?;
            // Process the slot
            let raw_block: RawBlock = match serde_cbor::from_slice(raw_block_data) {
                Ok(rb) => rb,
                Err(e) => {
                    error!("Failed to decode the value for slot {}: {}", slot, e);
                    continue;
                }
            };
            let block_time = raw_block.block.block_time;
            if let Err(e) = consumer.consume_block(slot, raw_block.block).await {
                error!("Error processing slot {}: {}", slot, e);
            }
            if let Some(block_time) = block_time {
                let dur = time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH + Duration::from_secs(block_time as u64))
                    .unwrap_or_default()
                    .as_millis() as f64;
                metrics.set_slot_delay_time("raw_slot_backfilled", dur);
            }
        }
        it.next();
    }
    Ok(())
}

#[async_trait]
impl<T, P> BlockConsumer for DirectBlockParser<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    async fn consume_block(
        &self,
        slot: u64,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), BlockConsumeError> {
        if block.transactions.is_none() {
            return Ok(());
        }
        let txs: Vec<EncodedTransactionWithStatusMeta> = block.transactions.unwrap();
        let mut results = Vec::new();
        for tx in txs.iter() {
            if !is_bubblegum_transaction_encoded(tx) {
                continue;
            }

            let builder = FlatBufferBuilder::new();
            let encoded_tx = tx.clone();
            let tx_wrap = EncodedConfirmedTransactionWithStatusMeta {
                transaction: encoded_tx,
                slot,
                block_time: block.block_time,
            };

            let builder = match seralize_encoded_transaction_with_status(builder, tx_wrap) {
                Ok(builder) => builder,
                Err(err) => {
                    error!("Error serializing transaction with plerkle: {}", err);
                    continue;
                }
            };

            let tx = builder.finished_data().to_vec();
            let tx = BufferedTransaction {
                transaction: tx,
                map_flatbuffer: false,
            };
            match self
                .ingester
                .get_ingest_transaction_results(tx.clone())
                .map_err(|e| e.to_string())
            {
                Ok(r) => {
                    results.push(r);
                    self.metrics.inc_data_processed("backfiller_tx_processed");
                }
                Err(e) => {
                    let signature =
                        plerkle_serialization::root_as_transaction_info(tx.transaction.as_slice())
                            .map(|parsed_tx| parsed_tx.signature().unwrap_or_default())
                            .unwrap_or_default();
                    error!("Failed to ingest transaction {}: {}", signature, e);
                    self.metrics
                        .inc_data_processed("backfiller_tx_processed_failed");
                }
            };
        }
        match self
            .persister
            .store_block(slot, results.as_slice())
            .await
            .map_err(|e| e.to_string())
        {
            Ok(_) => {
                self.metrics.inc_data_processed("backfiller_slot_processed");
            }
            Err(e) => {
                error!("Failed to persist block {}: {}", slot, e);
                self.metrics
                    .inc_data_processed("backfiller_slot_processed_failed");
            }
        };

        Ok(())
    }

    async fn already_processed_slot(&self, _slot: u64) -> Result<bool, BlockConsumeError> {
        Ok(false)
    }
}

pub async fn connect_new_bigtable_from_config(
    config: &BigTableConfig,
) -> Result<BigTableClient, IngesterError> {
    let big_table_creds = config.get_big_table_creds_key()?;
    let big_table_timeout = config.get_big_table_timeout_key()?;
    BigTableClient::connect_new_with(big_table_creds, big_table_timeout)
        .await
        .map_err(Into::into)
}

pub struct ForceReingestableSlotGetter<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    rocks_client: Arc<Storage>,
    direct_block_parser: Arc<DirectBlockParser<T, P>>,
}

impl<T, P> ForceReingestableSlotGetter<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    pub fn new(
        rocks_client: Arc<Storage>,
        direct_block_parser: Arc<DirectBlockParser<T, P>>,
    ) -> ForceReingestableSlotGetter<T, P> {
        ForceReingestableSlotGetter {
            rocks_client,
            direct_block_parser,
        }
    }
}

#[async_trait]
impl<T, P> SlotGetter for ForceReingestableSlotGetter<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    fn get_unprocessed_slots_iter(&self) -> impl Iterator<Item = u64> {
        self.rocks_client
            .force_reingestable_slots
            .iter_start()
            .filter_map(|k| k.ok())
            .map(|(k, _)| ForceReingestableSlots::decode_key(k.to_vec()))
            .filter_map(|k| k.ok())
    }

    async fn mark_slots_processed(
        &self,
        slots: Vec<u64>,
    ) -> core::result::Result<(), interface::error::StorageError> {
        self.rocks_client
            .force_reingestable_slots
            .delete_batch(slots.clone())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<T, P> SlotsDumper for ForceReingestableSlotGetter<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    async fn dump_slots(&self, slots: &[u64]) {
        if slots.is_empty() {
            return;
        }
        if let Err(e) = self
            .rocks_client
            .force_reingestable_slots
            .put_batch(slots.iter().fold(HashMap::new(), |mut acc, slot| {
                acc.insert(*slot, ForceReingestableSlots {});
                acc
            }))
            .await
        {
            tracing::error!("Error putting force-reingestable slots: {}", e);
        }
    }
}

#[async_trait]
impl<T, P> BlockConsumer for ForceReingestableSlotGetter<T, P>
where
    T: TransactionProcessor,
    P: TransactionResultPersister,
{
    async fn consume_block(
        &self,
        slot: u64,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), BlockConsumeError> {
        self.direct_block_parser.consume_block(slot, block).await
    }

    async fn already_processed_slot(&self, _slot: u64) -> Result<bool, BlockConsumeError> {
        return Ok(false);
    }
}
