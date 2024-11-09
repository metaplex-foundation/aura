use crate::config::{BackfillerConfig, BackfillerSourceMode, BigTableConfig};
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
use rocks_db::bubblegum_slots::ForceReingestableSlots;
use rocks_db::column::TypedColumn;
use rocks_db::transaction::{TransactionProcessor, TransactionResultPersister};
use rocks_db::{SlotStorage, Storage};
use solana_program::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta, UiConfirmedBlock,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinError;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use usecase::bigtable::{is_bubblegum_transaction_encoded, BigTableClient};
use usecase::slots_collector::SlotsGetter;
pub const GET_SIGNATURES_LIMIT: usize = 2000;
pub const GET_SLOT_RETRIES: u32 = 3;
pub const SECONDS_TO_WAIT_NEW_SLOTS: u64 = 10;
pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;
pub const DELETE_SLOT_RETRIES: u32 = 5;

pub async fn run_slot_force_persister<C, P, S>(
    force_reingestable_transactions_parser: Arc<TransactionsParser<C, P, S>>,
    rx: Receiver<()>,
) -> Result<(), JoinError>
where
    C: BlockConsumer,
    P: BlockProducer,
    S: SlotGetter,
{
    info!("Running slot force persister...");

    force_reingestable_transactions_parser
        .parse_transactions(rx)
        .await;

    info!("Force slot persister finished working.");

    Ok(())
}

pub enum BackfillSource {
    Bigtable(Arc<BigTableClient>),
    Rpc(Arc<BackfillRPC>),
}

impl BackfillSource {
    pub async fn new(
        source_mode: &BackfillerSourceMode,
        solana_rpc_address: String,
        big_table_config: &BigTableConfig,
    ) -> Self {
        match source_mode {
            BackfillerSourceMode::Bigtable => Self::Bigtable(Arc::new(
                connect_new_bigtable_from_config(big_table_config)
                    .await
                    .unwrap(),
            )),
            BackfillerSourceMode::RPC => {
                Self::Rpc(Arc::new(BackfillRPC::connect(solana_rpc_address)))
            }
        }
    }
}

#[async_trait]
impl SlotsGetter for BackfillSource {
    async fn get_slots(
        &self,
        collected_key: &Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        match self {
            BackfillSource::Bigtable(bigtable) => {
                bigtable
                    .big_table_inner_client
                    .get_slots(collected_key, start_at, rows_limit)
                    .await
            }
            BackfillSource::Rpc(rpc) => rpc.get_slots(collected_key, start_at, rows_limit).await,
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
pub struct TransactionsParser<C: BlockConsumer, P: BlockProducer, S: SlotGetter> {
    rocks_client: Arc<rocks_db::Storage>,
    slot_getter: Arc<S>,
    consumer: Arc<C>,
    producer: Arc<P>,
    metrics: Arc<BackfillerMetricsConfig>,
    workers_count: usize,
    chunk_size: usize,
}

impl<C, P, S> TransactionsParser<C, P, S>
where
    C: BlockConsumer,
    P: BlockProducer,
    S: SlotGetter,
{
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        slot_getter: Arc<S>,
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        workers_count: usize,
        chunk_size: usize,
    ) -> TransactionsParser<C, P, S> {
        TransactionsParser {
            rocks_client,
            slot_getter,
            consumer,
            producer,
            metrics,
            workers_count,
            chunk_size,
        }
    }

    pub async fn parse_transactions(&self, rx: Receiver<()>) {
        'outer: while rx.is_empty() {
            let mut slots_to_parse_iter = self.slot_getter.get_unprocessed_slots_iter();
            let mut slots_to_parse_vec = Vec::new();

            while slots_to_parse_vec.len() <= self.workers_count * self.chunk_size {
                match slots_to_parse_iter.next() {
                    Some(slot) => {
                        slots_to_parse_vec.push(slot);
                    }
                    None => {
                        if slots_to_parse_vec.is_empty() {
                            warn!("No slots to parse");
                            tokio::time::sleep(Duration::from_secs(SECONDS_TO_WAIT_NEW_SLOTS))
                                .await;
                            continue 'outer;
                        } else {
                            // got not enough slots to form usual batch
                            break;
                        }
                    }
                }
            }
            let none: Option<Arc<SlotStorage>> = None;
            let res = self
                .process_slots(slots_to_parse_vec.as_slice(), rx.resubscribe(), none)
                .await;
            match res {
                Ok(processed) => {
                    info!("Processed {} slots", processed);
                }
                Err(err) => {
                    error!("Error processing slots: {}", err);
                }
            }
        }
        tracing::info!("Transactions parser has finished working");
    }

    async fn process_slots(
        &self,
        slots_to_parse_vec: &[u64],
        rx: Receiver<()>,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<u64, String> {
        tracing::debug!("Got {} slots to parse", slots_to_parse_vec.len());
        let res = Self::parse_slots(
            self.consumer.clone(),
            self.producer.clone(),
            self.metrics.clone(),
            self.chunk_size,
            slots_to_parse_vec,
            rx,
            backup_provider,
        )
        .await?;
        let len = res.len() as u64;
        let mut counter = DELETE_SLOT_RETRIES;
        while counter > 0 {
            let result = self.slot_getter.mark_slots_processed(res.clone()).await;
            match result {
                Ok(_) => {
                    break;
                }
                Err(err) => {
                    error!("Error putting processed slots: {}", err);
                    counter -= 1;
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_ROCKSDB_OPERATION))
                        .await;
                    if counter == 0 {
                        return Err(err.to_string());
                    }
                }
            }
        }
        Ok(len)
    }

    // TODO: replace String with more meaningful error type
    pub async fn parse_slots(
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        chunk_size: usize,
        slots: &[u64],
        rx: Receiver<()>,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<Vec<u64>, String> {
        let mut tasks = Vec::new();
        let mut successful = Vec::new();
        for chunk in slots.chunks(chunk_size) {
            if !rx.is_empty() {
                break;
            }
            let chunk = chunk.to_vec();
            let c = consumer.clone();
            let p = producer.clone();
            let m = metrics.clone();
            let rx1 = rx.resubscribe();
            let backup_provider = backup_provider.clone();
            let task: tokio::task::JoinHandle<Vec<u64>> = tokio::spawn(async move {
                let mut processed = Vec::new();
                for s in chunk {
                    if !rx1.is_empty() {
                        break;
                    }
                    if c.already_processed_slot(s).await.unwrap_or(false) {
                        tracing::trace!("Slot {} is already processed, skipping", s);
                        m.inc_data_processed("slots_skipped_total");
                        processed.push(s);
                        continue;
                    }

                    let block = match p.get_block(s, backup_provider.clone()).await {
                        Ok(block) => block,
                        Err(err) => {
                            error!("Error getting block {}: {}", s, err);
                            m.inc_data_processed("error_getting_block");
                            continue;
                        }
                    };

                    if let Err(err) = c.consume_block(s, block).await {
                        error!("Error consuming block: {}", err);
                        m.inc_data_processed("slots_parsed_failed_total");
                        continue;
                    }
                    processed.push(s);
                    m.inc_data_processed("slots_parsed_success_total");
                    m.set_last_processed_slot("parsed_slot", s as i64);
                }
                processed
            });

            tasks.push(task);
        }

        for task in tasks {
            match task.await {
                Ok(r) => {
                    successful.extend(r);
                }
                Err(err) => {
                    error!(
                        "Task for parsing slots has failed: {}. Returning immediately",
                        err
                    );
                    return Err(err.to_string());
                }
            };
        }
        tracing::info!(
            "successfully parsed {} slots out of {} requested",
            successful.len(),
            slots.len()
        );
        Ok(successful)
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
) 
where
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
        .get_parameter::<u64>(rocks_db::parameters::Parameter::LastBackfilledSlot)
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
            let block_time = raw_block.block.block_time.clone();
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
