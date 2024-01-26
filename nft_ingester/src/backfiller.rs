use crate::config::BackfillerConfig;
use crate::error::IngesterError;
use async_trait::async_trait;
use entities::models::BufferedTransaction;
use flatbuffers::FlatBufferBuilder;
use futures::future::join_all;
use interface::signature_persistence::{BlockConsumer, BlockProducer};
use log::{error, info, warn};
use metrics_utils::BackfillerMetricsConfig;
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use rocks_db::bubblegum_slots::{bubblegum_slots_key_to_value, form_bubblegum_slots_key};
use rocks_db::column::TypedColumn;
use rocks_db::transaction::{TransactionProcessor, TransactionResultPersister};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Duration;
use usecase::bigtable::BigTableClient;
use usecase::slots_collector::SlotsCollector;

const BBG_PREFIX: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY/";
pub const GET_SIGNATURES_LIMIT: usize = 2000;
pub const GET_SLOT_RETRIES: u32 = 3;
pub const SECONDS_TO_WAIT_NEW_SLOTS: u64 = 30;
pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;
pub const DELETE_SLOT_RETRIES: u32 = 5;

pub struct Backfiller {
    rocks_client: Arc<rocks_db::Storage>,
    big_table_client: Arc<BigTableClient>,
    slot_start_from: u64,
    slot_parse_until: u64,
    workers_count: usize,
    chunk_size: usize,
}

impl Backfiller {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        big_table_client: Arc<BigTableClient>,
        config: BackfillerConfig,
    ) -> Backfiller {
        Backfiller {
            rocks_client,
            big_table_client,
            slot_start_from: config.slot_start_from,
            slot_parse_until: config.get_slot_until(),
            workers_count: config.workers_count,
            chunk_size: config.chunk_size,
        }
    }

    pub async fn start_backfill<C, P>(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        keep_running: Arc<AtomicBool>,
        metrics: Arc<BackfillerMetricsConfig>,
        block_consumer: Arc<C>,
        block_producer: Arc<P>,
    ) -> Result<(), IngesterError>
    where
        C: BlockConsumer,
        P: BlockProducer,
    {
        info!("Backfiller is started");

        let slots_collector = SlotsCollector::new(
            self.rocks_client.clone(),
            self.big_table_client.big_table_inner_client.clone(),
            self.slot_start_from,
            self.slot_parse_until,
            metrics.clone(),
        );

        let cloned_keep_running = keep_running.clone();
        tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Running slots parser...");

            tokio::select! {
                _ = slots_collector.collect_slots(BBG_PREFIX) => {},
                _ = async {
                    while cloned_keep_running.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_secs(1)).await
                    }
                } => {}
            }
        }));

        let transactions_parser = Arc::new(TransactionsParser::new(
            self.rocks_client.clone(),
            block_consumer,
            block_producer,
            metrics.clone(),
            self.workers_count,
            self.chunk_size,
        ));

        let cloned_keep_running = keep_running.clone();
        tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Running transactions parser...");

            transactions_parser
                .parse_transactions(cloned_keep_running)
                .await;
        }));

        Ok(())
    }
}

#[derive(Clone)]
pub struct TransactionsParser<C: BlockConsumer, P: BlockProducer> {
    rocks_client: Arc<rocks_db::Storage>,
    consumer: Arc<C>,
    producer: Arc<P>,
    metrics: Arc<BackfillerMetricsConfig>,
    workers_count: usize,
    chunk_size: usize,
}

impl<C, P> TransactionsParser<C, P>
where
    C: BlockConsumer,
    P: BlockProducer,
{
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        workers_count: usize,
        chunk_size: usize,
    ) -> TransactionsParser<C, P> {
        TransactionsParser {
            rocks_client,
            consumer,
            producer,
            metrics,
            workers_count,
            chunk_size,
        }
    }

    pub async fn parse_raw_transactions(
        &self,
        keep_running: Arc<AtomicBool>,
        permits: usize,
        start_slot: Option<u64>,
    ) {
        let slots_to_parse_iter = match start_slot {
            Some(slot) => self.rocks_client.raw_blocks_cbor.iter(slot),
            None => self.rocks_client.raw_blocks_cbor.iter_start(),
        };
        let cnt = AtomicU64::new(0);
        let mut slots_to_parse_vec = Vec::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(permits));
        let mut tasks = Vec::new();
        for next in slots_to_parse_iter {
            if !keep_running.load(Ordering::SeqCst) {
                tracing::info!("terminating transactions parser");
                break;
            }
            if let Err(e) = next {
                tracing::error!("Error getting next slot: {}", e);
                continue;
            }
            let (key_box, _value_box) = next.unwrap();
            let key = rocks_db::raw_block::RawBlock::decode_key(key_box.to_vec());
            if let Err(e) = key {
                tracing::error!("Error decoding key: {}", e);
                continue;
            }
            let key = key.unwrap();
            slots_to_parse_vec.push(key);
            if slots_to_parse_vec.len() >= self.workers_count * self.chunk_size {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let slots = slots_to_parse_vec.clone();
                let c = self.consumer.clone();
                let p = self.producer.clone();
                let m = self.metrics.clone();
                let chunk_size = self.chunk_size;
                let keep_running_clone = keep_running.clone();
                let task_number = cnt.fetch_add(1, Ordering::Relaxed);
                tasks.push(tokio::task::spawn(async move {
                    let _permit = permit;
                    tracing::info!(
                        "Started a task {}, parsing {} slots",
                        task_number,
                        slots.len()
                    );
                    let res =
                        Self::parse_slots(c, p, m, chunk_size, slots, keep_running_clone).await;
                    if let Err(err) = res {
                        error!("Error parsing slots: {}", err);
                    }
                    tracing::info!("Task {} finished", task_number);
                }));
                slots_to_parse_vec.clear();
            }
        }
        if !slots_to_parse_vec.is_empty() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let slots = slots_to_parse_vec.clone();
            let c = self.consumer.clone();
            let p = self.producer.clone();
            let m = self.metrics.clone();
            let chunk_size = self.chunk_size;
            let keep_running_clone = keep_running.clone();
            let task_number = cnt.fetch_add(1, Ordering::Relaxed);
            tasks.push(tokio::task::spawn(async move {
                let _permit = permit;
                tracing::info!(
                    "Started a task {}, parsing {} slots",
                    task_number,
                    slots.len()
                );
                let res = Self::parse_slots(c, p, m, chunk_size, slots, keep_running_clone).await;
                if let Err(err) = res {
                    error!("Error parsing slots: {}", err);
                }
                tracing::info!("Task {} finished", task_number);
            }));
        }
        join_all(tasks).await;
        tracing::info!("Transactions parser has finished working");
    }

    pub async fn parse_transactions(&self, keep_running: Arc<AtomicBool>) {
        let mut counter = GET_SLOT_RETRIES;

        'outer: while keep_running.load(Ordering::SeqCst) {
            let mut slots_to_parse_iter = self.rocks_client.bubblegum_slots.iter_end();
            let mut slots_to_parse_vec = Vec::new();

            while slots_to_parse_vec.len() <= self.workers_count * self.chunk_size {
                match slots_to_parse_iter.next() {
                    Some(result) => {
                        match result {
                            Ok((key, _)) => {
                                let blgm_slot_key = String::from_utf8(key.to_vec());

                                if blgm_slot_key.is_err() {
                                    error!("Error while converting key received from RocksDB to string");
                                    continue;
                                }

                                let slot = bubblegum_slots_key_to_value(blgm_slot_key.unwrap());
                                slots_to_parse_vec.push(slot);
                            }
                            Err(e) => {
                                error!("Error while iterating over bubblegum slots: {:?}", e);
                                break;
                            }
                        }
                    }
                    None => {
                        if slots_to_parse_vec.is_empty() {
                            warn!("No slots to parse");
                            counter -= 1;
                            if counter == 0 {
                                break 'outer;
                            }
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

            counter = GET_SLOT_RETRIES;
            tracing::debug!("Got {} slots to parse", slots_to_parse_vec.len());
            let res = Self::parse_slots(
                self.consumer.clone(),
                self.producer.clone(),
                self.metrics.clone(),
                self.chunk_size,
                slots_to_parse_vec.clone(),
                keep_running.clone(),
            )
            .await;
            match res {
                Ok(v) => self.delete_slots(v).await,
                Err(err) => {
                    error!("Error parsing slots: {}", err);
                    continue;
                }
            }
        }
        tracing::info!("Transactions parser has finished working");
    }

    async fn delete_slots(&self, slots: Vec<u64>) {
        let mut counter = DELETE_SLOT_RETRIES;

        while counter > 0 {
            let delete_result = self
                .rocks_client
                .bubblegum_slots
                .delete_batch(slots.iter().map(|k| form_bubblegum_slots_key(*k)).collect())
                .await;

            match delete_result {
                Ok(_) => {
                    break;
                }
                Err(err) => {
                    error!("Error deleting slot: {}", err);
                    counter -= 1;
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_ROCKSDB_OPERATION))
                        .await;
                    continue;
                }
            }
        }
    }

    pub async fn parse_slots(
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        chunk_size: usize,
        slots: Vec<u64>,
        keep_running: Arc<AtomicBool>,
    ) -> Result<Vec<u64>, String> {
        let mut tasks = Vec::new();
        let mut successful = Vec::new();
        for chunk in slots.chunks(chunk_size) {
            if !keep_running.load(Ordering::SeqCst) {
                break;
            }
            let chunk = chunk.to_vec();
            let c = consumer.clone();
            let p = producer.clone();
            let m = metrics.clone();
            let keep_running_clone = keep_running.clone();
            let task: tokio::task::JoinHandle<Vec<u64>> = tokio::spawn(async move {
                let mut processed = Vec::new();
                for s in chunk {
                    if !keep_running_clone.load(Ordering::SeqCst) {
                        break;
                    }
                    if c.already_processed_slot(s).await.unwrap_or(false) {
                        tracing::debug!("Slot {} is already processed, skipping", s);
                        processed.push(s);
                        continue;
                    }

                    let block = match p.get_block(s).await {
                        Ok(block) => block,
                        Err(err) => {
                            error!("Error getting block: {}", err);
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
    ) -> Result<(), String> {
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
            .store_block(results)
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

    async fn already_processed_slot(&self, _slot: u64) -> Result<bool, String> {
        Ok(false)
    }
}

pub async fn connect_new_bigtable_from_config(
    config: BackfillerConfig,
) -> Result<BigTableClient, IngesterError> {
    let big_table_creds = config.big_table_config.get_big_table_creds_key()?;
    let big_table_timeout = config.big_table_config.get_big_table_timeout_key()?;
    BigTableClient::connect_new_with(big_table_creds, big_table_timeout)
        .await
        .map_err(Into::into)
}

fn is_bubblegum_transaction_encoded(tx: &EncodedTransactionWithStatusMeta) -> bool {
    let meta = if let Some(meta) = tx.meta.clone() {
        if let Err(_err) = meta.status {
            return false;
        }
        meta
    } else {
        error!("Unexpected, EncodedTransactionWithStatusMeta struct has no metadata");
        return false;
    };
    let decoded_tx = tx.transaction.decode();
    if decoded_tx.is_none() {
        return false;
    }
    let decoded_tx = decoded_tx.unwrap();
    let msg = decoded_tx.message;
    let atl_keys = msg.address_table_lookups();

    let lookup_key = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
    if msg.static_account_keys().iter().any(|k| *k == lookup_key) {
        return true;
    }

    if atl_keys.is_some() {
        let lookup_key = lookup_key.to_string();
        if let solana_transaction_status::option_serializer::OptionSerializer::Some(ad) =
            meta.loaded_addresses
        {
            return ad.writable.iter().any(|k| *k == lookup_key)
                || ad.readonly.iter().any(|k| *k == lookup_key);
        }
    }
    false
}
