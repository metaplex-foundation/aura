use crate::config::BackfillerConfig;
use crate::error::IngesterError;
use async_trait::async_trait;
use entities::models::BufferedTransaction;
use flatbuffers::FlatBufferBuilder;
use futures::future::join_all;
use interface::signature_persistence::{BlockConsumer, BlockProducer};
use interface::slots_dumper::SlotGetter;
use log::{error, info, warn};
use metrics_utils::BackfillerMetricsConfig;
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use rocks_db::bubblegum_slots::BubblegumSlotGetter;
use rocks_db::column::TypedColumn;
use rocks_db::transaction::{TransactionProcessor, TransactionResultPersister};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta,
};
use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
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

#[derive(Clone)]
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

    pub async fn run_perpetual_slot_collection(
        &self,
        metrics: Arc<BackfillerMetricsConfig>,
        wait_period: Duration,
        mut rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), IngesterError> {
        info!("Starting perpetual slot parser");

        let slots_collector = SlotsCollector::new(
            self.rocks_client.clone(),
            self.big_table_client.big_table_inner_client.clone(),
            metrics.clone(),
        );

        let top_collected_slot = self
            .rocks_client
            .get_parameter::<u64>(rocks_db::parameters::Parameter::LastFetchedSlot)
            .await?;
        let mut parse_until = self.slot_parse_until;
        if let Some(slot) = top_collected_slot {
            parse_until = slot;
        }
        loop {
            let top_collected_slot = slots_collector
                .collect_slots(BBG_PREFIX, u64::MAX, parse_until, &rx)
                .await;
            if let Some(slot) = top_collected_slot {
                parse_until = slot;
                if let Err(e) = self
                    .rocks_client
                    .put_parameter(rocks_db::parameters::Parameter::LastFetchedSlot, slot)
                    .await
                {
                    error!("Error while updating last fetched slot: {}", e);
                }
            }

            let sleep = tokio::time::sleep(wait_period);
            tokio::select! {
            _ = sleep => {},
            _ = rx.recv() => {
                info!("Received stop signal, stopping perpetual slot parser");
                return Ok(());
            },
            }
        }
    }

    pub async fn run_perpetual_slot_processing<C, P, S>(
        &self,
        metrics: Arc<BackfillerMetricsConfig>,
        slot_getter: Arc<S>,
        block_consumer: Arc<C>,
        block_producer: Arc<P>,
        wait_period: Duration,
        rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), IngesterError>
    where
        C: BlockConsumer,
        P: BlockProducer,
        S: SlotGetter,
    {
        let transactions_parser = Arc::new(TransactionsParser::new(
            self.rocks_client.clone(),
            slot_getter,
            block_consumer,
            block_producer,
            metrics.clone(),
            self.workers_count,
            self.chunk_size,
        ));

        let mut rx = rx.resubscribe();
        while rx.is_empty() {
            transactions_parser
                .process_all_slots(rx.resubscribe())
                .await;
            tokio::select! {
            _ = tokio::time::sleep(wait_period) => {},
            _ = rx.recv() => {
                info!("Received stop signal, returning from run_perpetual_slot_fetching");
                return Ok(());
            }
            }
        }
        Ok(())
    }

    pub fn calculate_ranges(low_number: u64, high_number: u64, workers: u64) -> Vec<(u64, u64)> {
        let total_numbers = high_number - low_number + 1; // +1 to include both ends
        let base_range_size = max(total_numbers / workers, 10);
        let mut ranges = Vec::new();
        let expected_number_of_workers = max(total_numbers / base_range_size, 1);
        let mut start = low_number;
        for i in 0..expected_number_of_workers {
            // Calculate extra elements to distribute the remainder
            let extra = if i < (total_numbers % expected_number_of_workers) {
                1
            } else {
                0
            };
            let end = if i < expected_number_of_workers - 1 {
                start + base_range_size + extra - 1 // -1 to adjust for inclusive ranges
            } else {
                // The last worker should always end with high_number, no adjustment needed
                high_number
            };
            ranges.push((start, end));

            // For overlapping, the next start is the same as the current end for all but the last worker
            start = if i < expected_number_of_workers - 1 {
                end
            } else {
                end + 1
            }; // No overlap for the last element
        }

        ranges
    }

    pub async fn start_backfill<C, P>(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        rx: tokio::sync::broadcast::Receiver<()>,
        metrics: Arc<BackfillerMetricsConfig>,
        block_consumer: Arc<C>,
        block_producer: Arc<P>,
    ) -> Result<(), IngesterError>
    where
        C: BlockConsumer,
        P: BlockProducer,
    {
        info!("Backfiller is started");

        let start_from = self.slot_start_from;
        let parse_until = max(self.slot_parse_until, 130_000_000);
        let rx1 = rx.resubscribe();
        let rx2 = rx.resubscribe();
        let low_density_slot_cutoff = 200_000_000;
        let cloned_rocks = self.rocks_client.clone();
        let cloned_big_table = self.big_table_client.big_table_inner_client.clone();
        let cloned_metrics = metrics.clone();
        if parse_until < low_density_slot_cutoff {
            tasks.lock().await.spawn(tokio::spawn(async move {
                info!(
                    "Running slots parser for range {} - {}...",
                    low_density_slot_cutoff, parse_until,
                );
                let slots_collector = SlotsCollector::new(
                    cloned_rocks.clone(),
                    cloned_big_table.clone(),
                    cloned_metrics.clone(),
                );

                slots_collector
                    .collect_slots(BBG_PREFIX, start_from, parse_until, &rx1.resubscribe())
                    .await;
                info!(
                    "Slots parser for range {} - {} finished",
                    low_density_slot_cutoff, parse_until
                );
            }));
        }
        let parse_until = max(self.slot_parse_until, low_density_slot_cutoff);
        let ranges = Self::calculate_ranges(parse_until, start_from, self.workers_count as u64);
        for (low, high) in ranges {
            let cloned_rocks = self.rocks_client.clone();
            let cloned_big_table = self.big_table_client.big_table_inner_client.clone();
            let cloned_metrics = metrics.clone();
            let rx1 = rx.resubscribe();
            tasks.lock().await.spawn(tokio::spawn(async move {
                info!("Running slots parser...");
                let slots_collector = SlotsCollector::new(
                    cloned_rocks.clone(),
                    cloned_big_table.clone(),
                    cloned_metrics.clone(),
                );

                slots_collector
                    .collect_slots(BBG_PREFIX, high, low, &rx1.resubscribe())
                    .await;
            }));
        }

        let transactions_parser = Arc::new(TransactionsParser::new(
            self.rocks_client.clone(),
            Arc::new(BubblegumSlotGetter::new(self.rocks_client.clone())),
            block_consumer,
            block_producer,
            metrics.clone(),
            self.workers_count,
            self.chunk_size,
        ));
        tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Running transactions parser...");

            transactions_parser.parse_transactions(rx2).await;
        }));

        Ok(())
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

    pub async fn parse_raw_transactions(
        &self,
        rx: Receiver<()>,
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
            if !rx.is_empty() {
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
                let task_number = cnt.fetch_add(1, Ordering::Relaxed);
                let rx = rx.resubscribe();
                tasks.push(tokio::task::spawn(async move {
                    let _permit = permit;
                    tracing::info!(
                        "Started a task {}, parsing {} slots",
                        task_number,
                        slots.len()
                    );
                    let res = Self::parse_slots(c, p, m, chunk_size, slots, rx.resubscribe()).await;
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
            let task_number = cnt.fetch_add(1, Ordering::Relaxed);
            tasks.push(tokio::task::spawn(async move {
                let _permit = permit;
                tracing::info!(
                    "Started a task {}, parsing {} slots",
                    task_number,
                    slots.len()
                );
                let res = Self::parse_slots(c, p, m, chunk_size, slots, rx.resubscribe()).await;
                if let Err(err) = res {
                    error!("Error parsing slots: {}", err);
                }
                tracing::info!("Task {} finished", task_number);
            }));
        }
        join_all(tasks).await;
        tracing::info!("Transactions parser has finished working");
    }

    pub async fn process_all_slots(&self, rx: Receiver<()>) {
        let slots_iter = self.slot_getter.get_unprocessed_slots_iter();
        let chunk_size = self.workers_count * self.chunk_size;

        let mut slots_batch = Vec::with_capacity(chunk_size);

        for slot in slots_iter {
            if !rx.is_empty() {
                info!("Received stop signal, returning from process_all_slots");
                return;
            }
            slots_batch.push(slot);
            if slots_batch.len() >= chunk_size {
                info!("Got {} slots to parse", slots_batch.len());
                let res = self
                    .process_slots(slots_batch.clone(), rx.resubscribe())
                    .await;
                match res {
                    Ok(processed) => {
                        info!("Processed {} slots", processed);
                    }
                    Err(err) => {
                        error!("Error processing slots: {}", err);
                    }
                }
                slots_batch.clear();
            }
        }
        if !rx.is_empty() {
            info!("Received stop signal, returning");
            return;
        }
        if !slots_batch.is_empty() {
            info!("Got {} slots to parse", slots_batch.len());
            let res = self.process_slots(slots_batch, rx.resubscribe()).await;
            match res {
                Ok(processed) => {
                    info!("Processed {} slots", processed);
                }
                Err(err) => {
                    error!("Error processing slots: {}", err);
                }
            }
        }
    }

    pub async fn parse_transactions(&self, rx: Receiver<()>) {
        let mut counter = GET_SLOT_RETRIES;
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
            let res = self
                .process_slots(slots_to_parse_vec, rx.resubscribe())
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
        slots_to_parse_vec: Vec<u64>,
        rx: Receiver<()>,
    ) -> Result<u64, String> {
        tracing::debug!("Got {} slots to parse", slots_to_parse_vec.len());
        let res = Self::parse_slots(
            self.consumer.clone(),
            self.producer.clone(),
            self.metrics.clone(),
            self.chunk_size,
            slots_to_parse_vec.clone(),
            rx,
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

    pub async fn parse_slots(
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        chunk_size: usize,
        slots: Vec<u64>,
        rx: Receiver<()>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_ranges() {
        let ranges = Backfiller::calculate_ranges(200_000_000, 245_867_321, 10);
        assert_eq!(ranges.len(), 10);
        assert_eq!(ranges[0], (200_000_000, 204_586_732));
        assert_eq!(ranges[1], (204_586_732, 209_173_464));
        assert_eq!(ranges[2], (209_173_464, 213_760_195));
        assert_eq!(ranges[3], (213_760_195, 218_346_926));
        assert_eq!(ranges[4], (218_346_926, 222_933_657));
        assert_eq!(ranges[5], (222_933_657, 227_520_388));
        assert_eq!(ranges[6], (227_520_388, 232_107_119));
        assert_eq!(ranges[7], (232_107_119, 236_693_850));
        assert_eq!(ranges[8], (236_693_850, 241_280_581));
        assert_eq!(ranges[9], (241_280_581, 245_867_321));
    }

    #[test]
    fn test_calculate_ranges_small_range() {
        let ranges = Backfiller::calculate_ranges(200_000_000, 200_000_002, 10);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (200_000_000, 200_000_002));
    }
}
