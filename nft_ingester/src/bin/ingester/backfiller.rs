use async_trait::async_trait;
use entities::models::BufferedTransaction;
use flatbuffers::FlatBufferBuilder;
use interface::error::StorageError;
use interface::signature_persistence::{BlockConsumer, BlockProducer, TransactionIngester};
use log::{error, info};
use metrics_utils::{BackfillerMetricsConfig, MetricStatus};
use nft_ingester::config::BackfillerConfig;
use nft_ingester::error::IngesterError;
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use rocks_db::bubblegum_slots::{
    bubblegum_slots_key_to_value, form_bubblegum_slots_key, BubblegumSlots,
};
use solana_bigtable_connection::{bigtable::BigTableConnection, CredentialType};
use solana_sdk::clock::Slot;
use solana_storage_bigtable::LedgerStorage;
use solana_storage_bigtable::{DEFAULT_APP_PROFILE_ID, DEFAULT_INSTANCE_NAME};
use solana_transaction_status::{
    BlockEncodingOptions, EncodedConfirmedTransactionWithStatusMeta,
    EncodedTransactionWithStatusMeta, TransactionDetails,
};
use std::collections::HashMap;
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::{mpsc, Mutex};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Duration;

const BBG_PREFIX: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY/";
pub const GET_SIGNATURES_LIMIT: usize = 2000;
pub const BATCH_SLOTS_TO_PARSE: usize = 100;
pub const SECONDS_TO_WAIT_NEW_SLOTS: u64 = 30;
pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_GET_DATA_FROM_BG: u64 = 5;
pub const PUT_SLOT_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;

pub struct Backfiller {
    rocks_client: Arc<rocks_db::Storage>,
    big_table_client: Arc<BigTableClient>,
    slot_start_from: u64,
    slot_parse_until: u64,
    workers_count: usize,
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
        }
    }

    pub async fn start_backfill<C, P>(
        &self,
        tasks: Arc<Mutex<JoinSet<core::result::Result<(), JoinError>>>>,
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
        )
        .await;

        let cloned_keep_running = keep_running.clone();
        tasks.lock().await.spawn(tokio::spawn(async move {
            info!("Running slots parser...");

            slots_collector.collect_slots(cloned_keep_running).await;
        }));

        let transactions_parser = Arc::new(
            TransactionsParser::new(
                self.rocks_client.clone(),
                block_consumer,
                block_producer,
                metrics.clone(),
                self.workers_count,
            )
            .await,
        );

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

pub struct SlotsCollector {
    rocks_client: Arc<rocks_db::Storage>,
    big_table_inner_client: Arc<BigTableConnection>,
    slot_start_from: u64,
    slot_parse_until: u64,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl SlotsCollector {
    pub async fn new(
        rocks_client: Arc<rocks_db::Storage>,
        big_table_inner_client: Arc<BigTableConnection>,
        slot_start_from: u64,
        slot_parse_until: u64,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> SlotsCollector {
        SlotsCollector {
            rocks_client,
            big_table_inner_client,
            slot_start_from,
            slot_parse_until,
            metrics,
        }
    }

    pub async fn collect_slots(&self, keep_running: Arc<AtomicBool>) {
        let mut start_at_slot = self.slot_start_from;

        while keep_running.load(Ordering::SeqCst) {
            let slots = self
                .big_table_inner_client
                .client()
                .get_row_keys(
                    "tx-by-addr",
                    Some(self.slot_to_row(BBG_PREFIX, start_at_slot)),
                    Some(self.slot_to_row(BBG_PREFIX, Slot::MIN)),
                    GET_SIGNATURES_LIMIT as i64,
                )
                .await;

            match slots {
                Ok(bg_slots) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::SUCCESS);

                    let mut slots = Vec::new();

                    for slot in bg_slots.iter() {
                        let slot_value = self.row_to_slot(BBG_PREFIX, slot);

                        match slot_value {
                            Ok(slot) => {
                                slots.push(!slot);
                            }
                            Err(err) => {
                                error!(
                                    "Error while converting key received from BigTable to slot: {}",
                                    err
                                );
                            }
                        }
                    }

                    if !slots.is_empty() {
                        // safe to call unwrap because we checked that slots is not empty
                        let last_slot = *slots.last().unwrap();
                        self.save_slots(&slots).await;

                        self.metrics
                            .set_last_processed_slot("collected_slot", last_slot as i64);

                        if (slots.len() == 1 && slots[0] == start_at_slot)
                            || (last_slot < self.slot_parse_until)
                        {
                            info!("All the slots are collected");
                            break;
                        }
                        start_at_slot = last_slot;
                    } else {
                        info!("All the slots are collected");
                        break;
                    }
                }
                Err(err) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::FAILURE);
                    error!("Error getting slots: {}", err);
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                }
            }
        }
    }

    async fn save_slots(&self, slots: &[u64]) {
        let slots_map: HashMap<String, BubblegumSlots> = slots.iter().fold(
            HashMap::new(),
            |mut acc: HashMap<String, BubblegumSlots>, slot| {
                acc.insert(form_bubblegum_slots_key(*slot), BubblegumSlots {});
                acc
            },
        );

        if !slots_map.is_empty() {
            let mut counter = PUT_SLOT_RETRIES;
            while counter > 0 {
                let put_result = self
                    .rocks_client
                    .bubblegum_slots
                    .put_batch(slots_map.clone())
                    .await;

                match put_result {
                    Ok(_) => {
                        break;
                    }
                    Err(err) => {
                        error!("Error putting slots: {}", err);
                        counter -= 1;
                        tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_ROCKSDB_OPERATION))
                            .await;
                        continue;
                    }
                }
            }
        }
    }

    fn slot_to_row(&self, prefix: &str, slot: Slot) -> String {
        let slot = !slot;
        format!("{}{slot:016x}", prefix)
    }

    fn row_to_slot(&self, prefix: &str, key: &str) -> Result<Slot, ParseIntError> {
        Slot::from_str_radix(&key[prefix.len()..], 16)
    }
}

#[derive(Clone)]
pub struct TransactionsParser<C: BlockConsumer, P: BlockProducer> {
    rocks_client: Arc<rocks_db::Storage>,
    consumer: Arc<C>,
    producer: Arc<P>,
    metrics: Arc<BackfillerMetricsConfig>,
    workers_count: usize,
}

impl<C, P> TransactionsParser<C, P>
where
    C: BlockConsumer,
    P: BlockProducer,
{
    pub async fn new(
        rocks_client: Arc<rocks_db::Storage>,
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        workers_count: usize,
    ) -> TransactionsParser<C, P> {
        TransactionsParser {
            rocks_client,
            consumer,
            producer,
            metrics,
            workers_count,
        }
    }

    pub async fn parse_transactions(&self, keep_running: Arc<AtomicBool>) {
        let (tx, rx) = mpsc::channel::<u64>(BATCH_SLOTS_TO_PARSE);
        let rx = Arc::new(Mutex::new(rx));

        // Slot fetcher task
        let fetcher = {
            let tx = tx.clone();
            let keep_running = keep_running.clone();
            let rocks_client = self.rocks_client.clone();
            spawn(async move {
                while keep_running.load(Ordering::SeqCst) {
                    let slots_to_parse_iter = rocks_client.bubblegum_slots.iter_end();
                    for slot in slots_to_parse_iter {
                        match slot {
                            Ok((key, _)) => {
                                let blgm_slot_key = String::from_utf8(key.to_vec());

                                if blgm_slot_key.is_err() {
                                    error!("Error while converting key received from RocksDB to string");
                                    continue;
                                }

                                let slot = bubblegum_slots_key_to_value(blgm_slot_key.unwrap());
                                if let Err(e) = tx.send(slot).await {
                                    error!("Error sending slot to parse: {:?}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Error while iterating over bubblegum slots: {:?}", e);
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_WAIT_NEW_SLOTS)).await;
                }
            })
        };

        // Worker tasks
        let mut workers = Vec::new();
        for _ in 0..self.workers_count {
            let rx = rx.clone();
            let consumer = self.consumer.clone();
            let producer = self.producer.clone();
            let metrics = self.metrics.clone();
            let client = self.rocks_client.clone();
            workers.push(spawn(async move {
                while let Some(slot) = rx.lock().await.recv().await {
                    // Process the slot
                    Self::process_slot(
                        consumer.clone(),
                        producer.clone(),
                        metrics.clone(),
                        client.clone(),
                        slot,
                    )
                    .await; // Implement process_slot based on your logic
                }
            }));
        }

        // Wait for all tasks to complete
        fetcher.await.expect("Fetcher task panicked");
        for worker in workers {
            worker.await.expect("Worker task panicked");
        }
    }

    async fn process_slot(
        consumer: Arc<C>,
        producer: Arc<P>,
        metrics: Arc<BackfillerMetricsConfig>,
        db_client: Arc<rocks_db::Storage>,
        slot: u64,
    ) {
        if consumer.already_processed_slot(slot).await.unwrap_or(false) {
            return;
        }
        let block = match producer.get_block(slot).await {
            Ok(block) => block,
            Err(err) => {
                error!("Error getting block: {}", err);
                return;
            }
        };

        if let Err(err) = consumer.consume_block(block).await {
            error!("Error consuming block: {}", err);
            return;
        }
        if let Err(err) = db_client
            .bubblegum_slots
            .delete(form_bubblegum_slots_key(slot))
        {
            error!("Error deleting slot: {}", err);
            return;
        }
        metrics.set_last_processed_slot("parsed_slot", slot as i64);
    }
}

#[derive(Clone)]
pub struct DirectBlockParser<T>
where
    T: TransactionIngester,
{
    ingester: Arc<T>,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl<T> DirectBlockParser<T>
where
    T: TransactionIngester,
{
    pub fn new(ingester: Arc<T>, metrics: Arc<BackfillerMetricsConfig>) -> DirectBlockParser<T> {
        DirectBlockParser { ingester, metrics }
    }
}

#[async_trait]
impl<T> BlockConsumer for DirectBlockParser<T>
where
    T: TransactionIngester,
{
    async fn consume_block(
        &self,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), String> {
        if block.transactions.is_none() {
            return Ok(());
        }
        let txs: Vec<EncodedTransactionWithStatusMeta> = block.transactions.unwrap();
        for tx in txs.iter() {
            if !is_bubblegum_transaction(tx) {
                continue;
            }

            let builder = FlatBufferBuilder::new();
            let encoded_tx = tx.clone();
            let tx_wrap = EncodedConfirmedTransactionWithStatusMeta {
                transaction: encoded_tx,
                slot: block.parent_slot,
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
            self.ingester
                .ingest_transaction(tx)
                .await
                .map_err(|e| e.to_string())?;
            self.metrics.inc_data_processed("backfiller_tx_processed");
        }
        self.metrics.inc_data_processed("backfiller_slot_processed");

        Ok(())
    }

    async fn already_processed_slot(&self, _slot: u64) -> Result<bool, String> {
        Ok(false)
    }
}

pub struct BigTableClient {
    pub big_table_client: Arc<LedgerStorage>,
    pub big_table_inner_client: Arc<BigTableConnection>,
}

impl BigTableClient {
    pub fn new(
        big_table_client: Arc<LedgerStorage>,
        big_table_inner_client: Arc<BigTableConnection>,
    ) -> BigTableClient {
        BigTableClient {
            big_table_client,
            big_table_inner_client,
        }
    }

    pub async fn connect_new_from_config(
        config: BackfillerConfig,
    ) -> Result<BigTableClient, IngesterError> {
        let big_table_creds = config.big_table_config.get_big_table_creds_key()?;
        let big_table_timeout = config.big_table_config.get_big_table_timeout_key()?;

        let big_table_client = LedgerStorage::new(
            true,
            Some(Duration::from_secs(big_table_timeout as u64)),
            Some(big_table_creds.to_string()),
        )
        .await?;

        let big_table_inner_client = solana_bigtable_connection::bigtable::BigTableConnection::new(
            DEFAULT_INSTANCE_NAME,
            DEFAULT_APP_PROFILE_ID,
            true,
            None,
            CredentialType::Filepath(Some(big_table_creds)),
        )
        .await
        .unwrap();

        Ok(BigTableClient::new(
            Arc::new(big_table_client),
            Arc::new(big_table_inner_client),
        ))
    }
}

#[async_trait]
impl BlockProducer for BigTableClient {
    async fn get_block(
        &self,
        slot: u64,
    ) -> Result<solana_transaction_status::UiConfirmedBlock, StorageError> {
        let mut counter = GET_DATA_FROM_BG_RETRIES;

        loop {
            let block = match self.big_table_client.get_confirmed_block(slot).await {
                Ok(block) => block,
                Err(err) => {
                    error!("Error getting block: {}", err);
                    counter -= 1;
                    if counter == 0 {
                        return Err(StorageError::Common(format!(
                            "Error getting block: {}",
                            err
                        )));
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                }
            };

            let mut encoded: solana_transaction_status::UiConfirmedBlock = block
                .clone()
                .encode_with_options(
                    solana_transaction_status::UiTransactionEncoding::Base58,
                    BlockEncodingOptions {
                        transaction_details: TransactionDetails::Full,
                        show_rewards: true,
                        max_supported_transaction_version: Some(u8::MAX),
                    },
                )
                .map_err(|e| StorageError::Common(e.to_string()))?;
            encoded.transactions = encoded.transactions.map(|txs| {
                txs.iter()
                    .filter(|tx| is_bubblegum_transaction(tx))
                    .cloned()
                    .collect()
            });
            return Ok(encoded);
        }
    }
}

fn is_bubblegum_transaction(tx: &EncodedTransactionWithStatusMeta) -> bool {
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
