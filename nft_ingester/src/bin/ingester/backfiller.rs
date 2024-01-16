use async_trait::async_trait;
use entities::models::BufferedTransaction;
use flatbuffers::FlatBufferBuilder;
use log::{debug, error, info, warn};
use metrics_utils::{BackfillerMetricsConfig, MetricStatus};
use nft_ingester::buffer::Buffer;
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
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::collections::HashMap;
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Duration;

const BBG_PREFIX: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY/";
pub const GET_SIGNATURES_LIMIT: usize = 2000;
pub const GET_SLOT_RETRIES: u32 = 3;
pub const BATCH_SLOTS_TO_PARSE: usize = 100;
pub const SECONDS_TO_WAIT_NEW_SLOTS: u64 = 30;
pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_GET_DATA_FROM_BG: u64 = 5;
pub const PUT_SLOT_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;
pub const DELETE_SLOT_RETRIES: u32 = 5;

pub struct Backfiller {
    rocks_client: Arc<rocks_db::Storage>,
    big_table_client: Arc<LedgerStorage>,
    big_table_inner_client: Arc<BigTableConnection>,
    buffer: Arc<Buffer>,
    slot_start_from: u64,
    slot_parse_until: u64,
}

#[async_trait]
pub trait BlockConsumer {
    async fn consume_block(&self, block: solana_transaction_status::ConfirmedBlock);
}

impl Backfiller {
    pub async fn new(
        rocks_client: Arc<rocks_db::Storage>,
        buffer: Arc<Buffer>,
        config: BackfillerConfig,
    ) -> Result<Backfiller, IngesterError> {
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

        Ok(Backfiller {
            rocks_client,
            big_table_client: Arc::new(big_table_client),
            big_table_inner_client: Arc::new(big_table_inner_client),
            buffer,
            slot_start_from: config.slot_start_from,
            slot_parse_until: config.get_slot_until(),
        })
    }

    pub async fn start_backfill(
        &self,
        tasks: Arc<Mutex<JoinSet<core::result::Result<(), JoinError>>>>,
        keep_running: Arc<AtomicBool>,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> Result<(), IngesterError> {
        info!("Backfiller is started");

        let slots_collector = SlotsCollector::new(
            self.rocks_client.clone(),
            self.big_table_inner_client.clone(),
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
                self.big_table_client.clone(),
                Arc::new(DirectBlockParser {
                    buffer: self.buffer.clone(),
                    metrics: metrics.clone(),
                }),
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
pub struct TransactionsParser<C: BlockConsumer + Send + Clone> {
    rocks_client: Arc<rocks_db::Storage>,
    big_table_client: Arc<LedgerStorage>,
    consumer: Arc<C>,
}

impl<C> TransactionsParser<C>
where
    C: BlockConsumer + Send + Clone + Sync + 'static,
{
    pub async fn new(
        rocks_client: Arc<rocks_db::Storage>,
        big_table_client: Arc<LedgerStorage>,
        consumer: Arc<C>,
    ) -> TransactionsParser<C> {
        TransactionsParser {
            rocks_client,
            big_table_client,
            consumer,
        }
    }

    pub async fn parse_transactions(&self, keep_running: Arc<AtomicBool>) {
        let mut counter = GET_SLOT_RETRIES;

        'outer: while keep_running.load(Ordering::SeqCst) {
            let mut slots_to_parse_iter = self.rocks_client.bubblegum_slots.iter_end();

            let mut slots_to_parse_vec = Vec::new();

            while slots_to_parse_vec.len() <= BATCH_SLOTS_TO_PARSE {
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

            let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

            for slot in slots_to_parse_vec.iter() {
                let bg_client = self.big_table_client.clone();
                let s = *slot;
                let c = self.consumer.clone();
                let task = tokio::spawn(async move {
                    let block = match get_block_from_bg(bg_client, s).await {
                        Ok(block) => block,
                        Err(err) => {
                            error!("Error getting block: {}", err);
                            return;
                        }
                    };
                    c.consume_block(block).await;
                });

                tasks.push(task);
            }

            for task in tasks {
                match task.await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Task for parsing slots was failed: {}", err);
                    }
                };
            }

            let mut counter = DELETE_SLOT_RETRIES;

            while counter > 0 {
                let delete_result = self
                    .rocks_client
                    .bubblegum_slots
                    .delete_batch(
                        slots_to_parse_vec
                            .iter()
                            .map(|k| form_bubblegum_slots_key(*k))
                            .collect(),
                    )
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
    }
}

#[derive(Clone)]
pub struct DirectBlockParser {
    buffer: Arc<Buffer>,
    metrics: Arc<BackfillerMetricsConfig>,
}

#[async_trait]
impl BlockConsumer for DirectBlockParser {
    async fn consume_block(&self, block: solana_transaction_status::ConfirmedBlock) {
        for tx in block.transactions.iter() {
            let meta = if let Some(meta) = tx.get_status_meta() {
                if let Err(_err) = meta.status {
                    continue;
                }
                meta
            } else {
                error!("Unexpected, EncodedTransactionWithStatusMeta struct has no metadata");
                continue;
            };

            let decoded_tx = tx.get_transaction();
            let sig = decoded_tx.signatures[0].to_string();
            let msg = decoded_tx.message;
            let atl_keys = msg.address_table_lookups();

            let account_keys = msg.static_account_keys();
            let account_keys = {
                let mut account_keys_vec = vec![];
                for key in account_keys.iter() {
                    account_keys_vec.push(key.to_bytes());
                }
                if atl_keys.is_some() {
                    let ad = meta.loaded_addresses;

                    for i in &ad.writable {
                        account_keys_vec.push(i.to_bytes());
                    }

                    for i in &ad.readonly {
                        account_keys_vec.push(i.to_bytes());
                    }
                }
                account_keys_vec
            };

            let bubblegum = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID.to_bytes();
            if account_keys.iter().all(|pk| *pk != bubblegum) {
                continue;
            }

            let builder = FlatBufferBuilder::new();
            debug!("Serializing transaction in backfiller {}", sig);

            let encoded_tx = match tx
                .clone()
                .encode(UiTransactionEncoding::Base58, Some(0), false)
            {
                Ok(tx) => tx,
                Err(err) => {
                    error!("Error encoding transaction: {}", err);
                    continue;
                }
            };

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

            let mut d = self.buffer.transactions.lock().await;

            d.push_back(BufferedTransaction {
                transaction: tx,
                map_flatbuffer: false,
            });

            self.metrics.inc_data_processed("backfiller_tx_processed");
        }
        self.metrics.inc_data_processed("backfiller_slot_processed");

        self.metrics
            .set_last_processed_slot("parsed_slot", block.parent_slot as i64);
    }
}
pub async fn get_block_from_bg(
    big_table_client: Arc<LedgerStorage>,
    slot: u64,
) -> Result<solana_transaction_status::ConfirmedBlock, IngesterError> {
    let mut counter = GET_DATA_FROM_BG_RETRIES;

    loop {
        let block = match big_table_client.get_confirmed_block(slot).await {
            Ok(block) => block,
            Err(err) => {
                error!("Error getting block: {}", err);
                counter -= 1;
                if counter == 0 {
                    return Err(IngesterError::BigTableError(format!(
                        "Error getting block: {}",
                        err
                    )));
                }
                tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG)).await;
                continue;
            }
        };

        return Ok(block);
    }
}
