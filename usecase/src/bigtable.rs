use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use entities::models::{RawBlockWithTransactions, TransactionInfo};
use futures::{stream, StreamExt, TryStreamExt};
use interface::{
    error::{StorageError, UsecaseError},
    signature_persistence::BlockProducer,
};
use solana_bigtable_connection::{bigtable::BigTableConnection, CredentialType};
use solana_storage_bigtable::{LedgerStorage, DEFAULT_APP_PROFILE_ID, DEFAULT_INSTANCE_NAME};
use solana_transaction_status::{
    ConfirmedBlock, EncodedTransactionWithStatusMeta, TransactionWithStatusMeta,
};
use tracing::{error, warn};

pub const GET_DATA_FROM_BG_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_GET_DATA_FROM_BG: u64 = 2;

pub struct BigTableClient {
    pub big_table_client: Arc<LedgerStorage>,
    pub big_table_inner_client: Arc<BigTableConnection>,
}

impl BigTableClient {
    pub fn new(
        big_table_client: Arc<LedgerStorage>,
        big_table_inner_client: Arc<BigTableConnection>,
    ) -> Self {
        Self { big_table_client, big_table_inner_client }
    }

    pub async fn connect_new_with(
        big_table_creds: String,
        big_table_timeout: u32,
    ) -> Result<Self, UsecaseError> {
        let big_table_client =
            LedgerStorage::new_with_config(solana_storage_bigtable::LedgerStorageConfig {
                read_only: true,
                timeout: Some(Duration::from_secs(big_table_timeout as u64)),
                credential_type: solana_storage_bigtable::CredentialType::Filepath(Some(
                    big_table_creds.to_string(),
                )),
                max_message_size: 1024 * 1024 * 1024, // 1 * 1024 * 1024 * 1024
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            })
            .await?;

        let big_table_inner_client = BigTableConnection::new(
            DEFAULT_INSTANCE_NAME,
            DEFAULT_APP_PROFILE_ID,
            true,
            None,
            CredentialType::Filepath(Some(big_table_creds)),
        )
        .await
        .unwrap();

        Ok(Self::new(Arc::new(big_table_client), Arc::new(big_table_inner_client)))
    }
}

#[async_trait]
impl BlockProducer for BigTableClient {
    async fn get_block(
        &self,
        slot: u64,
        _backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<RawBlockWithTransactions, StorageError> {
        let mut counter = GET_DATA_FROM_BG_RETRIES;

        loop {
            let mut block = match self.big_table_client.get_confirmed_block(slot).await {
                Ok(block) => block,
                Err(err) => {
                    // as this will be retried we're logging as warn. If the error persists, it will be returned as error
                    warn!("Error getting block: {}, retrying", err);
                    counter -= 1;
                    if counter == 0 {
                        return Err(StorageError::Common(format!("Error getting block: {}", err)));
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                },
            };
            block.transactions.retain(is_bubblegum_transaction);

            return Ok(confirmed_block_to_raw(slot, block));
        }
    }
}
fn slot_to_key(slot: u64) -> solana_bigtable_connection::bigtable::RowKey {
    format!("{slot:016x}")
}
// Reverse of `slot_to_key`
fn key_to_slot(key: &str) -> Option<u64> {
    match solana_program::clock::Slot::from_str_radix(key, 16) {
        Ok(slot) => Some(slot),
        Err(err) => {
            // bucket data is probably corrupt
            warn!("Failed to parse object key as a slot: {}: {}", key, err);
            None
        },
    }
}

pub async fn get_blocks(
    connection: &BigTableConnection,
    slots: &[u64],
) -> Result<Vec<(u64, RawBlockWithTransactions)>, StorageError> {
    // 1) Fetch raw data from Bigtable (same as before)
    let row_keys = slots.iter().map(|slot| slot_to_key(*slot)).collect::<Vec<_>>();
    let mut client = connection.client();

    let rows = client
        .get_multi_row_data("blocks", &row_keys)
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?;

    // 2) Turn the Vec of (row_key, Vec<(colName, bytes)>) into a stream
    //    so we can process them in parallel.
    let concurrency_limit = 4; // tweak as appropriate

    let results_stream = stream::iter(rows)
        .map(|(row_key, block_cell_data)| async move {
            // We'll do all the CPU-heavy steps in this async block.
            // If you want to run truly CPU-bound work on a dedicated thread,
            // you could also wrap each step in `spawn_blocking`.

            // Convert row_key -> slot
            let slot = key_to_slot(&row_key).ok_or_else(|| {
                StorageError::Common(format!("Failed to parse slot from key: {row_key}"))
            })?;

            // Find the "proto" column
            let (_, proto_bytes) =
                block_cell_data.into_iter().find(|(name, _)| name == "proto").ok_or_else(|| {
                    StorageError::Common(format!("Missing 'proto' column for slot={slot}"))
                })?;

            // Decompress
            let data =
                solana_bigtable_connection::compression::decompress(&proto_bytes).map_err(|e| {
                    StorageError::Common(format!("Decompress failed for slot={slot}: {e}"))
                })?;

            // Decode Protobuf -> ConfirmedBlock
            let block_proto: solana_storage_proto::convert::generated::ConfirmedBlock =
                prost::Message::decode(&data[..]).map_err(|e| {
                    StorageError::Common(format!("Protobuf decode failed for slot={slot}: {e}"))
                })?;

            let confirmed_block: ConfirmedBlock = block_proto.try_into().map_err(|e| {
                StorageError::Common(format!("try_into failed for slot={slot}: {e}"))
            })?;

            // If we get here, success
            Ok::<_, StorageError>((slot, confirmed_block_to_raw(slot, confirmed_block)))
        })
        // 3) Run up to `concurrency_limit` tasks in parallel.
        .buffer_unordered(concurrency_limit);

    // 4) Collect the stream of Results into a single Result<Vec<_>, _>
    let blocks = results_stream.try_collect().await?; // if any item is Err(..), we fail fast

    Ok(blocks)
}

fn confirmed_block_to_raw(
    slot: u64,
    mut confirmed_block: ConfirmedBlock,
) -> RawBlockWithTransactions {
    // Filter out non-bubblegum transactions
    confirmed_block.transactions.retain(is_bubblegum_transaction);
    let transactions = confirmed_block
        .transactions
        .into_iter()
        .filter_map(|t| TransactionInfo::from_transaction_with_status_meta_and_slot(t, slot))
        .collect();
    RawBlockWithTransactions {
        blockhash: confirmed_block.blockhash,
        parent_slot: confirmed_block.parent_slot,
        previous_blockhash: confirmed_block.previous_blockhash,
        transactions,
        block_time: confirmed_block.block_time.and_then(|t| t.try_into().ok()),
    }
}

fn is_bubblegum_transaction(tx: &TransactionWithStatusMeta) -> bool {
    let meta = if let Some(meta) = tx.get_status_meta() {
        if let Err(_err) = meta.status {
            return false;
        }
        meta
    } else {
        error!("Unexpected, TransactionWithStatusMeta struct has no metadata");
        return false;
    };
    let decoded_tx = tx.get_transaction();
    let msg = decoded_tx.message;
    let atl_keys = msg.address_table_lookups();

    let lookup_key = mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
    if msg.static_account_keys().iter().any(|k| *k == lookup_key) {
        return true;
    }

    if atl_keys.is_some() {
        let ad = meta.loaded_addresses;

        return ad.writable.iter().any(|k| *k == lookup_key)
            || ad.readonly.iter().any(|k| *k == lookup_key);
    }
    false
}

pub fn is_bubblegum_transaction_encoded(tx: &EncodedTransactionWithStatusMeta) -> bool {
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

pub fn is_bubblegum_transaction_from_info(tx: &TransactionInfo) -> bool {
    tx.account_keys.iter().any(|p| *p == mpl_bubblegum::programs::MPL_BUBBLEGUM_ID)
}
