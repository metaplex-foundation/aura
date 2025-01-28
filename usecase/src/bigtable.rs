use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use interface::{
    error::{StorageError, UsecaseError},
    signature_persistence::BlockProducer,
};
use solana_bigtable_connection::{bigtable::BigTableConnection, CredentialType};
use solana_storage_bigtable::{LedgerStorage, DEFAULT_APP_PROFILE_ID, DEFAULT_INSTANCE_NAME};
use solana_transaction_status::{
    BlockEncodingOptions, EncodedTransactionWithStatusMeta, TransactionDetails,
    TransactionWithStatusMeta,
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
        let big_table_client = LedgerStorage::new(
            true,
            Some(Duration::from_secs(big_table_timeout as u64)),
            Some(big_table_creds.to_string()),
        )
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
    ) -> Result<solana_transaction_status::UiConfirmedBlock, StorageError> {
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

            let encoded: solana_transaction_status::UiConfirmedBlock = block
                .clone()
                .encode_with_options(
                    solana_transaction_status::UiTransactionEncoding::Base58,
                    BlockEncodingOptions {
                        transaction_details: TransactionDetails::Full,
                        show_rewards: false,
                        max_supported_transaction_version: Some(u8::MAX),
                    },
                )
                .map_err(|e| StorageError::Common(e.to_string()))?;
            return Ok(encoded);
        }
    }
}
impl BigTableClient {
    pub async fn get_blocks(
        &self,
        slots: &[u64],
    ) -> Result<Vec<(u64, solana_transaction_status::UiConfirmedBlock)>, StorageError> {
        let mut counter = GET_DATA_FROM_BG_RETRIES;

        loop {
            let blocks_iter = match self
                .big_table_client
                .get_confirmed_blocks_with_data(slots)
                .await
            {
                Ok(blocks_iter) => blocks_iter, // `blocks_iter` implements Iterator<Item = (u64, ConfirmedBlock)>
                Err(err) => {
                    warn!("Error getting blocks: {}, retrying", err);
                    counter -= 1;
                    if counter == 0 {
                        return Err(StorageError::Common(format!("Error getting block: {}", err)));
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                },
            };

            // For each `(slot, block)` we’ll transform it into
            //   Result<(u64, UiConfirmedBlock), StorageError>
            let encoded_blocks_result = blocks_iter
                .map(
                    |(slot, mut block)| -> Result<
                        (u64, solana_transaction_status::UiConfirmedBlock),
                        StorageError,
                    > {
                        // Filter out any transactions that are not bubblegum.
                        block.transactions.retain(is_bubblegum_transaction);

                        // Attempt to encode the block. If it fails, return Err(...) here,
                        // which causes the entire collect() to fail.
                        let encoded = block
                            .clone()
                            .encode_with_options(
                                solana_transaction_status::UiTransactionEncoding::Base58,
                                BlockEncodingOptions {
                                    transaction_details: TransactionDetails::Full,
                                    show_rewards: false,
                                    max_supported_transaction_version: Some(u8::MAX),
                                },
                            )
                            .map_err(|e| StorageError::Common(e.to_string()))?;

                        Ok((slot, encoded))
                    },
                )
                // Collect all Ok(...) items into a Vec, or return the first Err(...) we encounter.
                .collect::<Result<Vec<_>, StorageError>>();

            // If encoding any block failed, you might want to *retry* the entire request
            // or just bubble up the error. Here we bubble it up:
            match encoded_blocks_result {
                Ok(blocks) => {
                    // Succeeded encoding every block; return immediately.
                    return Ok(blocks);
                },
                Err(e) => {
                    warn!("Error encoding a block: {}", e);
                    return Err(e);
                },
            }
        }
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
