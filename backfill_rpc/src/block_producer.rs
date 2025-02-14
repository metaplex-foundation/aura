use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use entities::{
    models::{RawBlockWithTransactions, TransactionInfo},
    utils::decode_encoded_transaction_with_status_meta,
};
use interface::{error::StorageError, signature_persistence::BlockProducer};
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::TransactionDetails;
use tracing::error;
use usecase::bigtable::is_bubblegum_transaction_encoded;

use crate::rpc::{BackfillRPC, MAX_RPC_RETRIES};

const SECONDS_TO_RETRY_GET_BLOCK: u64 = 5;

#[async_trait]
impl BlockProducer for BackfillRPC {
    async fn get_block(
        &self,
        slot: u64,
        _backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<RawBlockWithTransactions, StorageError> {
        let mut counter = MAX_RPC_RETRIES;

        loop {
            let mut encoded_block = match self
                .client
                .get_block_with_config(
                    slot,
                    RpcBlockConfig {
                        encoding: Some(solana_transaction_status::UiTransactionEncoding::Base58),
                        transaction_details: Some(TransactionDetails::Full),
                        rewards: Some(false),
                        commitment: Some(CommitmentConfig {
                            commitment: CommitmentLevel::Confirmed,
                        }),
                        max_supported_transaction_version: Some(u8::MAX),
                    },
                )
                .await
            {
                Ok(block) => block,
                Err(err) => {
                    error!("Error getting block: {}", err);
                    counter -= 1;
                    if counter == 0 {
                        return Err(StorageError::Common(format!("Error getting block: {}", err)));
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_BLOCK)).await;
                    continue;
                },
            };
            if let Some(ref mut txs) = encoded_block.transactions {
                txs.retain(is_bubblegum_transaction_encoded);
            }
            let raw_block = RawBlockWithTransactions {
                blockhash: encoded_block.blockhash,
                previous_blockhash: encoded_block.previous_blockhash,
                parent_slot: encoded_block.parent_slot,
                transactions: encoded_block
                    .transactions
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|t| {
                        decode_encoded_transaction_with_status_meta(t).and_then(|t| {
                            TransactionInfo::from_transaction_with_status_meta_and_slot(t, slot)
                        })
                    })
                    .collect(),
                block_time: encoded_block.block_time.and_then(|t| t.try_into().ok()),
            };

            return Ok(raw_block);
        }
    }
}
