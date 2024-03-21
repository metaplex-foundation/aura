use crate::rpc::{BackfillRPC, GET_TX_RETRIES};
use async_trait::async_trait;
use interface::error::StorageError;
use interface::signature_persistence::BlockProducer;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock};
use std::time::Duration;
use tracing::error;
use usecase::bigtable::is_bubblegum_transaction_encoded;

const SECONDS_TO_RETRY_GET_BLOCK: u64 = 5;

#[async_trait]
impl BlockProducer for BackfillRPC {
    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, StorageError> {
        let mut counter = GET_TX_RETRIES;

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
                        return Err(StorageError::Common(format!(
                            "Error getting block: {}",
                            err
                        )));
                    }
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_BLOCK)).await;
                    continue;
                }
            };
            if let Some(ref mut txs) = encoded_block.transactions {
                txs.retain(is_bubblegum_transaction_encoded);
            }

            return Ok(encoded_block);
        }
    }
}