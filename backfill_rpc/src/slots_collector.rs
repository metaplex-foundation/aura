use crate::rpc::BackfillRPC;
use async_trait::async_trait;
use interface::error::UsecaseError;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedTransaction, TransactionDetails, UiConfirmedBlock};
use std::str::FromStr;
use usecase::slots_collector::SlotsGetter;

#[async_trait]
impl SlotsGetter for BackfillRPC {
    async fn get_slots(
        &self,
        collected_key: &solana_program::pubkey::Pubkey,
        start_at: u64,
        _rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        let block_with_start_signature = self
            .client
            .get_block_with_config(
                start_at,
                RpcBlockConfig {
                    encoding: None,
                    transaction_details: Some(TransactionDetails::Accounts),
                    rewards: Some(false),
                    commitment: Some(CommitmentConfig {
                        commitment: CommitmentLevel::Confirmed,
                    }),
                    max_supported_transaction_version: Some(u8::MAX),
                },
            )
            .await
            .map_err(|e| UsecaseError::SolanaRPC(e.to_string()))?;
        let signature = fetch_related_signature(collected_key, block_with_start_signature);

        Ok(self
            .get_signatures_by_address(
                None,
                signature.and_then(|s| Signature::from_str(&s).ok()),
                collected_key,
            )
            .await?
            .iter()
            .map(|s| s.slot)
            .collect::<Vec<_>>())
    }
}

fn fetch_related_signature(
    collected_key: &solana_program::pubkey::Pubkey,
    block_with_start_signature: UiConfirmedBlock,
) -> Option<String> {
    let Some(txs) = block_with_start_signature.transactions else {
        return None;
    };
    for tx in txs {
        if let EncodedTransaction::Accounts(accounts_list) = tx.transaction {
            if accounts_list
                .account_keys
                .iter()
                .any(|a| a.pubkey == collected_key.to_string())
                && !accounts_list.signatures.is_empty()
            {
                return Some(accounts_list.signatures[0].clone());
            }
        }
    }
    None
}
