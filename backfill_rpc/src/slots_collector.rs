use crate::rpc::BackfillRPC;
use async_trait::async_trait;
use interface::error::UsecaseError;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedTransaction, TransactionDetails, UiConfirmedBlock};
use std::collections::HashSet;
use std::str::FromStr;
use usecase::slots_collector::SlotsGetter;

const TRY_SKIPPED_BLOCKS_COUNT: u64 = 25;

#[async_trait]
impl SlotsGetter for BackfillRPC {
    async fn get_slots(
        &self,
        collected_key: &solana_program::pubkey::Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        let block_with_start_signature = self.try_get_block(start_at).await?;
        let signature = fetch_related_signature(collected_key, block_with_start_signature);
        let mut slots = HashSet::new();
        let mut before = signature.and_then(|s| Signature::from_str(&s).ok());
        loop {
            let signatures = self
                .get_signatures_by_address(None, before, collected_key)
                .await?;
            if signatures.is_empty() {
                break;
            }
            let last = signatures.last().unwrap();

            signatures.iter().for_each(|sig| {
                slots.insert(sig.slot);
            });
            before = Some(last.signature);
            if slots.len() >= rows_limit as usize {
                break;
            }
        }

        Ok(Vec::from_iter(slots))
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
        if tx.meta.and_then(|meta| meta.err).is_some() {
            continue;
        }
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

impl BackfillRPC {
    async fn try_get_block(&self, start_at: u64) -> Result<UiConfirmedBlock, UsecaseError> {
        for _ in (start_at - TRY_SKIPPED_BLOCKS_COUNT..start_at).rev() {
            if let Ok(block) = self
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
            {
                return Ok(block);
            }
        }

        Err(UsecaseError::SolanaRPC("Block not found".to_string()))
    }
}

#[cfg(feature = "rpc_tests")]
#[tokio::test]
async fn test_rpc_get_slots() {
    use solana_program::pubkey::Pubkey;

    let client = BackfillRPC::connect("https://api.mainnet-beta.solana.com".to_string());
    let slots = client
        .get_slots(
            &Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap(),
            253484000,
            2,
        )
        .await
        .unwrap();

    assert_eq!(slots.is_empty(), false)
}