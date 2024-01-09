use std::sync::Arc;

use interface::{
    error::StorageError,
    signature_persistence::{SignaturePersistence, TransactionIngester},
    solana_rpc::{GetSignaturesByAddress, GetTransactionsBySignatures, SignatureWithSlot},
};
use solana_sdk::pubkey::Pubkey;

pub struct SignatureFetcher<T>
where
    T: GetSignaturesByAddress + GetTransactionsBySignatures,
{
    pub data_layer: Arc<dyn SignaturePersistence>,
    pub rpc: Arc<T>,
    pub ingester: Arc<dyn TransactionIngester>,
}

const BATCH_SIZE: usize = 1000;

impl<T> SignatureFetcher<T>
where
    T: GetSignaturesByAddress + GetTransactionsBySignatures,
{
    pub async fn fetch_signatures(&self, program_id: Pubkey) -> Result<(), StorageError> {
        let signature = self
            .data_layer
            .first_persisted_signature_for(program_id)
            .await?;
        if signature.is_none() {
            return Ok(());
        }
        let signature = signature.unwrap();
        let mut all_signatures = self
            .rpc
            .get_signatures_by_address(signature.signature, program_id)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?;
        
        if all_signatures.is_empty() {
            return Ok(());
        }
        // we've got a list of signatures, potentially a huge one (10s of millions)
        // we need to filter out the ones we already have and ingest the rest, if any
        // we need to sort them and process in batches, sorting is ascending by the slot
        all_signatures.sort_by(|a, b| a.slot.cmp(&b.slot));
        // we need to split the list into batches of BATCH_SIZE 

        let mut batch_start = 0;
        while batch_start < all_signatures.len() {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, all_signatures.len());
            let batch = &all_signatures[batch_start..batch_end];
            let missing_signatures = self
                .data_layer
                .missing_signatures(program_id, batch.to_vec())
                .await?;
            if missing_signatures.is_empty() {
                continue;
            }
            tracing::debug!(
                "Found {} missing signatures for program {}. Fetching details...",
                missing_signatures.len(),
                program_id
            );
            let transactions: Vec<entities::models::BufferedTransaction> = self
                .rpc
                .get_txs_by_signatures(missing_signatures.iter().map(|s| s.signature).collect())
                .await
                .map_err(|e| StorageError::Common(e.to_string()))?;
            let tx_cnt = transactions.len();
            for transaction in transactions {
                self.ingester
                    .ingest_transaction(transaction)
                    .await?
            }
            // now we may drop the old signatures before the last element of the batch
            // we do this by constructing a fake key at the start of the same slot
            // and then dropping all signatures before it
            
            let fake_key = SignatureWithSlot {
                signature: Default::default(),
                slot: all_signatures[batch_end - 1].slot,
            };
            tracing::info!("Ingested {} transactions. Dropping signatures for program {} before slot {}.", tx_cnt, program_id, fake_key.slot);
            self.data_layer
                .drop_signatures_before(program_id, fake_key)
                .await?;
            
            batch_start = batch_end;
        }
        let fake_key = SignatureWithSlot {
            signature: Default::default(),
            slot: all_signatures[all_signatures.len() - 1].slot,
        };
        
        tracing::info!("Finished fetching signatures for program {}. Dropping signatures before slot {}.", program_id, fake_key.slot);
        self.data_layer
            .drop_signatures_before(program_id, fake_key)
            .await?;
        Ok(())
    }
}
