use std::sync::Arc;

use entities::models::{BufferedTransaction, SignatureWithSlot};
use interface::{
    error::StorageError,
    signature_persistence::{SignaturePersistence, TransactionIngester},
    solana_rpc::TransactionsGetter,
};
use metrics_utils::{MetricStatus, RpcBackfillerMetricsConfig};
use solana_sdk::pubkey::Pubkey;
use tracing::info;

pub struct SignatureFetcher<T, SP, TI>
where
    T: TransactionsGetter,
    SP: SignaturePersistence,
    TI: TransactionIngester,
{
    pub data_layer: Arc<SP>,
    pub rpc: Arc<T>,
    pub ingester: Arc<TI>,
    pub metrics: Arc<RpcBackfillerMetricsConfig>,
}

const BATCH_SIZE: usize = 1000;

impl<T, SP, TI> SignatureFetcher<T, SP, TI>
where
    T: TransactionsGetter,
    SP: SignaturePersistence,
    TI: TransactionIngester,
{
    pub fn new(
        data_layer: Arc<SP>,
        rpc: Arc<T>,
        ingester: Arc<TI>,
        metrics: Arc<RpcBackfillerMetricsConfig>,
    ) -> Self {
        Self {
            data_layer,
            rpc,
            ingester,
            metrics,
        }
    }

    pub async fn fetch_signatures(&self, program_id: Pubkey) -> Result<(), StorageError> {
        let signature = self
            .data_layer
            .first_persisted_signature_for(program_id)
            .await?;
        if signature.is_none() {
            return Ok(());
        }
        let signature = signature.unwrap();
        info!("Start fetching signatures...");
        let mut all_signatures = match self
            .rpc
            .get_signatures_by_address(signature, program_id)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
        {
            Ok(all_signatures) => {
                self.metrics
                    .inc_fetch_signatures("get_signatures_by_address", MetricStatus::SUCCESS);
                all_signatures
            }
            Err(e) => {
                self.metrics
                    .inc_fetch_signatures("get_signatures_by_address", MetricStatus::FAILURE);
                return Err(e);
            }
        };

        if all_signatures.is_empty() {
            return Ok(());
        }
        info!(
            "Fetched {} signatures since first persisted signature",
            &all_signatures.len()
        );

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
            info!(
                "Found {} missing signatures for program {}. Fetching details...",
                missing_signatures.len(),
                program_id
            );
            let transactions: Vec<BufferedTransaction> = match self
                .rpc
                .get_txs_by_signatures(missing_signatures.iter().map(|s| s.signature).collect())
                .await
                .map_err(|e| StorageError::Common(e.to_string()))
            {
                Ok(transactions) => {
                    self.metrics
                        .inc_fetch_transactions("get_txs_by_signatures", MetricStatus::SUCCESS);
                    transactions
                }
                Err(e) => {
                    self.metrics
                        .inc_fetch_transactions("get_txs_by_signatures", MetricStatus::FAILURE);
                    return Err(e);
                }
            };
            let tx_cnt = transactions.len();
            for transaction in transactions {
                match self.ingester.ingest_transaction(transaction).await {
                    Ok(_) => {
                        self.metrics.inc_transactions_processed(
                            "ingest_transaction",
                            MetricStatus::SUCCESS,
                        );
                    }
                    Err(e) => {
                        self.metrics.inc_transactions_processed(
                            "ingest_transaction",
                            MetricStatus::FAILURE,
                        );
                        return Err(e);
                    }
                };
            }
            // now we may drop the old signatures before the last element of the batch
            // we do this by constructing a fake key at the start of the same slot
            // and then dropping all signatures before it

            let fake_key = SignatureWithSlot {
                signature: Default::default(),
                slot: all_signatures[batch_end - 1].slot,
            };
            info!(
                "Ingested {} transactions. Dropping signatures for program {} before slot {}.",
                tx_cnt, program_id, fake_key.slot
            );
            self.data_layer
                .drop_signatures_before(program_id, fake_key)
                .await?;

            batch_start = batch_end;
        }
        let fake_key = SignatureWithSlot {
            signature: Default::default(),
            slot: all_signatures[all_signatures.len() - 1].slot,
        };

        info!(
            "Finished fetching signatures for program {}. Dropping signatures before slot {}.",
            program_id, fake_key.slot
        );
        self.data_layer
            .drop_signatures_before(program_id, fake_key)
            .await?;
        Ok(())
    }
}
