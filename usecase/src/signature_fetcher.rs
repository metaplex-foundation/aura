use std::{str::FromStr, sync::Arc};

use async_recursion::async_recursion;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use interface::{
    error::StorageError,
    signature_persistence::{SignaturePersistence, TransactionIngester},
    solana_rpc::TransactionsGetter,
};
use metrics_utils::{MetricStatus, RpcBackfillerMetricsConfig};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio_util::sync::CancellationToken;
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
const DOWNLOAD_TX_RETRY: u32 = 5;

impl<T, SP, TI> SignatureFetcher<T, SP, TI>
where
    T: TransactionsGetter + 'static,
    SP: SignaturePersistence,
    TI: TransactionIngester,
{
    pub fn new(
        data_layer: Arc<SP>,
        rpc: Arc<T>,
        ingester: Arc<TI>,
        metrics: Arc<RpcBackfillerMetricsConfig>,
    ) -> Self {
        Self { data_layer, rpc, ingester, metrics }
    }

    pub async fn fetch_signatures(
        &self,
        program_id: Pubkey,
        rpc_retry_interval_millis: u64,
        cancellation_token: CancellationToken,
    ) -> Result<(), StorageError> {
        let signature = cancellation_token
            .run_until_cancelled(self.data_layer.first_persisted_signature_for(program_id))
            .await
            .unwrap_or(Ok(None))?;
        if signature.is_none() {
            return Ok(());
        }
        let signature = signature.unwrap();
        info!("Start fetching signatures...");
        let mut all_signatures = cancellation_token
            .run_until_cancelled(async move {
                match self
                    .rpc
                    .get_signatures_by_address(signature, program_id)
                    .await
                    .map_err(|e| StorageError::Common(e.to_string()))
                {
                    Ok(all_signatures) => {
                        self.metrics.inc_fetch_signatures(
                            "get_signatures_by_address",
                            MetricStatus::SUCCESS,
                        );
                        Ok(all_signatures)
                    },
                    Err(e) => {
                        self.metrics.inc_fetch_signatures(
                            "get_signatures_by_address",
                            MetricStatus::FAILURE,
                        );
                        Err(e)
                    },
                }
            })
            .await
            .unwrap_or_else(|| Ok(vec![]))?;

        if all_signatures.is_empty() {
            return Ok(());
        }
        info!(
            "Fetched {} signatures since first persisted signature {}",
            &all_signatures.len(),
            signature.signature
        );

        // we've got a list of signatures, potentially a huge one (10s of millions)
        // we need to filter out the ones we already have and ingest the rest, if any
        // we need to sort them and process in batches, sorting is ascending by the slot
        all_signatures.sort_by(|a, b| a.slot.cmp(&b.slot));
        // we need to split the list into batches of BATCH_SIZE

        // this variable is required to account for the last batch potentially being smaller than
        // the rest
        let mut last_batch_index = 0;

        for signatures in all_signatures.chunks(BATCH_SIZE) {
            if cancellation_token.is_cancelled() {
                break;
            }
            let signatures_len = signatures.len();
            let missing_signatures = cancellation_token
                .run_until_cancelled(
                    self.data_layer.missing_signatures(program_id, signatures.to_vec()),
                )
                .await
                .unwrap_or_else(|| Ok(vec![]))?;
            if missing_signatures.is_empty() {
                continue;
            }
            info!(
                "Found {} missing signatures for program {}. Fetching details...",
                missing_signatures.len(),
                program_id
            );
            let signatures: Vec<Signature> =
                missing_signatures.iter().map(|s| s.signature).collect();
            let tx_cnt = signatures.len();
            let counter = 0;

            Self::process_transactions(
                self.rpc.clone(),
                self.ingester.clone(),
                self.metrics.clone(),
                signatures,
                rpc_retry_interval_millis,
                counter,
            )
            .await?;

            // now we may drop the old signatures before the last element of the batch
            // we do this by constructing a fake key at the start of the same slot
            // and then dropping all signatures before it

            let fake_key = SignatureWithSlot {
                signature: Default::default(),
                slot: all_signatures[last_batch_index + signatures_len - 1].slot,
            };
            info!(
                "Ingested {} transactions. Dropping signatures for program {} before slot {}.",
                tx_cnt, program_id, fake_key.slot
            );
            self.data_layer.drop_signatures_before(program_id, fake_key).await?;
            last_batch_index += signatures_len;
        }

        if !cancellation_token.is_cancelled() {
            let fake_key = SignatureWithSlot {
                signature: Default::default(),
                slot: all_signatures[all_signatures.len() - 1].slot,
            };

            info!(
                "Finished fetching signatures for program {}. Dropping signatures before slot {}.",
                program_id, fake_key.slot
            );
            self.data_layer.drop_signatures_before(program_id, fake_key).await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_transactions(
        rpc: Arc<T>,
        ingester: Arc<TI>,
        metrics: Arc<RpcBackfillerMetricsConfig>,
        signatures: Vec<Signature>,
        rpc_retry_interval_millis: u64,
        counter: u32,
    ) -> Result<(), StorageError> {
        // we need to check recursion depth not to fall in infinite loop with failed transactions
        if counter > DOWNLOAD_TX_RETRY {
            return Err(StorageError::Common(
                "Fetch transaction recursion reach it's maximum".to_string(),
            ));
        }

        let transactions: Vec<BufferedTransaction> = match rpc
            .get_txs_by_signatures(signatures, rpc_retry_interval_millis)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
        {
            Ok(transactions) => {
                metrics.inc_fetch_transactions("get_txs_by_signatures", MetricStatus::SUCCESS);
                transactions
            },
            Err(e) => {
                metrics.inc_fetch_transactions("get_txs_by_signatures", MetricStatus::FAILURE);
                return Err(e);
            },
        };

        let mut failed_tx_signatures = vec![];

        for transaction in transactions {
            match ingester.ingest_transaction(transaction.clone()).await {
                Ok(_) => {
                    metrics.inc_transactions_processed("ingest_transaction", MetricStatus::SUCCESS);
                },
                Err(_) => {
                    metrics.inc_transactions_processed("ingest_transaction", MetricStatus::FAILURE);

                    // deserialize to get signature from there
                    let transaction_info = plerkle_serialization::root_as_transaction_info(
                        transaction.transaction.as_slice(),
                    )
                    .map_err(|e| StorageError::Common(e.to_string()))?;

                    let signature = Signature::from_str(transaction_info.signature().ok_or(
                        StorageError::Common("No signature found in transaction".to_string()),
                    )?)
                    .map_err(|e| StorageError::Common(e.to_string()))?;

                    failed_tx_signatures.push(signature);
                },
            };
        }

        if !failed_tx_signatures.is_empty() {
            Self::process_transactions(
                rpc,
                ingester,
                metrics,
                failed_tx_signatures,
                rpc_retry_interval_millis,
                counter + 1,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use interface::{
        signature_persistence::{MockSignaturePersistence, MockTransactionIngester},
        solana_rpc::MockTransactionsGetter,
    };
    use metrics_utils::RpcBackfillerMetricsConfig;
    use mockall::predicate::{self, eq};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_fetch_signatures_with_over_batch_limit_elements_should_complete_without_infinite_loop(
    ) {
        let mut data_layer = MockSignaturePersistence::new();
        let mut rpc = MockTransactionsGetter::new();
        let ingester = MockTransactionIngester::new();
        let metrics = Arc::new(RpcBackfillerMetricsConfig::new());

        let program_id = solana_sdk::pubkey::new_rand();
        let signature = solana_sdk::signature::Signature::from([1u8; 64]);
        let slot = 1;
        let signature_with_slot = super::SignatureWithSlot { signature, slot };
        let signatures = vec![signature_with_slot.clone(); 2000];
        let sig_clone = signature_with_slot.clone();

        data_layer
            .expect_first_persisted_signature_for()
            .with(eq(program_id))
            .times(1)
            .return_once(move |_| Ok(Some(sig_clone)));
        rpc.expect_get_signatures_by_address()
            .with(eq(signature_with_slot), eq(program_id))
            .times(1)
            .return_once(move |_, _| Ok(signatures));
        data_layer
            .expect_missing_signatures()
            .with(eq(program_id), predicate::always())
            .times(2)
            .returning(move |_, _| Ok(vec![]));
        data_layer
            .expect_drop_signatures_before()
            .with(eq(program_id), predicate::always())
            .times(1)
            .returning(move |_, _| Ok(()));
        let fetcher = super::SignatureFetcher::new(
            Arc::new(data_layer),
            Arc::new(rpc),
            Arc::new(ingester),
            metrics,
        );
        fetcher.fetch_signatures(program_id, 0, CancellationToken::new()).await.unwrap();
    }
}
