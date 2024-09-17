use crate::bubblegum_updates_processor::BubblegumTxProcessor;
use crate::error::IngesterError;
use interface::signature_persistence::UnprocessedTransactionsGetter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::time::sleep as tokio_sleep;
use tracing::error;

const TRANSACTIONS_GETTER_IDLE_TIMEOUT_MILLIS: u64 = 250;

pub async fn run_transaction_processor<TG>(
    rx: Receiver<()>,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    unprocessed_transactions_getter: Arc<TG>,
    geyser_bubblegum_updates_processor: Arc<BubblegumTxProcessor>,
) where
    TG: UnprocessedTransactionsGetter + Send + Sync + 'static,
{
    let run_transaction_processor = async move {
        while rx.is_empty() {
            let txs = match unprocessed_transactions_getter.next_transactions().await {
                Ok(txs) => txs,
                Err(err) => {
                    error!("Get next transactions: {}", err);
                    tokio_sleep(Duration::from_millis(
                        TRANSACTIONS_GETTER_IDLE_TIMEOUT_MILLIS,
                    ))
                    .await;
                    continue;
                }
            };

            for tx in txs {
                match geyser_bubblegum_updates_processor
                    .process_transaction(tx.tx)
                    .await
                {
                    Ok(_) => unprocessed_transactions_getter.ack(tx.id),
                    Err(IngesterError::NotImplemented) => {}
                    Err(err) => {
                        error!("Background saver could not process received data: {}", err);
                    }
                }
            }
        }

        Ok(())
    };

    mutexed_tasks.lock().await.spawn(run_transaction_processor);
}
