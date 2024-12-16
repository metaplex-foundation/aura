use super::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor;
use crate::error::IngesterError;
use crate::redis_receiver::get_timestamp_from_id;
use chrono::Utc;
use interface::signature_persistence::UnprocessedTransactionsGetter;
use metrics_utils::MessageProcessMetricsConfig;
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
    message_process_metrics: Option<Arc<MessageProcessMetricsConfig>>,
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
                    .process_transaction(tx.tx, false)
                    .await
                {
                    Ok(_) => {
                        if let Some(message_process_metrics) = &message_process_metrics {
                            if let Some(message_timestamp) = get_timestamp_from_id(&tx.id) {
                                let current_timestamp = Utc::now().timestamp_millis() as u64;

                                message_process_metrics.set_data_read_time(
                                    "transactions",
                                    current_timestamp
                                        .checked_sub(message_timestamp)
                                        .unwrap_or_default()
                                        as f64,
                                );
                            }
                        }

                        unprocessed_transactions_getter.ack(tx.id)
                    }
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
