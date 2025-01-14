use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use entities::{
    enums::UnprocessedAccount,
    models::{BufferedTransaction, BufferedTxWithID, Task, UnprocessedAccountMessage},
};
use interface::{
    error::UsecaseError, signature_persistence::UnprocessedTransactionsGetter,
    unprocessed_data_getter::UnprocessedAccountsGetter,
};
use metrics_utils::IngesterMetricsConfig;
use solana_program::pubkey::Pubkey;
use tokio::{
    sync::{broadcast::Receiver, Mutex},
    task::JoinError,
    time::sleep as tokio_sleep,
};
use tonic::async_trait;
use tracing::log::info;

const TXS_BATCH_SIZE: usize = 100;

pub async fn debug_buffer(
    cloned_rx: Receiver<()>,
    cloned_buffer: Arc<Buffer>,
    cloned_metrics: Arc<IngesterMetricsConfig>,
) -> Result<(), JoinError> {
    while cloned_rx.is_empty() {
        cloned_buffer.debug().await;
        cloned_buffer.capture_metrics(&cloned_metrics).await;
        tokio_sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}

#[derive(Default)]
pub struct Buffer {
    pub transactions: Mutex<VecDeque<BufferedTransaction>>,
    pub json_tasks: Arc<Mutex<VecDeque<Task>>>,
    pub accounts: Mutex<HashMap<Pubkey, UnprocessedAccount>>,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            transactions: Mutex::new(VecDeque::<BufferedTransaction>::new()),
            json_tasks: Arc::new(Mutex::new(VecDeque::<Task>::new())),
            accounts: Mutex::new(HashMap::new()),
        }
    }

    pub async fn debug(&self) {
        info!(
            "\nTransactions buffer: {}\nJson tasks buffer: {}\nAccounts buffer: {}\n",
            self.transactions.lock().await.len(),
            self.json_tasks.lock().await.len(),
            self.accounts.lock().await.len(),
        );
    }

    pub async fn capture_metrics(&self, metrics: &Arc<IngesterMetricsConfig>) {
        metrics.set_buffer("buffer_transactions", self.transactions.lock().await.len() as i64);
        metrics.set_buffer("buffer_json_tasks", self.json_tasks.lock().await.len() as i64);
        metrics.set_buffer("buffer_accounts", self.accounts.lock().await.len() as i64);
    }
}

#[async_trait]
impl UnprocessedTransactionsGetter for Buffer {
    async fn next_transactions(&self) -> Result<Vec<BufferedTxWithID>, UsecaseError> {
        let mut buffer = self.transactions.lock().await;
        let mut result = Vec::with_capacity(TXS_BATCH_SIZE);

        while let Some(tx) = buffer.pop_front() {
            result.push(BufferedTxWithID { tx, id: String::new() });
            if result.len() >= TXS_BATCH_SIZE {
                break;
            }
        }
        Ok(result)
    }

    fn ack(&self, _id: String) {}
}

#[async_trait]
impl UnprocessedAccountsGetter for Buffer {
    async fn next_accounts(
        &self,
        batch_size: usize,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError> {
        let mut buffer = self.accounts.lock().await;
        let mut result = Vec::with_capacity(batch_size);
        let mut keys_to_remove = Vec::with_capacity(batch_size);
        for key in buffer.keys().cloned() {
            keys_to_remove.push(key);
            if result.len() >= batch_size {
                break;
            }
        }
        for key in keys_to_remove {
            if let Some((key, account)) = buffer.remove_entry(&key) {
                result.push(UnprocessedAccountMessage { account, key, id: String::new() });
            }
        }

        Ok(result)
    }

    fn ack(&self, _ids: Vec<String>) {}
}
