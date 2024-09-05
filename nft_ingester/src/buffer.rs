use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use entities::enums::UnprocessedAccount;
use entities::models::{BufferedTransaction, BufferedTxWithID, Task, UnprocessedAccountMessage};
use interface::error::UsecaseError;
use interface::signature_persistence::UnprocessedTransactionsGetter;
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use tokio::sync::Mutex;
use tonic::async_trait;
use tracing::log::info;

use metrics_utils::IngesterMetricsConfig;

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
        metrics.set_buffer(
            "buffer_transactions",
            self.transactions.lock().await.len() as i64,
        );
        metrics.set_buffer(
            "buffer_json_tasks",
            self.json_tasks.lock().await.len() as i64,
        );
        metrics.set_buffer("buffer_accounts", self.accounts.lock().await.len() as i64);
    }
}

#[async_trait]
impl UnprocessedTransactionsGetter for Buffer {
    async fn next_transactions(&self) -> Result<Vec<BufferedTxWithID>, UsecaseError> {
        let mut buffer = self.transactions.lock().await;
        // todo!()
        Ok(vec![BufferedTxWithID {
            tx: buffer.pop_front().unwrap(),
            id: "".to_string(),
        }])
    }

    fn ack(&self, _id: String) {}
}

#[async_trait]
impl UnprocessedAccountsGetter for Buffer {
    async fn next_accounts(&self) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError> {
        let mut buffer = self.accounts.lock().await;
        //todo!
        Ok(vec![buffer
            .keys()
            .next()
            .cloned()
            .and_then(|key| buffer.remove_entry(&key))
            .map(|f| UnprocessedAccountMessage {
                account: f.1,
                key: f.0,
                id: "".to_string(),
            })
            .unwrap()])
    }

    fn ack(&self, _ids: Vec<String>) {}
}
