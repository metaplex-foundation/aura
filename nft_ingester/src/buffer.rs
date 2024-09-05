use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use entities::enums::UnprocessedAccount;
use entities::models::{BufferedTransaction, CoreAssetFee, Task, UnprocessedAccountMessage};
use interface::error::UsecaseError;
use interface::signature_persistence::ProcessingDataGetter;
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
impl ProcessingDataGetter for Buffer {
    async fn get_processing_transaction(&self) -> Option<BufferedTransaction> {
        let mut buffer = self.transactions.lock().await;
        buffer.pop_front()
    }
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
}

#[derive(Default)]
pub struct FeesBuffer {
    pub mpl_core_fee_assets: Mutex<HashMap<Pubkey, CoreAssetFee>>,
    pub accounts: Mutex<HashMap<Pubkey, UnprocessedAccount>>,
}
impl FeesBuffer {
    pub fn new() -> Self {
        Self {
            mpl_core_fee_assets: Mutex::new(HashMap::new()),
            accounts: Mutex::new(HashMap::new()),
        }
    }
}
