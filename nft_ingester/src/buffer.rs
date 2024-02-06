use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use entities::models::BufferedTransaction;
use interface::signature_persistence::ProcessingDataGetter;
use tokio::sync::Mutex;
use tonic::async_trait;

use metrics_utils::IngesterMetricsConfig;
use rocks_db::columns::{Mint, TokenAccount};

use crate::{db_v2::Task, mplx_updates_processor::MetadataInfo};

#[derive(Default)]
pub struct Buffer {
    pub transactions: Mutex<VecDeque<BufferedTransaction>>,
    pub mplx_metadata_info: Mutex<HashMap<Vec<u8>, MetadataInfo>>,
    pub token_accs: Mutex<HashMap<Vec<u8>, TokenAccount>>,
    pub mints: Mutex<HashMap<Vec<u8>, Mint>>,
    pub json_tasks: Arc<Mutex<VecDeque<Task>>>,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            transactions: Mutex::new(VecDeque::<BufferedTransaction>::new()),
            mplx_metadata_info: Mutex::new(HashMap::new()),
            token_accs: Mutex::new(HashMap::new()),
            mints: Mutex::new(HashMap::new()),
            json_tasks: Arc::new(Mutex::new(VecDeque::<Task>::new())),
        }
    }

    pub async fn debug(&self) {
        println!(
            "\nMplx metadata info buffer: {}\nTransactions buffer: {}\nSPL Tokens buffer: {}\nSPL Mints buffer: {}\nJson tasks buffer: {}\n",
            self.mplx_metadata_info.lock().await.len(),
            self.transactions.lock().await.len(),
            self.token_accs.lock().await.len(),
            self.mints.lock().await.len(),
            self.json_tasks.lock().await.len(),
        );
    }

    pub async fn capture_metrics(&self, metrics: &Arc<IngesterMetricsConfig>) {
        metrics.set_buffer(
            "buffer_transactions",
            self.transactions.lock().await.len() as i64,
        );
        metrics.set_buffer(
            "buffer_mplx_accounts",
            self.mplx_metadata_info.lock().await.len() as i64,
        );
    }

    pub async fn mplx_metadata_len(&self) -> usize {
        self.mplx_metadata_info.lock().await.len()
    }
}

#[async_trait]
impl ProcessingDataGetter for Buffer {
    async fn get_processing_transaction(&self) -> Option<BufferedTransaction> {
        let mut buffer = self.transactions.lock().await;
        buffer.pop_front()
    }
}
