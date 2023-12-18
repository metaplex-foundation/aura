use metrics_utils::IngesterMetricsConfig;
use rocks_db::columns::{Mint, TokenAccount};
use spl_account_compression::events::ChangeLogEventV1;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::mplx_updates_processor::MetadataInfo;

pub struct Buffer {
    pub transactions: Mutex<VecDeque<BufferedTransaction>>,

    pub mplx_metadata_info: Mutex<HashMap<Vec<u8>, MetadataInfo>>,

    pub token_accs: Mutex<HashMap<Vec<u8>, TokenAccount>>,

    pub mints: Mutex<HashMap<Vec<u8>, Mint>>,

    pub compressed_change_log: Mutex<HashMap<Vec<u8>, ChangeLogEventV1>>,
}

#[derive(Clone)]
pub struct BufferedTransaction {
    pub transaction: Vec<u8>,
    // this flag tells if the transaction should be mapped from extrnode flatbuffer to mplx flatbuffer structure
    // data from geyser should be mapped and data from BG should not
    pub map_flatbuffer: bool,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            transactions: Mutex::new(VecDeque::<BufferedTransaction>::new()),

            mplx_metadata_info: Mutex::new(HashMap::new()),

            token_accs: Mutex::new(HashMap::new()),

            mints: Mutex::new(HashMap::new()),

            compressed_change_log: Mutex::new(HashMap::new()),
        }
    }

    pub async fn debug(&self) {
        println!(
            "\nMplx metadata info buffer: {}\nTransactions buffer: {}\nSPL Tokens buffer: {}\nSPL Mints buffer: {}\n",
            self.mplx_metadata_info.lock().await.len(),
            self.transactions.lock().await.len(),
            self.token_accs.lock().await.len(),
            self.mints.lock().await.len(),
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
}
