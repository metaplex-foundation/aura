use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use entities::models::{BufferedTransaction, Task};
use interface::signature_persistence::ProcessingDataGetter;
use tokio::sync::Mutex;
use tonic::async_trait;

use crate::inscriptions_processor::{InscriptionDataInfo, InscriptionInfo};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::columns::{Mint, TokenAccount};

use crate::mplx_updates_processor::{
    BurntMetadataSlot, IndexableAssetWithAccountInfo, TokenMetadata,
};
use crate::mplx_updates_processor::{CoreAssetFee, MetadataInfo};

#[derive(Default)]
pub struct Buffer {
    pub transactions: Mutex<VecDeque<BufferedTransaction>>,
    pub mplx_metadata_info: Mutex<HashMap<Vec<u8>, MetadataInfo>>,
    pub token_accs: Mutex<HashMap<Pubkey, TokenAccount>>,
    pub mints: Mutex<HashMap<Vec<u8>, Mint>>,
    pub json_tasks: Arc<Mutex<VecDeque<Task>>>,
    pub token_metadata_editions: Mutex<HashMap<Pubkey, TokenMetadata>>,
    pub burnt_metadata_at_slot: Mutex<HashMap<Pubkey, BurntMetadataSlot>>,
    pub burnt_mpl_core_at_slot: Mutex<HashMap<Pubkey, BurntMetadataSlot>>,
    pub mpl_core_indexable_assets: Mutex<HashMap<Pubkey, IndexableAssetWithAccountInfo>>,
    pub inscriptions: Mutex<HashMap<Pubkey, InscriptionInfo>>,
    pub inscriptions_data: Mutex<HashMap<Pubkey, InscriptionDataInfo>>,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            transactions: Mutex::new(VecDeque::<BufferedTransaction>::new()),
            mplx_metadata_info: Mutex::new(HashMap::new()),
            token_accs: Mutex::new(HashMap::new()),
            mints: Mutex::new(HashMap::new()),
            json_tasks: Arc::new(Mutex::new(VecDeque::<Task>::new())),
            token_metadata_editions: Mutex::new(HashMap::new()),
            burnt_metadata_at_slot: Mutex::new(HashMap::new()),
            burnt_mpl_core_at_slot: Mutex::new(HashMap::new()),
            mpl_core_indexable_assets: Mutex::new(HashMap::new()),
            inscriptions: Mutex::new(HashMap::new()),
            inscriptions_data: Mutex::new(HashMap::new()),
        }
    }

    pub async fn debug(&self) {
        println!(
            "\nMplx metadata info buffer: {}\nTransactions buffer: {}\nSPL Tokens buffer: {}\nSPL Mints buffer: {}\nJson tasks buffer: {}\nToken Metadata Editions buffer: {}\nBurnt Metadata buffer: {}\nMpl Core full assets buffer: {}\nBurnt Mpl Core buffer: {}\nInscriptions buffer: {}\nInscriptions Data buffer: {}\n",
            self.mplx_metadata_info.lock().await.len(),
            self.transactions.lock().await.len(),
            self.token_accs.lock().await.len(),
            self.mints.lock().await.len(),
            self.json_tasks.lock().await.len(),
            self.token_metadata_editions.lock().await.len(),
            self.burnt_metadata_at_slot.lock().await.len(),
            self.mpl_core_indexable_assets.lock().await.len(),
            self.burnt_mpl_core_at_slot.lock().await.len(),
            self.inscriptions.lock().await.len(),
            self.inscriptions_data.lock().await.len(),
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
        metrics.set_buffer(
            "buffer_editions",
            self.token_metadata_editions.lock().await.len() as i64,
        );
        metrics.set_buffer(
            "mpl_core",
            self.mpl_core_indexable_assets.lock().await.len() as i64,
        );
        metrics.set_buffer("inscriptions", self.inscriptions.lock().await.len() as i64);
        metrics.set_buffer(
            "inscriptions_data",
            self.inscriptions_data.lock().await.len() as i64,
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

#[derive(Default)]
pub struct FeesBuffer {
    pub mpl_core_fee_assets: Mutex<HashMap<Pubkey, CoreAssetFee>>,
}
impl FeesBuffer {
    pub fn new() -> Self {
        Self {
            mpl_core_fee_assets: Mutex::new(HashMap::new()),
        }
    }
}
