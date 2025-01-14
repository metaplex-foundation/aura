use async_trait::async_trait;
use bubblegum_batch_sdk::model::{BatchMint, BatchMintInstruction};
use mockall::automock;

use crate::error::UsecaseError;

#[automock]
#[async_trait]
pub trait BatchMintDownloader {
    async fn download_batch_mint(&self, url: &str) -> Result<Box<BatchMint>, UsecaseError>;
    async fn download_batch_mint_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<BatchMint>, UsecaseError>;
}

#[async_trait]
pub trait BatchMintTxSender {
    async fn send_batch_mint_tx(
        &self,
        instruction: BatchMintInstruction,
    ) -> Result<(), UsecaseError>;
}
