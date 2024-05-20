use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::rollup::{BatchMintInstruction, Rollup};
use mockall::automock;

#[automock]
pub trait BatchMintInstructionGetter {
    fn get_instruction(&self) -> BatchMintInstruction;
}

#[automock]
#[async_trait]
pub trait RollupDownloader {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError>;
    async fn download_rollup_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<Rollup>, UsecaseError>;
}
