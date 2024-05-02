use async_trait::async_trait;
use entities::rollup::Rollup;
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;

pub struct RollupDownloaderImpl;
#[async_trait]
impl RollupDownloader for RollupDownloaderImpl {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }

    async fn download_rollup_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<Rollup>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;

        let file_hash = xxhash_rust::xxh3::xxh3_128(&response);

        let hash_hex = hex::encode(file_hash.to_le_bytes());

        if hash_hex != checksum {
            return Err(UsecaseError::InvalidParameters(
                "File checksum mismatch".to_string(),
            ));
        }

        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}
