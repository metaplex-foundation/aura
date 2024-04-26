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
}
