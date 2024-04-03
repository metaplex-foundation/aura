use async_trait::async_trait;
use mockall::automock;

#[automock]
#[async_trait]
pub trait JsonProcessor {
    async fn download_file(&self, url: String)
        -> Result<String, crate::error::JsonDownloaderError>;
    async fn persist_response(
        &self,
        download_response: Result<String, crate::error::JsonDownloaderError>,
        task: entities::models::JsonDownloadTask,
    ) -> Option<String>;
}
