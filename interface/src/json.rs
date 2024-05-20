use async_trait::async_trait;
use mockall::automock;

#[automock]
#[async_trait]
pub trait JsonDownloader {
    async fn download_file(&self, url: String)
        -> Result<String, crate::error::JsonDownloaderError>;
}

#[automock]
#[async_trait]
pub trait JsonPersister {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<String, crate::error::JsonDownloaderError>)>,
    ) -> Result<(), crate::error::JsonDownloaderError>;
}
