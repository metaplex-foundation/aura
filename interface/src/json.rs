use async_trait::async_trait;
use mockall::automock;

#[derive(Debug, Clone)]
pub enum JsonDownloadResult {
    JsonContent(String),
    MediaUrlAndMimeType { url: String, mime_type: String },
}

#[automock]
#[async_trait]
pub trait JsonDownloader {
    async fn download_file(
        &self,
        url: String,
    ) -> Result<JsonDownloadResult, crate::error::JsonDownloaderError>;
}

#[automock]
#[async_trait]
pub trait JsonPersister {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<JsonDownloadResult, crate::error::JsonDownloaderError>)>,
    ) -> Result<(), crate::error::JsonDownloaderError>;
}
