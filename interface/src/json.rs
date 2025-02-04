use std::time::Duration;

use async_trait::async_trait;
use mockall::automock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonDownloadResult {
    JsonContent(String),
    MediaUrlAndMimeType { url: String, mime_type: String },
    NotModified,
}

#[automock]
#[async_trait]
pub trait JsonDownloader {
    async fn download_file(
        &self,
        url: String,
        timeout: Duration,
    ) -> Result<JsonDownloadResult, crate::error::JsonDownloaderError>;
    fn skip_refresh(&self) -> bool;
}

#[automock]
#[async_trait]
pub trait NewJsonDownloader {
    async fn download_file(
        &self,
        metadata_download_task: &entities::models::MetadataDownloadTask,
        timeout: Duration,
    ) -> Result<JsonDownloadResult, crate::error::JsonDownloaderError>;
    fn skip_refresh(&self) -> bool;
}

#[automock]
#[async_trait]
pub trait JsonPersister {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<JsonDownloadResult, crate::error::JsonDownloaderError>)>,
    ) -> Result<(), crate::error::JsonDownloaderError>;
}
