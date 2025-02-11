use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mockall::automock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonDownloadResult {
    JsonContent(String),
    MediaUrlAndMimeType { url: String, mime_type: String },
    NotModified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDownloadResult {
    pub etag: Option<String>,
    pub last_modified_at: Option<DateTime<Utc>>,
    pub result: JsonDownloadResult,
}

impl MetadataDownloadResult {
    pub fn new(
        etag: Option<String>,
        last_modified_at: Option<DateTime<Utc>>,
        result: JsonDownloadResult,
    ) -> Self {
        Self { etag, last_modified_at, result }
    }
}

#[automock]
#[async_trait]
pub trait JsonDownloader {
    async fn download_file(
        &self,
        metadata_download_task: &entities::models::MetadataDownloadTask,
        timeout: Duration,
    ) -> Result<MetadataDownloadResult, crate::error::JsonDownloaderError>;
    fn skip_refresh(&self) -> bool;
}

#[automock]
#[async_trait]
pub trait JsonPersister {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<MetadataDownloadResult, crate::error::JsonDownloaderError>)>,
    ) -> Result<(), crate::error::JsonDownloaderError>;
}
