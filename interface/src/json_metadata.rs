use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use entities::{enums::OffchainDataMutability, models::MetadataDownloadTask};
use mockall::automock;

use crate::error::JsonDownloaderError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonDownloadResult {
    /// Holds the actual JSON content as a string.
    JsonContent(String),
    /// Indicates a media URL and its MIME type (e.g., image/video).
    MediaUrlAndMimeType { url: String, mime_type: String },
    /// Indicates that the resource has not changed since the last known timestamp.
    NotModified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDownloadResult {
    pub etag: Option<String>,
    pub last_modified_at: Option<DateTime<Utc>>,
    pub cache_control: Option<CacheControlResponse>,
    pub result: JsonDownloadResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheControlResponse {
    pub publicity: Option<String>,
    pub max_age: Option<u64>,
    pub mutability: Option<OffchainDataMutability>,
}

impl MetadataDownloadResult {
    pub fn new(
        etag: Option<String>,
        last_modified_at: Option<DateTime<Utc>>,
        cache_control: Option<CacheControlResponse>,
        result: JsonDownloadResult,
    ) -> Self {
        Self { etag, last_modified_at, cache_control, result }
    }
}

#[automock]
#[async_trait]
pub trait JsonDownloader {
    async fn download_file(
        &self,
        metadata_download_task: &MetadataDownloadTask,
        timeout: Duration,
    ) -> Result<MetadataDownloadResult, JsonDownloaderError>;
    fn skip_refresh(&self) -> bool;
}

#[automock]
#[async_trait]
pub trait JsonPersister {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    ) -> Result<(), JsonDownloaderError>;
}
