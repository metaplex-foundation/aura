use async_trait::async_trait;
use mockall::automock;

use crate::asset_streaming_and_discovery::AsyncError;


pub struct UrlDownloadNotification {
    pub url: String,
    pub outcome: DownloadOutcome,
}

pub enum DownloadOutcome {
    Success { mime: String, size: u32 },
    NotFound,
    ServerError,
    NotSupportedFormat,
    TooLarge,
    TooManyRequests,
    CorruptedAsset,
    Nothing,
}

#[automock]
#[async_trait]
pub trait UrlsToDownloadStore: Send + Sync {
    fn get_urls_to_download(&self, number_of_urls: u32) -> Result<Vec<String>, AsyncError>;
    fn submit_download_results(&self, results: Vec<UrlDownloadNotification>) -> Result<(), AsyncError>;
}
