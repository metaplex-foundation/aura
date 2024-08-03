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

/// Interface for storage that serves as a queue for URLs to download,
/// i.e. URLs of assets that are to be sent to Storage service,
/// that will download actual assets and save them as previews.
#[automock]
#[async_trait]
pub trait UrlsToDownloadStore: Send + Sync {
    /// Fetch potions of URLs for downloading
    /// ## Args:
    /// * `number_of_urls` - maximum amount of URLs to be retrieved
    fn get_urls_to_download(&self, number_of_urls: u32) -> Result<Vec<String>, AsyncError>;

    /// Process URLs downloading results, which in case of successfull download assumes
    /// removal from URLs to download queue and putting into "assets previews"
    fn submit_download_results(
        &self,
        results: Vec<UrlDownloadNotification>,
    ) -> Result<(), AsyncError>;
}
