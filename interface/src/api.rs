use async_trait::async_trait;
use mockall::automock;

#[automock]
#[async_trait]
pub trait APIJsonDownloaderMiddleware {
    async fn get_metadata(
        &self,
        metadata_urls: std::collections::HashSet<String>,
    ) -> Result<std::collections::HashMap<String, String>, String>;
}
