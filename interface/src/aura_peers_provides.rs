use async_trait::async_trait;

/// Aura can connect other Aura peers to exchange various information.
#[async_trait]
pub trait AuraPeersProvides {
    /// Provide list of URLs of other Aura nodes that are trusted,
    /// meaning we don't expect that a data from them might be incorrect intentionally.
    async fn list_trusted_peers(&self) -> Vec<String>;
}
