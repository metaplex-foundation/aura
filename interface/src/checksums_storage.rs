use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;

/// Type of checksum for bubblegum epochs and account NFT buckets.
/// It is technically a SHA3 hash.
pub type Chksm = [u8; 32];

/// Data transfer object for bubblegum grand epoch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmGrandEpochCksm {
    pub tree_pubkey: Pubkey,
    pub checksum: Option<[u8; 32]>,
}

/// Data transfer object for bubblegum epoch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmEpochCksm {
    pub epoch: u32,
    pub tree_pubkey: Pubkey,
    pub checksum: Option<[u8; 32]>,
}

/// Data transfer object for bubblegum tree change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BbgmChangeRecord {
    pub tree_pubkey: Pubkey,
    pub slot: u64,
    pub seq: u64,
    pub signature: String,
}

/// Used to specify offset when fetching a portion of
/// bubblegum tree changes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmChangePos {
    pub slot: u64,
    pub seq: u64,
}

/// Data transfer object for account NFT grand bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccGrandBucketCksm {
    pub grand_bucket: u16,
    pub checksum: Option<[u8; 32]>,
}

/// Data transfer object for account NFT bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccBucketCksm {
    pub bucket: u16,
    pub checksum: Option<[u8; 32]>,
}

/// Data transfer object for account NFT last received state.
#[derive(Debug, Clone, PartialOrd, Ord)]
pub struct AccLastChange {
    pub account_pubkey: Pubkey,
    pub slot: u64,
    pub write_version: u64,
    pub data_hash: u64,
}

impl PartialEq for AccLastChange {
    fn eq(&self, other: &Self) -> bool {
        // we don't take the slot in account since it can vary because of forks
        self.account_pubkey == other.account_pubkey
            && self.write_version == other.write_version
            && self.data_hash == other.data_hash
    }
}
impl Eq for AccLastChange {}

impl core::hash::Hash for AccLastChange {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.account_pubkey.hash(state);
        self.write_version.hash(state);
        self.data_hash.hash(state);
    }

    fn hash_slice<H: std::hash::Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for piece in data {
            piece.hash(state)
        }
    }
}

/// Interface for querying bubblegum checksums from peer
/// or local storage.
#[async_trait]
pub trait BbgmChecksumServiceApi {
    async fn get_earliest_grand_epoch(&self) -> anyhow::Result<Option<u16>>;

    async fn list_grand_epoch_checksums(
        &self,
        grand_epoch: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<BbgmGrandEpochCksm>>;

    async fn list_epoch_checksums(
        &self,
        grand_epoch: u16,
        tree_pubkey: Pubkey,
    ) -> anyhow::Result<Vec<BbgmEpochCksm>>;

    async fn list_epoch_changes(
        &self,
        epoch: u32,
        tree_pubkey: Pubkey,
        limit: Option<u64>,
        after: Option<BbgmChangePos>,
    ) -> anyhow::Result<Vec<BbgmChangeRecord>>;

    async fn propose_missing_changes(&self, changes: &[BbgmChangeRecord]);
}

/// Interface for querying Account NFT checksums from peer
/// or local storage.
#[async_trait]
pub trait AccChecksumServiceApi {
    async fn list_grand_buckets(&self) -> anyhow::Result<Vec<AccGrandBucketCksm>>;

    async fn list_bucket_checksums(&self, grand_bucket: u16) -> anyhow::Result<Vec<AccBucketCksm>>;

    async fn list_accounts(
        &self,
        bucket: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<AccLastChange>>;

    async fn propose_missing_changes(&self, changes: Vec<AccLastChange>);
}
