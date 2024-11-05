use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;

pub type Chksm = [u8; 32];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmGrandEpochCksm {
    pub tree_pubkey: Pubkey,
    pub checksum: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmEpochCksm {
    pub epoch: u32,
    pub tree_pubkey: Pubkey,
    pub checksum: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BbgmChangeRecord {
    pub tree_pubkey: Pubkey,
    pub slot: u64,
    pub seq: u64,
    pub signature: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct BbgmChangePos {
    pub slot: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccGrandBucketCksm {
    pub grand_bucket: u16,
    pub checksum: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccBucketCksm {
    pub bucket: u16,
    pub checksum: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccLastChange {
    pub account_pubkey: Pubkey,
    pub slot: u64,
    pub write_version: u64,
}

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

// pub enum PairCmpRes {
//     KeyLess,
//     KeyGreater,
//     KeyEqualValDifferent,
//     FullyEqual,
// }

// pub trait PairCmp {
//     fn pair_cmp(&self, other: &Self) -> PairCmpRes;
// }

// impl PairCmp for BbgmGrandEpochCksm {
//     fn pair_cmp(&self, other: &Self) -> PairCmpRes {
//         if self.tree_pubkey < other.tree_pubkey {
//             PairCmpRes::KeyLess
//         } else if self.tree_pubkey > other.tree_pubkey {
//             PairCmpRes::KeyGreater
//         } else {
//             if self.checksum == other.checksum {
//                 PairCmpRes::FullyEqual
//             } else {
//                 PairCmpRes::KeyEqualValDifferent
//             }
//         }
//     }
// }
