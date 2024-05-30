use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RollupValidationError {
    #[error("PDACheckFail: expected: {0}, got: {1}")]
    PDACheckFail(String, String),
    #[error("InvalidDataHash: expected: {0}, got: {1}")]
    InvalidDataHash(String, String),
    #[error("InvalidCreatorsHash: expected: {0}, got: {1}")]
    InvalidCreatorsHash(String, String),
    #[error("InvalidRoot: expected: {0}, got: {1}")]
    InvalidRoot(String, String),
    #[error("CannotCreateMerkleTree: depth [{0}], size [{1}]")]
    CannotCreateMerkleTree(u32, u32),
    #[error("NoRelevantRolledMint: index {0}")]
    NoRelevantRolledMint(u64),
    #[error("WrongAssetPath: id {0}")]
    WrongAssetPath(String),
    #[error("StdIo {0}")]
    StdIo(String),
    #[error("WrongTreeIdForChangeLog: asset: {0}, expected: {1}, got: {2}")]
    WrongTreeIdForChangeLog(String, String, String),
    #[error("WrongChangeLogIndex: asset: {0}, expected: {0}, got: {1}")]
    WrongChangeLogIndex(String, u32, u32),
    #[error("SplCompression: {0}")]
    SplCompression(#[from] spl_account_compression::ConcurrentMerkleTreeError),
    #[error("Anchor {0}")]
    Anchor(#[from] anchor_lang::error::Error),
}

impl From<std::io::Error> for RollupValidationError {
    fn from(err: std::io::Error) -> Self {
        RollupValidationError::StdIo(err.to_string())
    }
}
