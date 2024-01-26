use thiserror::Error;

#[derive(Error, Debug)]
pub enum IntegrityVerificationError {
    #[error("Json {0}")]
    Json(#[from] serde_json::Error),
    #[error("Reqwest {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("IO {0}")]
    IO(#[from] std::io::Error),
    #[error("FetchKeys {0}")]
    FetchKeys(String),
    #[error("CannotFindAssetTree {0}")]
    CannotFindAssetTree(String),
    #[error("CannotGetSlot {0}")]
    CannotGetSlot(String),
}
