use solana_client::client_error::ClientError;
use solana_program::pubkey::ParsePubkeyError;
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
    #[error("RPC {0}")]
    Rpc(#[from] ClientError),
    #[error("Cannot get response field {0}")]
    CannotGetResponseField(String),
    #[error("ParsePubkey {0}")]
    ParsePubkey(#[from] ParsePubkeyError),
    #[error("Anchor {0}")]
    Anchor(#[from] anchor_lang::error::Error),
    #[error("CannotCreateMerkleTree: depth [{0}], size [{1}]")]
    CannotCreateMerkleTree(u32, u32),
}
