use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum GrpcError {
    #[error("Pubkey from: {0:?}")]
    PubkeyFrom(Vec<u8>),
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Cannot cast enum: {0} {1}")]
    EnumCast(String, String),
}
