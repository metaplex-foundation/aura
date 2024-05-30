use thiserror::Error;
use tonic::transport::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum GrpcError {
    #[error("Pubkey from: {0:?}")]
    PubkeyFrom(Vec<u8>),
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Cannot cast enum: {0} {1}")]
    EnumCast(String, String),
    #[error("UriCreate: {0}")]
    UriCreate(String),
    #[error("TonicTransport: {0}")]
    TonicTransport(String),
    #[error("Cbor: {0}")]
    Cbor(String),
}
impl From<tonic::transport::Error> for GrpcError {
    fn from(value: Error) -> Self {
        Self::TonicTransport(value.to_string())
    }
}

impl From<serde_cbor::error::Error> for GrpcError {
    fn from(value: serde_cbor::error::Error) -> Self {
        Self::Cbor(value.to_string())
    }
}
