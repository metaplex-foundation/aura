use crate::migrator::{RocksMigration, SerializationType};
use crate::offchain_data::{OffChainData, OffChainDataDeprecated, StorageMutability};

impl From<OffChainDataDeprecated> for OffChainData {
    fn from(value: OffChainDataDeprecated) -> Self {
        let immutability = StorageMutability::from(value.url.as_str());
        Self {
            storage_mutability: immutability,
            url: Some(value.url),
            metadata: Some(value.metadata),
            last_read_at: 0,
        }
    }
}

pub(crate) struct OffChainDataMigration;
impl RocksMigration for OffChainDataMigration {
    const VERSION: u64 = 4;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = OffChainData;
    type OldDataType = OffChainDataDeprecated;
}
