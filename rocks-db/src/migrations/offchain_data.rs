use crate::columns::offchain_data::{OffChainData, OffChainDataDeprecated, StorageMutability};
use crate::migrator::{RocksMigration, SerializationType};

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
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Flatbuffers;
    type NewDataType = OffChainData;
    type OldDataType = OffChainDataDeprecated;
}
