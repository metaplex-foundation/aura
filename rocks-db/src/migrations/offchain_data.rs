use rocksdb::MergeOperands;

use crate::{
    columns::offchain_data::{OffChainData, OffChainDataDeprecated, StorageMutability},
    migrator::{RocksMigration, SerializationType},
};

impl From<OffChainDataDeprecated> for OffChainData {
    fn from(value: OffChainDataDeprecated) -> Self {
        let immutability = StorageMutability::from_url(value.url.as_str());
        Self {
            storage_mutability: immutability,
            url: Some(value.url),
            metadata: Some(value.metadata),
            last_read_at: 0,
        }
    }
}

impl OffChainData {
    pub fn merge_off_chain_data(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut bytes = existing_val;

        if let Some(op_bytes) = operands.into_iter().last() {
            bytes = Some(op_bytes);
        }

        bytes.map(|bytes| bytes.to_vec())
    }
}

pub(crate) struct OffChainDataMigration;
impl RocksMigration for OffChainDataMigration {
    const VERSION: u64 = 4;
    const DESERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Flatbuffers;
    type KeyType = String;
    type NewDataType = OffChainData;
    type OldDataType = OffChainDataDeprecated;
}
