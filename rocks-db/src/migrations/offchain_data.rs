use bincode::deserialize;
use rocksdb::MergeOperands;
use tracing::error;

use crate::columns::offchain_data::{OffChainData, OffChainDataDeprecated, StorageMutability};
use crate::migrator::{RocksMigration, SerializationType};
use crate::ToFlatbuffersConverter;

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

impl OffChainData {
    pub fn merge_off_chain_data(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut old_data = None;

        // Deserialize existing value if present
        if let Some(existing_val) = existing_val {
            match deserialize::<OffChainDataDeprecated>(existing_val) {
                Ok(value) => old_data = Some(value),
                Err(e) => error!(
                    "RocksDB: AssetDynamicDetails deserialize existing_val: {}",
                    e
                ),
            }
        }

        // Iterate over operands and merge
        for op in operands {
            match deserialize::<OffChainDataDeprecated>(op) {
                Ok(new_val) => {
                    old_data = Some(new_val);
                }
                Err(e) => error!("RocksDB: AssetDynamicDetails deserialize new_val: {}", e),
            }
        }

        // Serialize the result back into bytes
        old_data.and_then(|deprecated_format| {
            Some(OffChainData::from(deprecated_format).convert_to_fb_bytes())
        })
    }
}

pub(crate) struct OffChainDataMigration;
impl RocksMigration for OffChainDataMigration {
    const VERSION: u64 = 4;
    const DESERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Flatbuffers;
    type NewDataType = OffChainData;
    type OldDataType = OffChainDataDeprecated;
}
