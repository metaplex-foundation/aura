use crate::column::TypedColumn;
use crate::generated::offchain_data_generated::off_chain_data as fb;
use crate::key_encoders::{decode_string, encode_string};
use crate::{Result, ToFlatbuffersConverter};
use entities::models::OffChainDataGrpc;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub enum StorageMutability {
    #[default]
    Immutable,
    Mutable,
}

impl StorageMutability {
    pub fn is_mutable(&self) -> bool {
        match self {
            StorageMutability::Immutable => false,
            StorageMutability::Mutable => true,
        }
    }
}

impl From<&str> for StorageMutability {
    fn from(storage_mutability: &str) -> Self {
        if storage_mutability.is_empty()
            || storage_mutability.starts_with("ipfs://")
            || storage_mutability.starts_with("https://ipfs")
            || storage_mutability.starts_with("https://arweave")
            || storage_mutability.starts_with("https://www.arweave")
        {
            return StorageMutability::Immutable;
        } else {
            return StorageMutability::Mutable;
        }
    }
}

impl From<OffChainData> for OffChainDataGrpc {
    fn from(off_chain_data: OffChainData) -> Self {
        OffChainDataGrpc {
            url: off_chain_data.url.unwrap_or_default(),
            metadata: off_chain_data.metadata.unwrap_or_default(),
        }
    }
}

impl<'a> From<fb::OffChainData<'a>> for OffChainData {
    fn from(value: fb::OffChainData<'a>) -> Self {
        Self {
            storage_mutability: value.storage_mutability().into(),
            url: value.url().map(|url| url.to_string()),
            metadata: value.metadata().map(|metadata| metadata.to_string()),
            last_read_at: value.last_read_at(),
        }
    }
}

impl<'a> From<fb::StorageMutability> for StorageMutability {
    fn from(storage_mutability: fb::StorageMutability) -> Self {
        match storage_mutability.variant_name() {
            Some("Immutable") => StorageMutability::Immutable,
            Some("Mutable") => StorageMutability::Mutable,
            Some(_) => unreachable!(),
            None => unreachable!(),
        }
    }
}

impl From<OffChainDataGrpc> for OffChainData {
    fn from(off_chain_data: OffChainDataGrpc) -> Self {
        let storage_mutability = StorageMutability::from(off_chain_data.url.as_str());
        OffChainData {
            storage_mutability,
            url: Some(off_chain_data.url),
            metadata: Some(off_chain_data.metadata),
            last_read_at: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OffChainData {
    pub storage_mutability: StorageMutability,
    pub url: Option<String>,
    pub metadata: Option<String>,
    pub last_read_at: i64,
}

impl<'a> ToFlatbuffersConverter<'a> for OffChainData {
    type Target = fb::OffChainData<'a>;

    fn convert_to_fb_bytes(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let off_chain_data = self.convert_to_fb(&mut builder);
        builder.finish_minimal(off_chain_data);
        builder.finished_data().to_vec()
    }

    fn convert_to_fb(&self, builder: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::Target> {
        let storage_mutability = self.storage_mutability.clone().into();
        let url = self.url.as_ref().map(|url| builder.create_string(&url));
        let metadata = self
            .metadata
            .as_ref()
            .map(|metadata| builder.create_string(&metadata));

        fb::OffChainData::create(
            builder,
            &fb::OffChainDataArgs {
                storage_mutability,
                url,
                metadata,
                last_read_at: self.last_read_at,
            },
        )
    }
}

impl From<StorageMutability> for fb::StorageMutability {
    fn from(storage_mutability: StorageMutability) -> Self {
        match storage_mutability {
            StorageMutability::Immutable => fb::StorageMutability::Immutable,
            StorageMutability::Mutable => fb::StorageMutability::Mutable,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OffChainDataDeprecated {
    pub url: String,
    pub metadata: String,
}

impl TypedColumn for OffChainDataDeprecated {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "OFFCHAIN_DATA"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

impl TypedColumn for OffChainData {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "OFFCHAIN_DATA_V2"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}
