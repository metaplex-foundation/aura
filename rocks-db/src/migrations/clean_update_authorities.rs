use crate::asset::AssetCollection;
use crate::migrator::{RocksMigration, SerializationType};
use crate::ToFlatbuffersConverter;
use entities::models::{UpdateVersion, Updated};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetCollectionBeforeCleanUp {
    pub pubkey: Pubkey,
    pub collection: Updated<Pubkey>,
    pub is_collection_verified: Updated<bool>,
    pub authority: Updated<Option<Pubkey>>,
}

impl From<AssetCollectionBeforeCleanUp> for AssetCollection {
    fn from(value: AssetCollectionBeforeCleanUp) -> Self {
        Self {
            pubkey: value.pubkey,
            collection: value.collection,
            is_collection_verified: value.is_collection_verified,
            authority: Updated::new(0, Some(UpdateVersion::WriteVersion(0)), None),
        }
    }
}

impl<'a> ToFlatbuffersConverter<'a> for AssetCollection {
    type Target = AssetCollection;

    fn convert_to_fb(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> flatbuffers::WIPOffset<Self::Target> {
        todo!()
    }

    fn convert_to_fb_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

pub(crate) struct CleanCollectionAuthoritiesMigration;
impl RocksMigration for CleanCollectionAuthoritiesMigration {
    const VERSION: u64 = 2;
    const SERIALIZATION_TYPE: SerializationType = SerializationType::Bincode;
    type NewDataType = AssetCollection;
    type OldDataType = AssetCollectionBeforeCleanUp;
}
