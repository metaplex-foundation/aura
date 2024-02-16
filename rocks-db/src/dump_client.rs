use crate::{key_encoders::decode_pubkey, storage_traits::AssetIndexReader, Storage};
use csv::Writer;
use entities::enums::{
    OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
};
use hex;
use inflector::Inflector;
use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, io::Write};

fn serialize_as_snake_case<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: std::fmt::Debug, // Enums can be Debug-formatted to get their variant names
{
    let variant_name = format!("{:?}", value); // Get the variant name as a string
    let snake_case_name = variant_name.to_snake_case(); // Convert to snake_case
    serializer.serialize_str(&snake_case_name)
}

fn serialize_option_as_snake_case<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: std::fmt::Debug, // Assumes T can be formatted using Debug, which is true for enums
{
    match value {
        Some(v) => {
            let variant_name = format!("{:?}", v); // Convert the enum variant to a string
            let snake_case_name = variant_name.to_snake_case(); // Convert to snake_case
            serializer.serialize_some(&snake_case_name)
        }
        None => serializer.serialize_none(),
    }
}
#[derive(Serialize)]
struct AssetRecord {
    ast_pubkey: String,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_specification_version: SpecificationVersions,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_specification_asset_class: SpecificationAssetClass,
    #[serde(serialize_with = "serialize_as_snake_case")]
    ast_royalty_target_type: RoyaltyTargetType,
    ast_royalty_amount: i64,
    ast_slot_created: i64,
    #[serde(serialize_with = "serialize_option_as_snake_case")]
    ast_owner_type: Option<OwnerType>,
    ast_owner: Option<String>,
    ast_delegate: Option<String>,
    ast_authority: Option<String>,
    ast_collection: Option<String>,
    ast_is_collection_verified: Option<bool>,
    ast_is_burnt: bool,
    ast_is_compressible: bool,
    ast_is_compressed: bool,
    ast_is_frozen: bool,
    ast_supply: Option<i64>,
    ast_metadata_url_id: Option<String>,
    ast_slot_updated: i64,
}

impl Storage {
    pub async fn dump_csv<W: Write>(
        &self,
        metadata_writer: &mut Writer<W>,
        creators_writer: &mut Writer<W>,
        assets_writer: &mut Writer<W>,
        batch_size: usize,
        rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut batch = Vec::with_capacity(batch_size);
        let mut metadata_key_set = HashSet::new();
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            batch.push(k);
            if batch.len() == batch_size {
                self.dump_csv_batch(
                    &batch,
                    metadata_writer,
                    creators_writer,
                    assets_writer,
                    &mut metadata_key_set,
                )
                .await?;
                batch.clear();
            }
            if !rx.is_empty() {
                break;
            }
        }
        if !batch.is_empty() {
            self.dump_csv_batch(
                &batch,
                metadata_writer,
                creators_writer,
                assets_writer,
                &mut metadata_key_set,
            )
            .await?;
        }
        metadata_writer.flush().map_err(|e| e.to_string())?;
        creators_writer.flush().map_err(|e| e.to_string())?;
        assets_writer.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn dump_csv_batch<W: Write>(
        &self,
        batch: &[Pubkey],
        metadata_writer: &mut csv::Writer<W>,
        creators_writer: &mut csv::Writer<W>,
        assets_writer: &mut csv::Writer<W>,
        metadata_key_set: &mut HashSet<Vec<u8>>,
    ) -> Result<(), String> {
        let indexes = self
            .get_asset_indexes(batch)
            .await
            .map_err(|e| e.to_string())?;
        for (key, index) in indexes {
            let metadata_url = index.metadata_url.map(|url| {
                let mut hasher = Sha256::new();
                let url = url.metadata_url.trim();
                hasher.update(url);
                let metadata_key = hasher.finalize().to_vec();
                (metadata_key, url.to_string())
            });
            if let Some((ref metadata_key, ref url)) = metadata_url {
                if !metadata_key_set.contains(metadata_key) {
                    metadata_key_set.insert(metadata_key.clone());
                    metadata_writer
                        .serialize((hex::encode(metadata_key), url.to_string(), "pending"))
                        .map_err(|e| e.to_string())?;
                }
            }
            for creator in index.creators {
                creators_writer
                    .serialize((
                        hex::encode(key.to_bytes()),
                        hex::encode(creator.creator),
                        creator.creator_verified,
                        index.slot_updated,
                    ))
                    .map_err(|e| e.to_string())?;
            }
            let record = AssetRecord {
                ast_pubkey: hex::encode(key.to_bytes()),
                ast_specification_version: index.specification_version,
                ast_specification_asset_class: index.specification_asset_class,
                ast_royalty_target_type: index.royalty_target_type,
                ast_royalty_amount: index.royalty_amount,
                ast_slot_created: index.slot_created,
                ast_owner_type: index.owner_type,
                ast_owner: index.owner.map(hex::encode),
                ast_delegate: index.delegate.map(hex::encode),
                ast_authority: index.authority.map(hex::encode),
                ast_collection: index.collection.map(hex::encode),
                ast_is_collection_verified: index.is_collection_verified,
                ast_is_burnt: index.is_burnt,
                ast_is_compressible: index.is_compressible,
                ast_is_compressed: index.is_compressed,
                ast_is_frozen: index.is_frozen,
                ast_supply: index.supply,
                ast_metadata_url_id: metadata_url.map(|(k, _)| k).map(hex::encode),
                ast_slot_updated: index.slot_updated,
            };
            assets_writer.serialize(record).map_err(|e| e.to_string())?;
            // metadata_writer.serialize(asset)?;
            // creators_writer.serialize(asset)?;
            // assets_writer.serialize(asset)?;
        }

        Ok(())
    }
}
