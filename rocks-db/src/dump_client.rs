use crate::{
    key_encoders::decode_pubkey,
    storage_traits::{AssetIndexReader, Dumper},
    Storage,
};
use async_trait::async_trait;
use csv::{Writer, WriterBuilder};
use entities::enums::{
    OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
};
use hex;
use inflector::Inflector;
use serde::{Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
};

const ONE_G: usize = 1024 * 1024 * 1024;
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
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut metadata_key_set = HashSet::new();
        let mut batch = Vec::with_capacity(batch_size);
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
                return Err("dump cancelled".to_string());
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
            let metadata_url = index
                .metadata_url
                .map(|url| (url.get_metadata_id(), url.metadata_url.trim().to_owned()));
            if let Some((ref metadata_key, ref url)) = metadata_url {
                if !metadata_key_set.contains(metadata_key) {
                    metadata_key_set.insert(metadata_key.clone());
                    metadata_writer
                        .serialize((Self::encode(metadata_key), url.to_string(), "pending"))
                        .map_err(|e| e.to_string())?;
                }
            }
            for creator in index.creators {
                creators_writer
                    .serialize((
                        Self::encode(key.to_bytes()),
                        Self::encode(creator.creator),
                        creator.creator_verified,
                        index.slot_updated,
                    ))
                    .map_err(|e| e.to_string())?;
            }
            let record = AssetRecord {
                ast_pubkey: Self::encode(key.to_bytes()),
                ast_specification_version: index.specification_version,
                ast_specification_asset_class: index.specification_asset_class,
                ast_royalty_target_type: index.royalty_target_type,
                ast_royalty_amount: index.royalty_amount,
                ast_slot_created: index.slot_created,
                ast_owner_type: index.owner_type,
                ast_owner: index.owner.map(Self::encode),
                ast_delegate: index.delegate.map(Self::encode),
                ast_authority: index.authority.map(Self::encode),
                ast_collection: index.collection.map(Self::encode),
                ast_is_collection_verified: index.is_collection_verified,
                ast_is_burnt: index.is_burnt,
                ast_is_compressible: index.is_compressible,
                ast_is_compressed: index.is_compressed,
                ast_is_frozen: index.is_frozen,
                ast_supply: index.supply,
                ast_metadata_url_id: metadata_url.map(|(k, _)| k).map(Self::encode),
                ast_slot_updated: index.slot_updated,
            };
            assets_writer.serialize(record).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn encode<T: AsRef<[u8]>>(v: T) -> String {
        format!("\\x{}", hex::encode(v))
    }
}

#[async_trait]
impl Dumper for Storage {
    async fn dump_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let metadata_path = base_path.join("metadata.csv").to_str().map(str::to_owned);
        if metadata_path.is_none() {
            return Err("invalid path".to_string());
        }
        let creators_path = base_path.join("creators.csv").to_str().map(str::to_owned);
        if creators_path.is_none() {
            return Err("invalid path".to_string());
        }
        let assets_path = base_path.join("assets.csv").to_str().map(str::to_owned);
        if assets_path.is_none() {
            return Err("invalid path".to_string());
        }
        let authorities_path = base_path.join("assets_authorities.csv").to_str().map(str::to_owned);
        if authorities_path.is_none() {
            return Err("invalid path".to_string());
        }
        tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}",
            metadata_path,
            creators_path,
            assets_path
        );
        let metadata_file = File::create(metadata_path.unwrap()).unwrap();
        let assets_file = File::create(assets_path.unwrap()).unwrap();
        let creators_file = File::create(creators_path.unwrap()).unwrap();
        // Wrap each file in a BufWriter
        let metadata_buf_writer = BufWriter::with_capacity(ONE_G, metadata_file);
        let assets_buf_writer = BufWriter::with_capacity(ONE_G, assets_file);
        let creators_buf_writer = BufWriter::with_capacity(ONE_G, creators_file);
        let mut metadata_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(metadata_buf_writer);
        let mut assets_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(assets_buf_writer);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(creators_buf_writer);

        self.dump_csv(
            &mut metadata_writer,
            &mut creators_writer,
            &mut assets_writer,
            batch_size,
            rx,
        )
        .await?;
        Ok(())
    }
}
