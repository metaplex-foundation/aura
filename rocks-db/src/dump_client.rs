use crate::asset::MplCoreCollectionAuthority;
use crate::asset_generated::asset as fb;
use crate::column::TypedColumn;
use crate::{column::Column, storage_traits::Dumper, Storage};
use async_trait::async_trait;
use csv::WriterBuilder;
use entities::models::SplMint;
use entities::{
    enums::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    models::{TokenAccount, UrlWithStatus},
};
use hex;
use inflector::Inflector;
use metrics_utils::SynchronizerMetricsConfig;
use serde::{Serialize, Serializer};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufWriter,
    sync::Arc,
};
use tokio::time::Instant;
use tracing::{error, info};

const BUF_CAPACITY: usize = 1024 * 1024 * 32;

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
    ast_authority_fk: Option<String>,
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
    /// Concurrently dumps data into several `CSV files`,
    ///     where each file corresponds to a separate table in the index database (`Postgres`).
    ///
    /// # Args:
    /// `metadata_file_and_path` - The file and path whose data will be written to the corresponding `tasks` table.
    /// `assets_file_and_path` - The file and path whose data will be written to the corresponding `assets_v3` table.
    /// `creators_file_and_path` - The file and path whose data will be written to the corresponding `asset_creators_v3` table.
    /// `authority_file_and_path` - The file and path whose data will be written to the corresponding `assets_authorities` table.
    /// `batch_size` - Batch size.
    /// `rx` - Channel for graceful shutdown.
    #[allow(clippy::too_many_arguments)]
    pub async fn dump_csv(
        &self,
        metadata_file_and_path: (File, String),
        assets_file_and_path: (File, String),
        creators_file_and_path: (File, String),
        authority_file_and_path: (File, String),
        fungible_tokens_file_and_path: (File, String),
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), String> {
        // dump fungible assets in separate blocking thread
        let column: Column<TokenAccount> = Self::column(self.db.clone(), self.red_metrics.clone());
        let cloned_metrics = synchronizer_metrics.clone();
        let fungible_assets_join = tokio::task::spawn_blocking(move || {
            Self::dump_fungible_assets(
                column,
                fungible_tokens_file_and_path,
                batch_size,
                cloned_metrics,
            )
        });

        let mut core_collections: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut core_collections_iter = self
            .db
            .raw_iterator_cf(&self.mpl_core_collection_authorities.handle());
        core_collections_iter.seek_to_first();
        while core_collections_iter.valid() {
            let key = core_collections_iter.key().unwrap();
            let value = core_collections_iter.value().unwrap();
            if let Ok(value) = bincode::deserialize::<MplCoreCollectionAuthority>(value) {
                if let Some(authority) = value.authority.value {
                    core_collections.insert(key.to_vec(), authority.to_bytes().to_vec());
                }
            }
            core_collections_iter.next();
        }

        let mut metadata_key_set = HashSet::new();
        let mut authorities_key_set = HashSet::new();

        let buf_writer = BufWriter::with_capacity(BUF_CAPACITY, assets_file_and_path.0);

        let mut asset_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        let buf_writer = BufWriter::with_capacity(BUF_CAPACITY, authority_file_and_path.0);
        let mut authority_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);
        let buf_writer = BufWriter::with_capacity(BUF_CAPACITY, creators_file_and_path.0);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        let buf_writer = BufWriter::with_capacity(BUF_CAPACITY, metadata_file_and_path.0);
        let mut metadata_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        // Iteration over `asset_data` column via CUSTOM iterator.
        let mut iter = self.db.raw_iterator_cf(&self.asset_data.handle());
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key().unwrap();
            let encoded_key = Self::encode(key);
            let value = iter.value().unwrap();
            let asset;
            unsafe {
                asset = fb::root_as_asset_complete_details_unchecked(value);
            }
            if asset.static_details().is_none() {
                iter.next();
                continue;
            }
            // this will slow down the sync, but it is necessary to determine if the asset is an NFT
            // TODO: optimize this
            let mut sac: SpecificationAssetClass = asset
                .static_details()
                .map(|static_details| static_details.specification_asset_class().into())
                .unwrap_or_default();
            if sac == SpecificationAssetClass::Unknown
                || sac == SpecificationAssetClass::FungibleToken
                || sac == SpecificationAssetClass::FungibleAsset
            {
                // get the spl token account and check its supply and decimals
                // if those are 1 and 0, then it is not a fungible asset, but an NFT
                let ta = self
                    .db
                    .get_cf(&self.db.cf_handle(SplMint::NAME).unwrap(), key);
                if let Ok(Some(ta)) = ta {
                    if let Ok(ta) = bincode::deserialize::<SplMint>(&ta) {
                        if ta.is_nft() {
                            sac = SpecificationAssetClass::Nft;
                        }
                    }
                }
            }
            let metadata_url = asset
                .dynamic_details()
                .and_then(|dd| dd.url())
                .and_then(|url| url.value())
                .filter(|s| !s.is_empty())
                .map(|s| (UrlWithStatus::get_metadata_id_for(s), s));
            if let Some((ref metadata_key, ref url)) = metadata_url {
                {
                    if !metadata_key_set.contains(metadata_key) {
                        metadata_key_set.insert(metadata_key.clone());
                        if let Err(e) = metadata_writer.serialize((
                            Self::encode(metadata_key),
                            url.to_string(),
                            "pending".to_string(),
                        )) {
                            error!("Error writing metadata to csv: {:?}", e);
                        }
                        synchronizer_metrics.inc_num_of_records_written("metadata", 1);
                    }
                }
            }

            let slot_updated = asset.get_slot_updated() as i64;
            if let Some(cc) = asset
                .dynamic_details()
                .and_then(|d| d.creators())
                .and_then(|u| u.value())
            {
                for creator in cc {
                    let c_key = creator.creator().unwrap().bytes();
                    if let Err(e) = creators_writer.serialize((
                        encoded_key.clone(),
                        Self::encode(c_key),
                        creator.creator_verified(),
                        slot_updated,
                    )) {
                        error!("Error writing creator to csv: {:?}", e);
                    }
                    synchronizer_metrics.inc_num_of_records_written("creators", 1);
                }
            }
            let update_authority = asset
                .collection()
                .and_then(|c| c.collection())
                .and_then(|c| c.value())
                .and_then(|c| core_collections.get(c.bytes()))
                .map(|b| b.to_owned());
            let authority = asset
                .authority()
                .and_then(|a| a.authority())
                .map(|a| a.bytes().to_vec());
            let collection = asset
                .collection()
                .and_then(|c| c.collection())
                .and_then(|uc| uc.value())
                .map(|c| Self::encode(c.bytes()));
            let record = AssetRecord {
                ast_pubkey: encoded_key.clone(),
                ast_specification_version: SpecificationVersions::V1,
                ast_specification_asset_class: sac,
                ast_royalty_target_type: asset
                    .static_details()
                    .map(|static_details| static_details.royalty_target_type().into())
                    .unwrap_or_default(),
                ast_royalty_amount: asset
                    .dynamic_details()
                    .and_then(|d| d.royalty_amount())
                    .map(|ra| ra.value())
                    .unwrap_or_default() as i64,
                ast_slot_created: asset
                    .static_details()
                    .map(|static_details| static_details.created_at())
                    .unwrap_or_default(),
                ast_owner_type: asset
                    .owner()
                    .and_then(|o| o.owner_type().map(|o| OwnerType::from(o.value()))),
                ast_owner: asset
                    .owner()
                    .and_then(|o| o.owner())
                    .and_then(|o| o.value())
                    .map(|v| v.bytes())
                    .map(Self::encode),
                ast_delegate: asset
                    .owner()
                    .and_then(|o| o.delegate())
                    .and_then(|o| o.value())
                    .map(|v| v.bytes())
                    .map(Self::encode),
                ast_authority_fk: if let Some(collection) = collection.as_ref() {
                    if update_authority.is_some() {
                        Some(collection.to_owned())
                    } else if authority.is_some() {
                        Some(encoded_key.clone())
                    } else {
                        None
                    }
                } else if authority.is_some() {
                    Some(encoded_key.clone())
                } else {
                    None
                },
                ast_collection: collection.clone(),
                ast_is_collection_verified: asset
                    .collection()
                    .and_then(|c| c.is_collection_verified())
                    .map(|v| v.value()),
                ast_is_burnt: asset
                    .dynamic_details()
                    .and_then(|d| d.is_burnt())
                    .map(|v| v.value())
                    .unwrap_or_default(),
                ast_is_compressible: asset
                    .dynamic_details()
                    .and_then(|d| d.is_compressible())
                    .map(|v| v.value())
                    .unwrap_or_default(),
                ast_is_compressed: asset
                    .dynamic_details()
                    .and_then(|d| d.is_compressed())
                    .map(|v| v.value())
                    .unwrap_or_default(),
                ast_is_frozen: asset
                    .dynamic_details()
                    .and_then(|d| d.is_frozen())
                    .map(|v| v.value())
                    .unwrap_or_default(),
                ast_supply: asset
                    .dynamic_details()
                    .and_then(|d| d.supply())
                    .map(|v| v.value() as i64),
                ast_metadata_url_id: metadata_url.map(|(k, _)| k).map(Self::encode),
                ast_slot_updated: slot_updated,
            };

            if let Err(e) = asset_writer.serialize(record) {
                error!("Error writing asset to csv: {:?}", e);
            }
            synchronizer_metrics.inc_num_of_records_written("asset", 1);
            let authority_key = if update_authority.is_some() {
                collection
            } else {
                Some(encoded_key)
            };
            let authority = update_authority.or(authority);
            if let (Some(authority_key), Some(authority)) = (authority_key, authority) {
                {
                    if !authorities_key_set.contains(&authority_key) {
                        authorities_key_set.insert(authority_key.clone());
                        if let Err(e) = authority_writer.serialize((
                            authority_key,
                            Self::encode(authority),
                            slot_updated,
                        )) {
                            error!("Error writing authority to csv: {:?}", e);
                        }
                        synchronizer_metrics.inc_num_of_records_written("authority", 1);
                    }
                }
            }
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
            iter.next();
            synchronizer_metrics.inc_num_of_assets_iter("asset", 1);
        }
        _ = tokio::try_join!(
            tokio::task::spawn_blocking(move || asset_writer.flush()),
            tokio::task::spawn_blocking(move || authority_writer.flush()),
            tokio::task::spawn_blocking(move || creators_writer.flush()),
            tokio::task::spawn_blocking(move || metadata_writer.flush())
        )
        .map_err(|e| e.to_string())?;

        info!("asset writers are flushed.");
        if let Err(e) = fungible_assets_join.await {
            error!(
                "Error happened during fungible assets dumping: {}",
                e.to_string()
            );
        }
        info!("Finish dumping fungible assets.");

        Ok(())
    }

    fn dump_fungible_assets(
        storage: Column<TokenAccount>,
        file_and_path: (File, String),
        batch_size: usize,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) {
        let buf_writer = BufWriter::with_capacity(BUF_CAPACITY, file_and_path.0);
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        // asset, owner, balance, slot updated
        let mut batch: Vec<(String, String, String, i64, i64)> = Vec::new();

        for (_, token) in storage.pairs_iterator(storage.iter_start()) {
            batch.push((
                Self::encode(token.pubkey),
                Self::encode(token.owner),
                Self::encode(token.mint),
                token.amount,
                token.slot_updated,
            ));
            synchronizer_metrics.inc_num_of_assets_iter("token_account", 1);

            if batch.len() >= batch_size {
                let start = Instant::now();
                for rec in &batch {
                    if let Err(e) = writer.serialize(rec).map_err(|e| e.to_string()) {
                        error!(
                            "Error while writing data into {:?}. Err: {:?}",
                            file_and_path.1, e
                        );
                    }
                }
                batch.clear();
                synchronizer_metrics.set_file_write_time(
                    file_and_path.1.as_ref(),
                    start.elapsed().as_millis() as f64,
                );
            }
        }

        if !batch.is_empty() {
            for rec in &batch {
                let start = Instant::now();
                if let Err(e) = writer.serialize(rec).map_err(|e| e.to_string()) {
                    error!(
                        "Error while writing data into {:?}. Err: {:?}",
                        file_and_path.1, e
                    );
                }

                synchronizer_metrics.set_file_write_time(
                    file_and_path.1.as_ref(),
                    start.elapsed().as_millis() as f64,
                );
                synchronizer_metrics
                    .inc_num_of_records_written(&file_and_path.1, batch.len() as u64);
            }
            batch.clear();
        }

        if let Err(e) = writer.flush().map_err(|e| e.to_string()) {
            error!(
                "Error happened during flushing data to {:?}. Err: {:?}",
                file_and_path.1, e
            );
        }
    }

    fn encode<T: AsRef<[u8]>>(v: T) -> String {
        format!("\\x{}", hex::encode(v))
    }
}

#[async_trait]
impl Dumper for Storage {
    /// The `dump_db` function is an asynchronous method responsible for dumping database content into multiple `CSV files`.
    /// It writes metadata, asset information, creator details, and asset authorities to separate `CSV files` in the provided directory.
    /// The function supports batch processing and listens to a signal using a `tokio::sync::broadcast::Receiver` to handle cancellation
    ///     or control flow.
    /// # Args:
    /// * `base_path` - A reference to a Path that specifies the base directory where the `CSV files` will be created.
    ///     The function will append filenames (`metadata.csv, creators.csv, assets.csv, assets_authorities.csv`) to this path.
    /// * `batch_size` - The size of the data batches to be processed and written to the files.
    /// * `rx` - A receiver that listens for cancellation signals.
    async fn dump_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
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
        let authorities_path = base_path
            .join("assets_authorities.csv")
            .to_str()
            .map(str::to_owned);
        if authorities_path.is_none() {
            return Err("invalid path".to_string());
        }
        let fungible_tokens_path = base_path
            .join("fungible_tokens.csv")
            .to_str()
            .map(str::to_owned);
        if authorities_path.is_none() {
            return Err("invalid path".to_string());
        }
        tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}, authorities: {:?}, fungible_tokens: {:?}",
            metadata_path,
            creators_path,
            assets_path,
            authorities_path,
            fungible_tokens_path
        );

        let metadata_file = File::create(metadata_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;
        let assets_file = File::create(assets_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
        let creators_file = File::create(creators_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
        let authority_file = File::create(authorities_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for authority dump: {}", e))?;
        let fungible_tokens_file = File::create(fungible_tokens_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for fungible tokens dump: {}", e))?;

        self.dump_csv(
            (metadata_file, metadata_path.unwrap()),
            (assets_file, assets_path.unwrap()),
            (creators_file, creators_path.unwrap()),
            (authority_file, authorities_path.unwrap()),
            (fungible_tokens_file, fungible_tokens_path.unwrap()),
            batch_size,
            rx,
            synchronizer_metrics,
        )
        .await?;
        Ok(())
    }
}
