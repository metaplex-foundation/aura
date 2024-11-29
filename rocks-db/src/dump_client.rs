use crate::asset::MplCoreCollectionAuthority;
use crate::asset_generated::asset as fb;
use crate::column::TypedColumn;
use crate::key_encoders::encode_u64x2_pubkey;
use crate::storage_traits::AssetUpdatedKey;
use crate::{column::Column, storage_traits::Dumper, Storage};
use async_trait::async_trait;
use bincode::deserialize;
use csv::WriterBuilder;
use entities::enums::AssetType;
use entities::models::SplMint;
use entities::{
    enums::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    models::{TokenAccount, UrlWithStatus},
};
use hex;
use inflector::Inflector;
use metrics_utils::SynchronizerMetricsConfig;
use serde::{Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufWriter,
    sync::Arc,
};
use tokio::sync::Mutex;
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

impl Dumper for Storage {
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
    fn dump_nft_csv(
        &self,
        assets_file: File,
        creators_file: File,
        authority_file: File,
        metadata_file: File,
        buf_capacity: usize,
        asset_limit: Option<usize>,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<usize, String> {
        let mut metadata_key_set = HashSet::new();

        let mut core_collection_authorities: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut core_collections_iter = self
            .db
            .raw_iterator_cf(&self.mpl_core_collection_authorities.handle());
        core_collections_iter.seek_to_first();
        while core_collections_iter.valid() {
            let key = core_collections_iter.key().unwrap();
            let value = core_collections_iter.value().unwrap();
            if let Ok(value) = bincode::deserialize::<MplCoreCollectionAuthority>(value) {
                if let Some(authority) = value.authority.value {
                    core_collection_authorities.insert(key.to_vec(), authority.to_bytes().to_vec());
                }
            }
            core_collections_iter.next();
        }

        let buf_writer = BufWriter::with_capacity(buf_capacity, assets_file);

        let mut asset_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        let buf_writer = BufWriter::with_capacity(buf_capacity, authority_file);
        let mut authority_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);
        let buf_writer = BufWriter::with_capacity(buf_capacity, creators_file);
        let mut creators_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);
        let buf_writer = BufWriter::with_capacity(buf_capacity, metadata_file);
        let mut metadata_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        // Iteration over `asset_data` column via CUSTOM iterator.
        let mut iter = self.db.raw_iterator_cf(&self.asset_data.handle());
        if let Some(start_pubkey) = start_pubkey {
            iter.seek(&start_pubkey.to_bytes());
        } else {
            iter.seek_to_first();
        }
        let end_pubkey = end_pubkey.map(|pk| pk.to_bytes());
        let mut cnt = 0;
        while iter.valid() {
            if let Some(end_pubkey) = end_pubkey {
                if iter.key().unwrap() > end_pubkey.as_slice() {
                    break;
                }
            }
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
            let metadata_url_id = asset
                .dynamic_details()
                .and_then(|dd| dd.url())
                .and_then(|url| url.value())
                .filter(|s| !s.is_empty())
                .map(|s| UrlWithStatus::new(s, false))
                .filter(|uws| !uws.metadata_url.is_empty())
                .map(|uws| {
                    let metadata_key = uws.get_metadata_id();
                    if !metadata_key_set.contains(&metadata_key) {
                        metadata_key_set.insert(metadata_key.clone());
                        if let Err(e) = metadata_writer.serialize((
                            Self::encode(&metadata_key),
                            uws.metadata_url.to_string(),
                            "pending".to_string(),
                        )) {
                            error!("Error writing metadata to csv: {:?}", e);
                        }
                        synchronizer_metrics.inc_num_of_records_written("metadata", 1);
                    }
                    metadata_key
                });

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
            let core_collection_update_authority = asset
                .collection()
                .and_then(|c| c.collection())
                .and_then(|c| c.value())
                .and_then(|c| core_collection_authorities.get(c.bytes()))
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
                    if core_collection_update_authority.is_some() {
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
                ast_metadata_url_id: metadata_url_id.map(Self::encode),
                ast_slot_updated: slot_updated,
            };

            if let Err(e) = asset_writer.serialize(record) {
                error!("Error writing asset to csv: {:?}", e);
            }
            synchronizer_metrics.inc_num_of_records_written("asset", 1);
            // the authority key is the collection key if the core collection update authority is set, or the asset key itself
            // for the asset keys, we could write those without the need to check if they are already written,
            // and for the collection keys, we just skip as those will be written as part of the core collection account dump
            // todo: this was refactored, need to be verified as part of https://linear.app/mplx/issue/MTG-979/fix-test-mpl-core-get-assets-by-authority
            if core_collection_update_authority.is_none() {
                let authority_key = if core_collection_update_authority.is_some() {
                    collection
                } else {
                    Some(encoded_key)
                };
                let authority = core_collection_update_authority.or(authority);
                if let (Some(authority_key), Some(authority)) = (authority_key, authority) {
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
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
            iter.next();
            cnt += 1;
            if let Some(limit) = asset_limit {
                if cnt >= limit {
                    break;
                }
            }
            synchronizer_metrics.inc_num_of_assets_iter("asset", 1);
        }
        asset_writer.flush().map_err(|e| e.to_string())?;
        authority_writer.flush().map_err(|e| e.to_string())?;
        creators_writer.flush().map_err(|e| e.to_string())?;
        metadata_writer.flush().map_err(|e| e.to_string())?;
        info!("asset writers are flushed.");
        Ok(cnt)
    }

    /// Dumps data into several into `CSV file` dedicated for fungible tokens,
    ///     which corresponds to a separate table in the index database (`Postgres`).
    ///
    /// # Args:
    /// `fungible_tokens_file_and_path` - The file and path whose data will be written to the corresponding `fungible_tokens` table.
    /// `batch_size` - Batch size.
    /// `rx` - Channel for graceful shutdown.
    #[allow(clippy::too_many_arguments)]
    fn dump_fungible_csv(
        &self,
        fungible_tokens_file_and_path: (File, String),
        buf_capacity: usize,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<usize, String> {
        let column: Column<TokenAccount> = Self::column(self.db.clone(), self.red_metrics.clone());

        let buf_writer = BufWriter::with_capacity(buf_capacity, fungible_tokens_file_and_path.0);
        let mut iter = self.db.raw_iterator_cf(&column.handle());
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        if let Some(start_pubkey) = start_pubkey {
            iter.seek(&start_pubkey.to_bytes());
        } else {
            iter.seek_to_first();
        }
        let end_pubkey = end_pubkey.map(|pk| pk.to_bytes());
        let mut cnt = 0;
        while iter.valid() {
            if let Some(end_pubkey) = end_pubkey {
                if iter.key().unwrap() > end_pubkey.as_slice() {
                    break;
                }
            }
            if !rx.is_empty() {
                info!("Shutdown signal received...");
                return Ok(cnt);
            }
            if let Some(token) = iter
                .value()
                .map(deserialize::<TokenAccount>)
                .map(|v| v.ok())
                .flatten()
            {
                if let Err(e) = writer
                    .serialize((
                        Self::encode(token.pubkey),
                        Self::encode(token.owner),
                        Self::encode(token.mint),
                        token.amount,
                        token.slot_updated,
                    ))
                    .map_err(|e| e.to_string())
                {
                    let msg = format!(
                        "Error while writing data into {:?}. Err: {:?}",
                        fungible_tokens_file_and_path.1, e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
                synchronizer_metrics.inc_num_of_assets_iter("token_account", 1);
            }
            iter.next();
        }

        if let Err(e) = writer.flush().map_err(|e| e.to_string()) {
            let msg = format!(
                "Error happened during flushing data to {:?}. Err: {:?}",
                fungible_tokens_file_and_path.1, e
            );
            error!("{}", msg);
            return Err(msg);
        }

        info!("Finish dumping fungible assets.");
        Ok(cnt)
    }
}
impl Storage {
    fn encode<T: AsRef<[u8]>>(v: T) -> String {
        format!("\\x{}", hex::encode(v))
    }

    pub fn dump_metadata(
        file: File,
        buf_capacity: usize,
        metadata: HashSet<String>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), String> {
        let buf_writer = BufWriter::with_capacity(buf_capacity, file);
        let mut metadata_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        metadata.into_iter().for_each(|s| {
            let url = s;
            let metadata_key = UrlWithStatus::get_metadata_id_for(&url);
            if let Err(e) = metadata_writer.serialize((
                Self::encode(metadata_key),
                url.to_string(),
                "pending".to_string(),
            )) {
                error!("Error writing metadata to csv: {:?}", e);
            }
            synchronizer_metrics.inc_num_of_records_written("metadata", 1);
        });
        metadata_writer.flush().map_err(|e| e.to_string())
    }

    pub fn dump_last_keys(
        file: File,
        last_known_nft_key: AssetUpdatedKey,
        last_known_fungible_key: AssetUpdatedKey,
    ) -> Result<(), String> {
        let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
        let nft_key = encode_u64x2_pubkey(
            last_known_nft_key.seq,
            last_known_nft_key.slot,
            last_known_nft_key.pubkey,
        );
        let fungible_key = encode_u64x2_pubkey(
            last_known_fungible_key.seq,
            last_known_fungible_key.slot,
            last_known_fungible_key.pubkey,
        );
        writer
            .serialize((AssetType::NonFungible as i32, Self::encode(nft_key)))
            .map_err(|e| e.to_string())?;
        writer
            .serialize((AssetType::Fungible as i32, Self::encode(fungible_key)))
            .map_err(|e| e.to_string())?;
        writer.flush().map_err(|e| e.to_string())
    }
}

// #[async_trait]
impl Storage {
    /// The `dump_db` function is an asynchronous method responsible for dumping fungible database content into a dedicated `CSV file`.
    /// The function supports batch processing and listens to a signal using a `tokio::sync::broadcast::Receiver` to handle cancellation
    ///     or control flow.
    /// # Args:
    /// * `base_path` - A reference to a Path that specifies the base directory where the `CSV files` will be created.
    ///     The function will append filenames (`metadata.csv, creators.csv, assets.csv, assets_authorities.csv`) to this path.
    /// * `batch_size` - The size of the data batches to be processed and written to the files.
    /// * `rx` - A receiver that listens for cancellation signals.
    async fn dump_fungible_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), String> {
        let fungible_tokens_path = base_path
            .join("fungible_tokens.csv")
            .to_str()
            .map(str::to_owned);
        tracing::info!("Dumping to fungible_tokens: {:?}", fungible_tokens_path);

        let fungible_tokens_file = File::create(fungible_tokens_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for fungible tokens dump: {}", e))?;

        self.dump_fungible_csv(
            (fungible_tokens_file, fungible_tokens_path.unwrap()),
            BUF_CAPACITY,
            None,
            None,
            rx,
            synchronizer_metrics,
        )?;
        Ok(())
    }

    /// The `dump_db` function is an asynchronous method responsible for dumping database content into multiple `CSV files`.
    /// It writes metadata, asset information, creator details, and asset authorities to separate `CSV files` in the provided directory.
    /// The function supports batch processing and listens to a signal using a `tokio::sync::broadcast::Receiver` to handle cancellation
    ///     or control flow.
    /// # Args:
    /// * `base_path` - A reference to a Path that specifies the base directory where the `CSV files` will be created.
    ///     The function will append filenames (`metadata.csv, creators.csv, assets.csv, assets_authorities.csv`) to this path.
    /// * `batch_size` - The size of the data batches to be processed and written to the files.
    /// * `rx` - A receiver that listens for cancellation signals.
    async fn dump_nft_db(
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
        if authorities_path.is_none() {
            return Err("invalid path".to_string());
        }
        tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}, authorities: {:?}",
            metadata_path,
            creators_path,
            assets_path,
            authorities_path,
        );

        let metadata_file = File::create(metadata_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;
        let assets_file = File::create(assets_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
        let creators_file = File::create(creators_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
        let authority_file = File::create(authorities_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for authority dump: {}", e))?;

        let _cnt = self
            .dump_nft_csv(
                assets_file,
                creators_file,
                authority_file,
                metadata_file,
                BUF_CAPACITY,
                None,
                None,
                None,
                rx,
                synchronizer_metrics.clone(),
            )?;
        // info!("metadata dump started");
        // Self::dump_metadata(metadata_file, BUF_CAPACITY, metadata, synchronizer_metrics)?;
        // info!("metadata dump finished");
        Ok(())
    }
}
