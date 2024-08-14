use crate::{
    key_encoders::decode_pubkey,
    storage_traits::{AssetIndexReader, Dumper},
    Storage,
};
use async_trait::async_trait;
use csv::WriterBuilder;
use entities::{
    enums::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    models::AssetIndex,
};
use hex;
use inflector::Inflector;
use log::{error, info};
use serde::{Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufWriter,
    sync::Arc,
};
use tokio::{sync::mpsc, task::JoinError};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};
use usecase::graceful_stop::graceful_stop;

const MPSC_BUFFER_SIZE: usize = 1_000_000;
const ITERATION_WORKERS: usize = 3;

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
    pub async fn dump_csv(
        &self,
        metadata_file_and_path: (File, String),
        assets_file_and_path: (File, String),
        creators_file_and_path: (File, String),
        authority_file_and_path: (File, String),
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let mut iterator_tasks = JoinSet::new();
        let mut writer_tasks = JoinSet::new();

        let (iterator_shutdown_tx, iterator_shutdown_rx) = broadcast::channel::<()>(1);
        let (writer_shutdown_tx, writer_shutdown_rx) = broadcast::channel::<()>(1);

        let (tx_indexes, rx_indexes) = async_channel::unbounded();

        // Launch async tokio task for each worker which writes data to csv file.
        // As a result they process every type of data independently.
        let (tx_metadata, rx_metadata) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        writer_tasks.spawn(async move {
            Self::write_to_file(
                metadata_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_metadata,
            )
            .await
        });

        let (tx_assets, rx_assets) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        writer_tasks.spawn(async move {
            Self::write_to_file(assets_file_and_path, rx_cloned, shutdown_cloned, rx_assets).await
        });

        let (tx_creators, rx_creators) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        writer_tasks.spawn(async move {
            Self::write_to_file(
                creators_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_creators,
            )
            .await
        });

        let (tx_authority, rx_authority) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        writer_tasks.spawn(async move {
            Self::write_to_file(
                authority_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_authority,
            )
            .await
        });

        let metadata_key_set = Arc::new(Mutex::new(HashSet::new()));
        let authorities_key_set = Arc::new(Mutex::new(HashSet::new()));

        // Launch N workers which iterates over index data - asset's data selected from RocksDB.
        // During that iteration it splits asset's data and push it to appropriate
        // channel so "file writers" could process it
        for _ in 0..ITERATION_WORKERS {
            let rx_cloned = rx.resubscribe();
            let shutdown_cloned = iterator_shutdown_rx.resubscribe();
            let rx_indexes_cloned = rx_indexes.clone();
            let tx_metadata_cloned = tx_metadata.clone();
            let tx_creators_cloned = tx_creators.clone();
            let tx_assets_cloned = tx_assets.clone();
            let tx_authority_cloned = tx_authority.clone();
            let metadata_key_set_cloned = metadata_key_set.clone();
            let authorities_key_set_cloned = authorities_key_set.clone();
            iterator_tasks.spawn(async move {
                Self::iterate_over_indexes(
                    rx_cloned,
                    shutdown_cloned,
                    rx_indexes_cloned,
                    tx_metadata_cloned,
                    tx_creators_cloned,
                    tx_assets_cloned,
                    tx_authority_cloned,
                    metadata_key_set_cloned,
                    authorities_key_set_cloned,
                )
                .await
            });
        }

        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut batch = Vec::with_capacity(batch_size);
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            batch.push(k);
            if batch.len() == batch_size {
                let indexes = self
                    .get_asset_indexes(batch.as_ref())
                    .await
                    .map_err(|e| e.to_string())?;
                tx_indexes
                    .send(indexes)
                    .await
                    .map_err(|e| format!("Error sending asset indexes to channel: {}", e))?;

                batch.clear();
            }
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
        }

        if !batch.is_empty() {
            let indexes = self
                .get_asset_indexes(batch.as_ref())
                .await
                .map_err(|e| e.to_string())?;
            tx_indexes
                .send(indexes)
                .await
                .map_err(|e| format!("Error sending asset indexes to channel: {}", e))?;
        }

        // Once we iterate through all the assets in RocksDB we have to send stop signal
        // to iterators and wait until they finish it's job. Because that workers populate channel
        // for writers.
        iterator_shutdown_tx
            .send(())
            .map_err(|e| format!("Error sending stop signal for indexes iterator: {}", e))?;
        info!("Stopping iterators...");
        graceful_stop(&mut iterator_tasks).await;
        info!("All iterators are stopped.");

        // Once iterators are stopped it's safe to shutdown writers.
        writer_shutdown_tx
            .send(())
            .map_err(|e| format!("Error sending stop signal for file writers: {}", e))?;
        info!("Stopping writers...");
        graceful_stop(&mut writer_tasks).await;
        info!("All writers are stopped.");

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn iterate_over_indexes(
        rx_cloned: tokio::sync::broadcast::Receiver<()>,
        shutdown_cloned: tokio::sync::broadcast::Receiver<()>,
        rx_indexes_cloned: async_channel::Receiver<HashMap<Pubkey, AssetIndex>>,
        tx_metadata_cloned: tokio::sync::mpsc::Sender<(String, String, String)>,
        tx_creators_cloned: tokio::sync::mpsc::Sender<(String, String, bool, i64)>,
        tx_assets_cloned: tokio::sync::mpsc::Sender<AssetRecord>,
        tx_authority_cloned: tokio::sync::mpsc::Sender<(String, String, i64)>,
        metadata_key_set: Arc<Mutex<HashSet<Vec<u8>>>>,
        authorities_key_set: Arc<Mutex<HashSet<Pubkey>>>,
    ) -> Result<(), JoinError> {
        loop {
            // whole application is stopped
            if !rx_cloned.is_empty() {
                break;
            }
            // process with data collection stopped
            if !shutdown_cloned.is_empty() && rx_indexes_cloned.is_empty() {
                break;
            }

            if rx_indexes_cloned.is_empty() {
                continue;
            } else if let Ok(indexes) = rx_indexes_cloned.try_recv() {
                for (key, index) in indexes {
                    let metadata_url = index
                        .metadata_url
                        .map(|url| (url.get_metadata_id(), url.metadata_url.trim().to_owned()));
                    if let Some((ref metadata_key, ref url)) = metadata_url {
                        {
                            let mut metadata_keys = metadata_key_set.lock().await;
                            if !metadata_keys.contains(metadata_key) {
                                metadata_keys.insert(metadata_key.clone());
                                if let Err(e) = tx_metadata_cloned
                                    .send((
                                        Self::encode(metadata_key),
                                        url.to_string(),
                                        "pending".to_string(),
                                    ))
                                    .await
                                {
                                    error!("Error sending message: {:?}", e);
                                }
                            }
                        }
                    }
                    for creator in index.creators {
                        if let Err(e) = tx_creators_cloned
                            .send((
                                Self::encode(key.to_bytes()),
                                Self::encode(creator.creator),
                                creator.creator_verified,
                                index.slot_updated,
                            ))
                            .await
                        {
                            error!("Error sending message: {:?}", e);
                        }
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
                        ast_authority_fk: if let Some(collection) = index.collection {
                            if index.update_authority.is_some() {
                                Some(Self::encode(collection))
                            } else if index.authority.is_some() {
                                Some(Self::encode(index.pubkey))
                            } else {
                                None
                            }
                        } else if index.authority.is_some() {
                            Some(Self::encode(index.pubkey))
                        } else {
                            None
                        },
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
                    if let Err(e) = tx_assets_cloned.send(record).await {
                        error!("Error sending message: {:?}", e);
                    }
                    let authority = index.update_authority.or(index.authority);
                    let authority_key = if index.update_authority.is_some() {
                        index.collection
                    } else {
                        Some(key)
                    };
                    if let (Some(authority_key), Some(authority)) = (authority_key, authority) {
                        {
                            let mut authorities_keys = authorities_key_set.lock().await;
                            if !authorities_keys.contains(&authority_key) {
                                authorities_keys.insert(authority_key);
                                if let Err(e) = tx_authority_cloned
                                    .send((
                                        Self::encode(authority_key.to_bytes()),
                                        Self::encode(authority.to_bytes()),
                                        index.slot_updated,
                                    ))
                                    .await
                                {
                                    error!("Error sending message: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn write_to_file<T: Serialize>(
        file_and_path: (File, String),
        application_shutdown: tokio::sync::broadcast::Receiver<()>,
        worker_shutdown: tokio::sync::broadcast::Receiver<()>,
        mut data_channel: tokio::sync::mpsc::Receiver<T>,
    ) -> Result<(), JoinError> {
        let buf_writer = BufWriter::with_capacity(ONE_G, file_and_path.0);
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        loop {
            if !application_shutdown.is_empty() {
                break;
            }

            if !worker_shutdown.is_empty() && data_channel.is_empty() {
                break;
            }

            if data_channel.is_empty() {
                continue;
            } else if let Ok(k) = data_channel.try_recv() {
                if let Err(e) = writer.serialize(k).map_err(|e| e.to_string()) {
                    error!(
                        "Error while writing data into {:?}. Err: {:?}",
                        file_and_path.1, e
                    );
                }
            }
        }
        if let Err(e) = writer.flush().map_err(|e| e.to_string()) {
            error!(
                "Error happened during flushing data to {:?}. Err: {:?}",
                file_and_path.1, e
            );
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
        let authorities_path = base_path
            .join("assets_authorities.csv")
            .to_str()
            .map(str::to_owned);
        if authorities_path.is_none() {
            return Err("invalid path".to_string());
        }
        tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}, authorities: {:?}",
            metadata_path,
            creators_path,
            assets_path,
            authorities_path
        );

        let metadata_file = File::create(metadata_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;
        let assets_file = File::create(assets_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
        let creators_file = File::create(creators_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
        let authority_file = File::create(authorities_path.clone().unwrap())
            .map_err(|e| format!("Could not create file for authority dump: {}", e))?;

        self.dump_csv(
            (metadata_file, metadata_path.unwrap()),
            (assets_file, assets_path.unwrap()),
            (creators_file, creators_path.unwrap()),
            (authority_file, authorities_path.unwrap()),
            batch_size,
            rx,
        )
        .await?;
        Ok(())
    }
}
