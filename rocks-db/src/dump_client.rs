use crate::{
    column::Column,
    key_encoders::decode_pubkey,
    storage_traits::{AssetIndexReader, Dumper},
    AssetStaticDetails, Storage,
};
use async_trait::async_trait;
use bincode::deserialize;
use csv::WriterBuilder;
use entities::{
    enums::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    models::{AssetIndex, TokenAccount},
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
use tokio::{sync::broadcast, task::JoinSet, time::Instant};
use tokio::{sync::mpsc, task::JoinError};
use tracing::{error, info};
use usecase::graceful_stop::graceful_stop;

const MPSC_BUFFER_SIZE: usize = 1_000_000;

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
    pub async fn dump_nft_csv(
        &self,
        metadata_file_and_path: (File, String),
        assets_file_and_path: (File, String),
        creators_file_and_path: (File, String),
        authority_file_and_path: (File, String),
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
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

        let cloned_metrics = synchronizer_metrics.clone();
        writer_tasks.spawn_blocking(move || {
            Self::write_to_file(
                metadata_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_metadata,
                cloned_metrics,
            )
        });

        let (tx_assets, rx_assets) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        let cloned_metrics = synchronizer_metrics.clone();
        writer_tasks.spawn_blocking(move || {
            Self::write_to_file(
                assets_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_assets,
                cloned_metrics,
            )
        });

        let (tx_creators, rx_creators) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        let cloned_metrics = synchronizer_metrics.clone();
        writer_tasks.spawn_blocking(move || {
            Self::write_to_file(
                creators_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_creators,
                cloned_metrics,
            )
        });

        let (tx_authority, rx_authority) = mpsc::channel(MPSC_BUFFER_SIZE);
        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = writer_shutdown_rx.resubscribe();

        let cloned_metrics = synchronizer_metrics.clone();
        writer_tasks.spawn_blocking(move || {
            Self::write_to_file(
                authority_file_and_path,
                rx_cloned,
                shutdown_cloned,
                rx_authority,
                cloned_metrics,
            )
        });

        let rx_cloned = rx.resubscribe();
        let shutdown_cloned = iterator_shutdown_rx.resubscribe();
        iterator_tasks.spawn(Self::iterate_over_indexes(
            rx_cloned,
            shutdown_cloned,
            rx_indexes,
            tx_metadata,
            tx_creators,
            tx_assets,
            tx_authority,
            synchronizer_metrics.clone(),
        ));

        let core_collection_keys: Vec<Pubkey> = self
            .asset_static_data
            .iter_start()
            .filter_map(|a| a.ok())
            .filter_map(|(_, v)| deserialize::<AssetStaticDetails>(v.to_vec().as_ref()).ok())
            .filter(|a| a.specification_asset_class == SpecificationAssetClass::MplCoreCollection)
            .map(|a| a.pubkey)
            .collect();

        let collections: HashMap<Pubkey, Pubkey> = self
            .asset_collection_data
            .batch_get(core_collection_keys)
            .await
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .filter_map(|asset| asset.authority.value.map(|v| (asset.pubkey, v)))
            .collect();

        // Iteration over `asset_static_data` column via CUSTOM iterator.
        let iter = self.asset_static_data.iter_start();
        let mut batch = Vec::with_capacity(batch_size);
        // Collect batch of keys.
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            synchronizer_metrics.inc_num_of_assets_iter("asset_static_data", 1);
            batch.push(k);
            // When batch is filled, find `AssetIndex` and send it to `tx_indexes` channel.
            if batch.len() >= batch_size {
                let start = chrono::Utc::now();
                let indexes = self
                    .get_nft_asset_indexes(batch.as_ref(), Some(&collections))
                    .await
                    .map_err(|e| e.to_string())?;
                self.red_metrics.observe_request(
                    "Synchronizer",
                    "get_batch_of_assets",
                    "get_asset_indexes",
                    start,
                );

                tx_indexes
                    .send(indexes)
                    .await
                    .map_err(|e| format!("Error sending asset indexes to channel: {}", e))?;

                // Clearing batch vector to continue iterating and collecting new batch.
                batch.clear();
            }
            if !rx.is_empty() {
                return Err("dump cancelled".to_string());
            }
        }

        // If there are any records left, we find the `AssetIndex` and send them to the `tx_indexes` channel.
        if !batch.is_empty() {
            let start = chrono::Utc::now();
            let indexes = self
                .get_nft_asset_indexes(batch.as_ref(), Some(&collections))
                .await
                .map_err(|e| e.to_string())?;
            self.red_metrics.observe_request(
                "Synchronizer",
                "get_batch_of_assets",
                "get_asset_indexes",
                start,
            );

            tx_indexes
                .send(indexes)
                .await
                .map_err(|e| format!("Error sending asset indexes to channel: {}", e))?;
        }

        // Once we iterate through all the assets in RocksDB we have to send stop signal
        //     to iterators and wait until they finish its job.
        // Because that workers populate channel for writers.
        iterator_shutdown_tx
            .send(())
            .map_err(|e| format!("Error sending stop signal for indexes iterator: {}", e))?;
        info!("Stopping iterators...");
        graceful_stop(&mut iterator_tasks).await;
        info!("All iterators are stopped.");

        // Once iterators are stopped it's safe to shut down writers.
        writer_shutdown_tx
            .send(())
            .map_err(|e| format!("Error sending stop signal for file writers: {}", e))?;
        info!("Stopping writers...");
        graceful_stop(&mut writer_tasks).await;
        info!("All writers are stopped.");

        Ok(())
    }

    /// Dumps data into several into `CSV file` dedicated for fungible tokens,
    ///     which corresponds to a separate table in the index database (`Postgres`).
    ///
    /// # Args:
    /// `fungible_tokens_file_and_path` - The file and path whose data will be written to the corresponding `fungible_tokens` table.
    /// `batch_size` - Batch size.
    /// `rx` - Channel for graceful shutdown.
    #[allow(clippy::too_many_arguments)]
    pub async fn dump_fungible_csv(
        &self,
        fungible_tokens_file_and_path: (File, String),
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), String> {
        let column: Column<TokenAccount> = Self::column(self.db.clone(), self.red_metrics.clone());

        let buf_writer = BufWriter::with_capacity(ONE_G, fungible_tokens_file_and_path.0);
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);

        // token_acc_key, owner, mint, balance, slot updated
        let mut batch: Vec<(String, String, String, i64, i64)> = Vec::new();

        for token in column
            .iter_start()
            .filter_map(|k| k.ok())
            .filter_map(|(_, value)| deserialize::<TokenAccount>(value.to_vec().as_ref()).ok())
        {
            if !rx.is_empty() {
                info!("Shutdown signal received...");
                return Ok(());
            }
            batch.push((
                token.pubkey.to_string(),
                token.owner.to_string(),
                token.mint.to_string(),
                token.amount,
                token.slot_updated,
            ));
            synchronizer_metrics.inc_num_of_assets_iter("token_account", 1);

            if batch.len() >= batch_size {
                let start = Instant::now();
                for rec in &batch {
                    if let Err(e) = writer.serialize(rec).map_err(|e| e.to_string()) {
                        let msg = format!(
                            "Error while writing data into {:?}. Err: {:?}",
                            fungible_tokens_file_and_path.1, e
                        );
                        error!("{}", msg);
                        return Err(msg);
                    }
                }
                batch.clear();
                synchronizer_metrics.set_file_write_time(
                    fungible_tokens_file_and_path.1.as_ref(),
                    start.elapsed().as_millis() as f64,
                );
            }
        }

        if !batch.is_empty() {
            for rec in &batch {
                if !rx.is_empty() {
                    info!("Shutdown signal received...");
                    return Ok(());
                }
                if let Err(e) = writer.serialize(rec).map_err(|e| e.to_string()) {
                    let msg = format!(
                        "Error while writing data into {:?}. Err: {:?}",
                        fungible_tokens_file_and_path.1, e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
            batch.clear();
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
        Ok(())
    }

    /// The `iterate_over_indexes` function is an asynchronous method responsible for iterating over a stream of asset indexes and processing them.
    /// It extracts `metadata`, `creators`, `assets`, and `authority` information from each index and sends this data to channels for further processing.
    /// The function listens to shut down signals to gracefully stop its operations.
    #[allow(clippy::too_many_arguments)]
    async fn iterate_over_indexes(
        rx_cloned: tokio::sync::broadcast::Receiver<()>,
        shutdown_cloned: tokio::sync::broadcast::Receiver<()>,
        rx_indexes_cloned: async_channel::Receiver<HashMap<Pubkey, AssetIndex>>,
        tx_metadata_cloned: tokio::sync::mpsc::Sender<(String, String, String)>,
        tx_creators_cloned: tokio::sync::mpsc::Sender<(String, String, bool, i64)>,
        tx_assets_cloned: tokio::sync::mpsc::Sender<AssetRecord>,
        tx_authority_cloned: tokio::sync::mpsc::Sender<(String, String, i64)>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    ) -> Result<(), JoinError> {
        let mut metadata_key_set = HashSet::new();
        let mut authorities_key_set = HashSet::new();

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
                let start = Instant::now();
                for (key, index) in indexes {
                    let metadata_url = index
                        .metadata_url
                        .map(|url| (url.get_metadata_id(), url.metadata_url.trim().to_owned()));
                    if let Some((ref metadata_key, ref url)) = metadata_url {
                        {
                            if !metadata_key_set.contains(metadata_key) {
                                metadata_key_set.insert(metadata_key.clone());
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
                                synchronizer_metrics
                                    .inc_num_of_records_sent_to_channel("metadata", 1);
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
                        synchronizer_metrics.inc_num_of_records_sent_to_channel("creators", 1);
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
                            if !authorities_key_set.contains(&authority_key) {
                                authorities_key_set.insert(authority_key);
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
                                synchronizer_metrics
                                    .inc_num_of_records_sent_to_channel("authority", 1);
                            }
                        }
                    }
                }

                synchronizer_metrics
                    .set_iter_over_assets_indexes(start.elapsed().as_millis() as f64);
            }
        }

        Ok(())
    }

    /// The `write_to_file` function is an asynchronous method responsible for writing
    ///     serialized data to a file using a buffered writer.
    /// It listens for data from a `tokio::sync::mpsc::Receiver` channel,
    ///     and the writing process is controlled by two shutdown signals: one for the application and one for the worker.
    ///
    /// # Args:
    /// `file_and_path` - A tuple containing:
    ///     A File object used for writing the serialized data.
    ///     A String representing the file path (for logging and debugging purposes).
    ///
    /// `application_shutdown` - A `broadcast::Receiver` channel that listens for an application-wide shutdown signal.
    ///     If this signal is received, the loop will terminate, and writing will stop.
    ///
    /// `worker_shutdown` - A `broadcast::Receiver` channel that listens for a worker-specific shutdown signal.
    ///     Writing will stop if both the worker shutdown signal is received and there is no more data to process in the `data_channel`.
    ///
    /// `data_channel` - An `mpsc::Receiver` channel that provides the serialized data (`T: Serialize`) to be written to the file.
    ///     Data is processed in the loop until one of the shutdown signals is triggered.
    fn write_to_file<T: Serialize>(
        file_and_path: (File, String),
        application_shutdown: tokio::sync::broadcast::Receiver<()>,
        worker_shutdown: tokio::sync::broadcast::Receiver<()>,
        mut data_channel: tokio::sync::mpsc::Receiver<T>,
        synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
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
                let start = Instant::now();

                if let Err(e) = writer.serialize(k).map_err(|e| e.to_string()) {
                    error!(
                        "Error while writing data into {:?}. Err: {:?}",
                        file_and_path.1, e
                    );
                }

                synchronizer_metrics.set_file_write_time(
                    file_and_path.1.as_ref(),
                    start.elapsed().as_millis() as f64,
                );
                synchronizer_metrics.inc_num_of_records_written(&file_and_path.1, 1);
            }
        }

        let start = Instant::now();
        if let Err(e) = writer.flush().map_err(|e| e.to_string()) {
            error!(
                "Error happened during flushing data to {:?}. Err: {:?}",
                file_and_path.1, e
            );
        }
        synchronizer_metrics
            .set_file_write_time(file_and_path.1.as_ref(), start.elapsed().as_millis() as f64);
        synchronizer_metrics.inc_num_of_records_written(&file_and_path.1, 1);

        Ok(())
    }

    fn encode<T: AsRef<[u8]>>(v: T) -> String {
        format!("\\x{}", hex::encode(v))
    }
}

#[async_trait]
impl Dumper for Storage {
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
            batch_size,
            rx,
            synchronizer_metrics,
        )
        .await?;
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

        self.dump_nft_csv(
            (metadata_file, metadata_path.unwrap()),
            (assets_file, assets_path.unwrap()),
            (creators_file, creators_path.unwrap()),
            (authority_file, authorities_path.unwrap()),
            batch_size,
            rx,
            synchronizer_metrics,
        )
        .await?;
        Ok(())
    }
}
