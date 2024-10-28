use crate::config::{IngesterConfig, INGESTER_BACKUP_NAME};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::backup_service::BackupService;
use rocks_db::errors::BackupServiceError;
use rocks_db::storage_traits::AssetSlotStorage;
use rocks_db::{backup_service, Storage};
use std::fs::{create_dir_all, remove_dir_all};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinError;
use tokio::time::sleep as tokio_sleep;
use tracing::{error, info};

pub async fn perform_backup(
    mut backup_service: BackupService,
    cloned_rx: Receiver<()>,
    cloned_metrics: Arc<IngesterMetricsConfig>,
) -> Result<(), JoinError> {
    backup_service
        .perform_backup(cloned_metrics, cloned_rx)
        .await;
    Ok(())
}

pub async fn receive_last_saved_slot(
    cloned_rx: Receiver<()>,
    cloned_tx: Sender<()>,
    cloned_rocks_storage: Arc<Storage>,
    first_processed_slot_clone: Arc<AtomicU64>,
    last_saved_slot: u64,
) -> Result<(), JoinError> {
    while cloned_rx.is_empty() {
        match cloned_rocks_storage.last_saved_slot() {
            Ok(Some(slot)) if slot != last_saved_slot => {
                first_processed_slot_clone.store(slot, Ordering::SeqCst);
                break;
            }
            Err(e) => {
                error!("Error while getting last saved slot: {}", e);
                cloned_tx.send(()).ok();
                break;
            }
            _ => {}
        }

        tokio_sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

pub async fn restore_rocksdb(config: &IngesterConfig) -> Result<(), BackupServiceError> {
    let rocks_backup_archives_dir = config.rocks_backup_archives_dir.clone();

    create_dir_all(&rocks_backup_archives_dir)?;

    let backup_path = format!("{}/{}", rocks_backup_archives_dir, INGESTER_BACKUP_NAME);

    backup_service::download_backup_archive(&config.rocks_backup_url, &backup_path).await?;
    backup_service::unpack_backup_archive(&backup_path, &rocks_backup_archives_dir)?;

    let unpacked_archive = format!(
        "{}/{}",
        &rocks_backup_archives_dir,
        backup_service::get_backup_dir_name(config.rocks_backup_dir.as_str())
    );

    backup_service::restore_external_backup(
        &unpacked_archive,
        config
            .rocks_db_path_container
            .as_deref()
            .unwrap_or("./my_rocksdb"),
    )?;

    // remove unpacked files
    remove_dir_all(unpacked_archive)?;

    info!("restore_rocksdb fin");
    Ok(())
}

pub enum RocksDbManager {
    Primary(Arc<Storage>),
    Secondary(RocksDbSecondaryDuplicateMode),
}

impl RocksDbManager {
    /// Instantiate a new RocksDbManager with primary storage
    pub fn new_primary(primary: Arc<Storage>) -> Self {
        RocksDbManager::Primary(primary)
    }

    /// Instantiate a new RocksDbManager with secondary storage
    pub fn new_secondary(primary: Storage, secondary: Storage) -> Self {
        RocksDbManager::Secondary(RocksDbSecondaryDuplicateMode {
            rocks_db_instances: [primary.into(), secondary.into()],
            serving_instance_toggle: AtomicBool::new(false),
        })
    }

    /// Returns the current storage instance
    ///
    /// It's always the same storage for primary mode
    /// For secondary mode, it will return a storage instance
    /// based on it's sync status with the primary storage
    pub fn acquire(&self) -> Arc<Storage> {
        match self {
            RocksDbManager::Primary(storage) => storage.clone(),
            RocksDbManager::Secondary(duplicate_mode) => duplicate_mode.rocks_db_instances
                [duplicate_mode
                    .serving_instance_toggle
                    .load(Ordering::Relaxed) as usize]
                .clone(),
        }
    }

    /// Syncronize secondary rocksdb with primary
    ///
    /// One of the DB will be blocked while the other one is processing request
    pub async fn catch_up(&self, shutdown_rx: Receiver<()>) {
        if !shutdown_rx.is_empty() {
            return;
        }

        const SLEEP_TIME_MS: Duration = Duration::from_millis(10);

        match self {
            RocksDbManager::Primary(_) => {}
            RocksDbManager::Secondary(duplicate_mode) => {
                let free_node_idx = (duplicate_mode
                    .serving_instance_toggle
                    .load(Ordering::Relaxed) as usize
                    + 1)
                    % 2;
                let free_node = &duplicate_mode.rocks_db_instances[free_node_idx];

                while Arc::<rocks_db::Storage>::strong_count(&free_node) > 1 {
                    if !shutdown_rx.is_empty() {
                        return;
                    }

                    tokio_sleep(SLEEP_TIME_MS).await;
                }

                if let Err(e) = free_node.db.try_catch_up_with_primary() {
                    error!("Sync rocksdb error: {}", e);
                }

                duplicate_mode
                    .serving_instance_toggle
                    .store(free_node_idx != 0, Ordering::Relaxed);
            }
        }
    }
}

pub struct RocksDbSecondaryDuplicateMode {
    rocks_db_instances: [Arc<Storage>; 2],
    serving_instance_toggle: AtomicBool,
}
