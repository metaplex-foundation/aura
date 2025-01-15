use std::{
    fs::{create_dir_all, remove_dir_all},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rocks_db::{
    backup_service, errors::RocksDbBackupServiceError, storage_traits::AssetSlotStorage, Storage,
};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinError,
    time::sleep as tokio_sleep,
};
use tracing::{error, info};

use crate::config::INGESTER_BACKUP_NAME;

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
                first_processed_slot_clone.store(slot, Ordering::Relaxed);
                break;
            },
            Err(e) => {
                error!("Error while getting last saved slot: {}", e);
                cloned_tx.send(()).ok();
                break;
            },
            _ => {},
        }

        tokio_sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

pub async fn restore_rocksdb(
    rocks_backup_url: &str,
    rocks_backup_archives_dir: &str,
    rocks_db_path_container: &str,
) -> Result<(), RocksDbBackupServiceError> {
    create_dir_all(rocks_backup_archives_dir)?;

    let backup_path = format!("{}/{}", rocks_backup_archives_dir, INGESTER_BACKUP_NAME);

    backup_service::download_backup_archive(rocks_backup_url, &backup_path).await?;
    backup_service::unpack_backup_archive(&backup_path, rocks_backup_archives_dir)?;

    let unpacked_archive = format!(
        "{}/{}",
        &rocks_backup_archives_dir,
        backup_service::get_backup_dir_name(rocks_backup_url)
    );

    backup_service::restore_external_backup(&unpacked_archive, rocks_db_path_container)?;

    // remove unpacked files
    remove_dir_all(unpacked_archive)?;

    info!("restore_rocksdb fin");
    Ok(())
}
