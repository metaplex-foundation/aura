use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rocks_db::{
    backup_service, errors::RocksDbBackupServiceError, storage_traits::AssetSlotStorage, Storage,
};
use tokio::{task::JoinError, time::sleep as tokio_sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::INGESTER_BACKUP_NAME;

pub async fn receive_last_saved_slot(
    cancellation_token: CancellationToken,
    cloned_rocks_storage: Arc<Storage>,
    first_processed_slot_clone: Arc<AtomicU64>,
    last_saved_slot: u64,
) -> Result<(), JoinError> {
    while !cancellation_token.is_cancelled() {
        match cloned_rocks_storage.last_saved_slot() {
            Ok(Some(slot)) if slot != last_saved_slot => {
                first_processed_slot_clone.store(slot, Ordering::Relaxed);
                break;
            },
            Err(e) => {
                error!("Error while getting last saved slot: {}", e);
                cancellation_token.cancel();
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
    rocks_backup_archives_dir: &PathBuf,
    rocks_db_path_container: &PathBuf,
) -> Result<(), RocksDbBackupServiceError> {
    create_dir_all(rocks_backup_archives_dir)?;

    let backup_path = rocks_backup_archives_dir.join(INGESTER_BACKUP_NAME);

    backup_service::download_backup_archive(rocks_backup_url, &backup_path).await?;
    backup_service::unpack_backup_archive(&backup_path, rocks_backup_archives_dir)?;

    let unpacked_archive = rocks_backup_archives_dir.join(
        backup_service::get_backup_dir_name(rocks_backup_url)
            .parse::<PathBuf>()
            .expect("invalid backup dir name"),
    );

    backup_service::restore_external_backup(&unpacked_archive, rocks_db_path_container)?;

    // remove unpacked files
    remove_dir_all(unpacked_archive)?;

    info!("restore_rocksdb fin");
    Ok(())
}
