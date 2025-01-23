use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
    Env, DB,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::errors::RocksDbBackupServiceError;

const BACKUP_PREFIX: &str = "backup-rocksdb";
const BACKUP_POSTFIX: &str = ".tar.lz4";
const ROCKS_NUM_BACKUPS_TO_KEEP: usize = 1;
const NUMBER_ARCHIVES_TO_STORE: usize = 2;
const DEFAULT_BACKUP_DIR_NAME: &str = "_rocksdb_backup";
const BASE_BACKUP_ID: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RocksDbBackupServiceConfig {
    pub rocks_backup_dir: PathBuf,
    pub rocks_backup_archives_dir: PathBuf,
    pub rocks_flush_before_backup: bool,
}

pub struct RocksDbBackupService {
    pub backup_engine: BackupEngine,
    pub backup_config: RocksDbBackupServiceConfig,
    pub db: Arc<DB>,
}

unsafe impl Send for RocksDbBackupService {}

impl RocksDbBackupService {
    pub fn new(
        db: Arc<DB>,
        config: &RocksDbBackupServiceConfig,
    ) -> Result<RocksDbBackupService, RocksDbBackupServiceError> {
        let env = Env::new()?;
        let backup_options = BackupEngineOptions::new(config.rocks_backup_dir.clone())?;
        let backup_engine = BackupEngine::open(&backup_options, &env)?;

        Ok(Self { backup_engine, backup_config: config.clone(), db })
    }

    fn create_backup(&mut self, backup_id: u32) -> Result<(), RocksDbBackupServiceError> {
        self.backup_engine.create_new_backup_flush(
            self.db.as_ref(),
            self.backup_config.rocks_flush_before_backup,
        )?;

        self.verify_backup_single(backup_id)
    }

    pub async fn perform_backup(&mut self) -> Result<(), RocksDbBackupServiceError> {
        let start_time = chrono::Utc::now();
        let last_backup_id = match self.backup_engine.get_backup_info().last() {
            None => BASE_BACKUP_ID,
            Some(backup_info) => backup_info.backup_id + 1,
        };

        let progress_bar = Arc::new(ProgressBar::new(4)); // four steps:
                                                          // create backup, delete the old one, build archive, delete old archives
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{bar:40.cyan/blue}] {percent}% \
                    ({pos}/{len}) {msg}",
                )
                .expect("Failed to set progress bar style")
                .progress_chars("#>-"),
        );

        self.create_backup(last_backup_id).inspect_err(|err| {
            error!(error = %err, "create_backup: {:?}", err);
        })?;
        progress_bar.inc(1);
        self.delete_old_backups().inspect_err(|err| {
            error!(error = %err, "delete_old_backups: {:?}", err);
        })?;
        progress_bar.inc(1);
        self.build_backup_archive(start_time.timestamp()).inspect_err(|err| {
            error!(error = %err, "build_backup_archive: {:?}", err);
        })?;
        progress_bar.inc(1);
        self.delete_old_archives().inspect_err(|err| {
            error!(error = %err, "delete_old_archives: {:?}", err);
        })?;
        progress_bar.inc(1);
        progress_bar.finish_with_message("Backup completed!");

        let duration = chrono::Utc::now().signed_duration_since(start_time);

        info!(duration = %duration.num_milliseconds(), "Performed backup in {}ms", duration.num_milliseconds());

        Ok(())
    }

    pub fn build_backup_archive(&self, backup_time: i64) -> Result<(), RocksDbBackupServiceError> {
        let file_path = format!(
            "{}/{}-{}{}",
            self.backup_config
                .rocks_backup_archives_dir
                .to_str()
                .expect("Invalid backup archives dir path"),
            BACKUP_PREFIX,
            backup_time,
            BACKUP_POSTFIX
        );
        std::fs::create_dir_all(self.backup_config.rocks_backup_archives_dir.clone())?;
        let file =
            std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(file_path)?;
        let mut enc = lz4::EncoderBuilder::new().level(1).build(file)?;
        let mut tar = tar::Builder::new(&mut enc);

        let backup_dir_name = get_backup_dir_name(
            self.backup_config
                .rocks_backup_dir
                .as_path()
                .to_str()
                .expect("invalid rocks backup dir provided"),
        );
        tar.append_dir_all(backup_dir_name, self.backup_config.rocks_backup_dir.clone())?;
        tar.into_inner()?;
        enc.finish().1?;

        Ok(())
    }

    pub fn verify_backup_all(&self) -> Result<(), RocksDbBackupServiceError> {
        let backup_infos = self.backup_engine.get_backup_info();
        if backup_infos.is_empty() {
            return Err(RocksDbBackupServiceError::BackupEngineInfoIsEmpty {});
        }
        for backup_info in backup_infos.iter() {
            self.verify_backup_single(backup_info.backup_id)?;
            if backup_info.size == 0 {
                return Err(RocksDbBackupServiceError::BackupEngineInfoSizeIsZero(
                    backup_info.backup_id,
                ));
            }
        }

        Ok(())
    }

    pub fn verify_backup_single(&self, backup_id: u32) -> Result<(), RocksDbBackupServiceError> {
        match self.backup_engine.verify_backup(backup_id) {
            Ok(_) => Ok(()),
            Err(err) => {
                Err(RocksDbBackupServiceError::BackupEngineInfo(backup_id, err.to_string()))
            },
        }
    }

    pub fn delete_old_archives(&self) -> Result<(), RocksDbBackupServiceError> {
        let mut entries: Vec<_> =
            std::fs::read_dir(self.backup_config.rocks_backup_archives_dir.clone())?
                .filter_map(|r| r.ok())
                .collect();

        if entries.len() <= NUMBER_ARCHIVES_TO_STORE {
            return Ok(());
        }

        entries.sort_by_key(|dir| dir.path());

        for e in &entries[..entries.len() - NUMBER_ARCHIVES_TO_STORE] {
            std::fs::remove_file(e.path())?
        }

        Ok(())
    }

    pub fn delete_old_backups(&mut self) -> Result<(), RocksDbBackupServiceError> {
        if self.backup_engine.get_backup_info().capacity() > ROCKS_NUM_BACKUPS_TO_KEEP {
            self.backup_engine.purge_old_backups(ROCKS_NUM_BACKUPS_TO_KEEP)?;
        }

        Ok(())
    }
}

pub fn get_backup_dir_name(backup_path: &str) -> String {
    let path_str = Path::new(backup_path);
    path_str
        .file_name()
        .and_then(OsStr::to_str)
        .map(String::from)
        .ok_or_else(|| {
            error!("Invalid path or Unicode in folder name");
        })
        .unwrap_or(DEFAULT_BACKUP_DIR_NAME.to_string())
}

pub async fn download_backup_archive(
    url: &str,
    backup_path: &PathBuf,
) -> Result<(), RocksDbBackupServiceError> {
    let resp = reqwest::get(url).await?;
    if resp.status().is_success() {
        let mut file = File::create(backup_path)?;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?)?;
        }
        return Ok(());
    }

    Err(RocksDbBackupServiceError::ReqwestError(resp.status().to_string()))
}

pub fn unpack_backup_archive(
    file_path: &PathBuf,
    dst: &PathBuf,
) -> Result<(), RocksDbBackupServiceError> {
    let file = File::open(file_path)?;
    let decoder = lz4::Decoder::new(BufReader::new(file))?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(dst)?;

    Ok(())
}

pub fn restore_external_backup(
    backup_dir: &PathBuf,
    new_db_dir: &PathBuf,
) -> Result<(), RocksDbBackupServiceError> {
    let env = Env::new()?;
    let backup_options = BackupEngineOptions::new(backup_dir)?;
    let mut backup_engine = BackupEngine::open(&backup_options, &env)?;
    let mut restore_option = RestoreOptions::default();
    restore_option.set_keep_log_files(true);
    backup_engine.restore_from_latest_backup(new_db_dir, new_db_dir, &restore_option)?;

    Ok(())
}
