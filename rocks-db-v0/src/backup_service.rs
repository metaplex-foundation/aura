use crate::errors::BackupServiceError;
use futures_util::StreamExt;
use log;
use log::{error, info};
use metrics_utils::IngesterMetricsConfig;
use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use rocksdb::{Env, DB};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BACKUP_PREFIX: &str = "backup-rocksdb";
const BACKUP_POSTFIX: &str = ".tar.lz4";
const ROCKS_NUM_BACKUPS_TO_KEEP: usize = 1;
const NUMBER_ARCHIVES_TO_STORE: usize = 2;
const DEFAULT_BACKUP_DIR_NAME: &str = "_rocksdb_backup";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupServiceConfig {
    pub rocks_backup_dir: String,
    pub rocks_backup_archives_dir: String,
    pub rocks_flush_before_backup: bool,
    pub rocks_interval_in_seconds: i64,
}

pub fn load_config() -> Result<BackupServiceConfig, BackupServiceError> {
    figment::Figment::new()
        .join(figment::providers::Env::prefixed("INGESTER_"))
        .extract()
        .map_err(|config_error| BackupServiceError::ConfigurationError(config_error.to_string()))
}

pub struct BackupService {
    pub backup_engine: BackupEngine,
    pub backup_config: BackupServiceConfig,
    pub db: Arc<DB>,
}

unsafe impl Send for BackupService {}

impl BackupService {
    pub fn new(
        db: Arc<DB>,
        config: &BackupServiceConfig,
    ) -> Result<BackupService, BackupServiceError> {
        let env = Env::new()?;
        let backup_options = BackupEngineOptions::new(config.rocks_backup_dir.clone())?;
        let backup_engine = BackupEngine::open(&backup_options, &env)?;

        Ok(Self {
            backup_engine,
            backup_config: config.clone(),
            db,
        })
    }

    fn create_backup(&mut self, backup_id: u32) -> Result<(), BackupServiceError> {
        self.backup_engine.create_new_backup_flush(
            self.db.as_ref(),
            self.backup_config.rocks_flush_before_backup,
        )?;

        self.verify_backup_single(backup_id)
    }

    pub fn perform_backup(
        &mut self,
        metrics: Arc<IngesterMetricsConfig>,
        keep_running: Arc<AtomicBool>,
    ) {
        let mut last_backup_id = 1;
        while keep_running.load(Ordering::SeqCst) {
            let start_time = chrono::Utc::now();
            last_backup_id = match self.backup_engine.get_backup_info().last() {
                None => last_backup_id,
                Some(backup_info) => {
                    if (backup_info.timestamp + self.backup_config.rocks_interval_in_seconds)
                        >= start_time.timestamp()
                    {
                        continue;
                    }
                    backup_info.backup_id + 1
                }
            };

            if let Err(err) = self.create_backup(last_backup_id) {
                error!("create_backup: {}", err);
            }
            if let Err(err) = self.delete_old_backups() {
                error!("delete_old_backups: {}", err);
            }
            if let Err(err) = self.build_backup_archive(start_time.timestamp()) {
                error!("build_backup_archive: {}", err);
            }
            if let Err(err) = self.delete_old_archives() {
                error!("delete_old_archives: {}", err);
            }

            let duration = chrono::Utc::now().signed_duration_since(start_time);
            metrics.set_rocksdb_backup_latency(duration.num_seconds() as f64);

            info!("perform_backup {}", duration.num_seconds());

            let mut seconds_sleep = 0;
            while seconds_sleep < self.backup_config.rocks_interval_in_seconds
                && keep_running.load(Ordering::SeqCst)
            {
                std::thread::sleep(Duration::from_secs(1));
                seconds_sleep += 1;
            }
            if !keep_running.load(Ordering::SeqCst) {
                return;
            }
        }
    }

    pub fn build_backup_archive(&self, backup_time: i64) -> Result<(), BackupServiceError> {
        let file_path = format!(
            "{}/{}-{}{}",
            self.backup_config.rocks_backup_archives_dir,
            BACKUP_PREFIX,
            backup_time,
            BACKUP_POSTFIX
        );
        std::fs::create_dir_all(self.backup_config.rocks_backup_archives_dir.clone())?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;
        let mut enc = lz4::EncoderBuilder::new().level(1).build(file)?;
        let mut tar = tar::Builder::new(&mut enc);

        let backup_dir_name = get_backup_dir_name(self.backup_config.rocks_backup_dir.as_str());
        tar.append_dir_all(backup_dir_name, self.backup_config.rocks_backup_dir.clone())?;
        tar.into_inner()?;
        let (_output, result) = enc.finish();
        result?;

        Ok(())
    }

    pub fn verify_backup_all(&self) -> Result<(), BackupServiceError> {
        let backup_infos = self.backup_engine.get_backup_info();
        if backup_infos.is_empty() {
            return Err(BackupServiceError::BackupEngineInfoIsEmpty {});
        }
        for backup_info in backup_infos.iter() {
            self.verify_backup_single(backup_info.backup_id)?;
            if backup_info.size == 0 {
                return Err(BackupServiceError::BackupEngineInfoSizeIsZero(
                    backup_info.backup_id,
                ));
            }
        }

        Ok(())
    }

    pub fn verify_backup_single(&self, backup_id: u32) -> Result<(), BackupServiceError> {
        match self.backup_engine.verify_backup(backup_id) {
            Ok(_) => Ok(()),
            Err(err) => Err(BackupServiceError::BackupEngineInfo(
                backup_id,
                err.to_string(),
            )),
        }
    }

    pub fn delete_old_archives(&self) -> Result<(), BackupServiceError> {
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

    pub fn delete_old_backups(&mut self) -> Result<(), BackupServiceError> {
        if self.backup_engine.get_backup_info().capacity() > ROCKS_NUM_BACKUPS_TO_KEEP {
            self.backup_engine
                .purge_old_backups(ROCKS_NUM_BACKUPS_TO_KEEP)?;
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
    backup_path: &str,
) -> Result<(), BackupServiceError> {
    let resp = reqwest::get(url).await?;
    if resp.status().is_success() {
        let mut file = File::create(backup_path)?;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?)?;
        }
        return Ok(());
    }

    Err(BackupServiceError::ReqwestError(resp.status().to_string()))
}

pub fn unpack_backup_archive(file_path: &str, dst: &str) -> Result<(), BackupServiceError> {
    let file = File::open(file_path)?;
    let decoder = lz4::Decoder::new(BufReader::new(file))?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(dst)?;

    Ok(())
}

pub fn restore_external_backup(
    backup_dir: &str,
    new_db_dir: &str,
) -> Result<(), BackupServiceError> {
    let env = Env::new()?;
    let backup_options = BackupEngineOptions::new(backup_dir)?;
    let mut backup_engine = BackupEngine::open(&backup_options, &env)?;
    let mut restore_option = RestoreOptions::default();
    restore_option.set_keep_log_files(true);
    backup_engine.restore_from_latest_backup(new_db_dir, new_db_dir, &restore_option)?;

    Ok(())
}
