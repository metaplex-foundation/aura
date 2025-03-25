#[cfg(feature = "profiling")]
use std::{fs::File, io::Write};
use std::{path::PathBuf, sync::Arc};

use metrics_utils::{red::RequestErrorDurationMetrics, MetricState};
use postgre_client::PgClient;
#[cfg(feature = "profiling")]
use pprof::{protos::Message, ProfilerGuard};
use rocks_db::{migrator::MigrationState, Storage};
use tempfile::TempDir;
#[cfg(feature = "profiling")]
use tokio::process::Command;
#[cfg(feature = "profiling")]
use tracing::error;
use tracing::info;

use crate::error::IngesterError;

#[cfg(feature = "profiling")]
const MALLOC_CONF_ENV: &str = "MALLOC_CONF";

pub async fn init_index_storage_with_migration(
    url: &str,
    max_pg_connections: u32,
    red_metrics: Arc<RequestErrorDurationMetrics>,
    min_pg_connections: u32,
    pg_migrations_path: &str,
    base_dump_path: Option<PathBuf>,
    pg_max_query_statement_timeout_secs: Option<u32>,
) -> Result<PgClient, IngesterError> {
    let pg_client = PgClient::new(
        url,
        min_pg_connections,
        max_pg_connections,
        base_dump_path,
        red_metrics,
        pg_max_query_statement_timeout_secs,
    )
    .await
    .map_err(|e| IngesterError::SqlxError(e.to_string()))?;

    pg_client.run_migration(pg_migrations_path).await.map_err(IngesterError::SqlxError)?;

    Ok(pg_client)
}

pub async fn init_primary_storage(
    db_path: &str,
    enable_migration_rocksdb: bool,
    migration_storage_path: &Option<String>,
    metrics_state: &MetricState,
) -> Result<Storage, IngesterError> {
    Storage::open(
        db_path,
        metrics_state.red_metrics.clone(),
        MigrationState::CreateColumnFamilies,
    )?;

    if enable_migration_rocksdb {
        info!("Start migration rocksdb process...");

        let migration_version_manager_dir = TempDir::new()?;
        let migration_version_manager = Storage::open_secondary(
            db_path,
            migration_version_manager_dir.path().to_str().unwrap(),
            metrics_state.red_metrics.clone(),
            MigrationState::Last,
        )?;

        Storage::apply_all_migrations(
            db_path,
            migration_storage_path.as_deref().ok_or(IngesterError::ConfigurationError {
                msg: "Migration storage path is not set".to_string(),
            })?,
            Arc::new(migration_version_manager),
        )
        .await?;

        info!("Migration rocksdb process was successful.");
    }

    Ok(Storage::open(db_path, metrics_state.red_metrics.clone(), MigrationState::Last)?)
}

#[cfg(feature = "profiling")]
pub async fn graceful_stop(
    cancellation_token: tokio_util::sync::CancellationToken,
    guard: Option<ProfilerGuard<'_>>,
    profile_path: Option<String>,
    heap_path: &str,
) {
    usecase::graceful_stop::graceful_shutdown(cancellation_token).await;

    if let Some(guard) = guard {
        if let Ok(report) = guard.report().build() {
            // This code will be called only for profiling, so unwraps is used
            let mut file = File::create(format!("{}/profile.pb", profile_path.unwrap())).unwrap();
            let profile = report.pprof().unwrap();

            let content = profile.write_to_bytes().unwrap();
            file.write_all(&content).unwrap();
        }
    }
    if std::env::var(MALLOC_CONF_ENV).is_ok() {
        generate_profiling_gif(heap_path).await;
    }
}

#[cfg(feature = "profiling")]
async fn generate_profiling_gif(heap_path: &str) {
    let program = match std::env::current_exe()
        .map_err(|e| IngesterError::Usecase(e.to_string()))
        .and_then(|exe| {
            exe.as_path()
                .to_str()
                .ok_or(IngesterError::Usecase("Cannot cast to string".to_string()))
                .map(|s| s.to_string())
        }) {
        Ok(program) => program,
        Err(e) => {
            error!("Cannot get program path: {}", e);
            return;
        },
    };

    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "jeprof --show_bytes --gif {0} {1}/.*.*.*.heap > {1}/profile.gif",
            program, heap_path
        ))
        .output()
        .await
        .expect("failed to execute process");

    if !output.status.success() {
        error!("jeprof: {}", String::from_utf8_lossy(&output.stderr));
    }
}
