use crate::config::IngesterConfig;
use crate::error::IngesterError;
use metrics_utils::MetricState;
use postgre_client::PgClient;
use pprof::protos::Message;
use pprof::ProfilerGuard;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use std::fs::File;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::error;

const MALLOC_CONF_ENV: &str = "MALLOC_CONF";

pub async fn init_index_storage_with_migration(
    config: &IngesterConfig,
    metrics_state: &MetricState,
    max_pg_connection_default_value: u32,
    min_pg_connection_default_value: u32,
    pg_migrations_path: &str,
) -> Result<PgClient, IngesterError> {
    let max_pg_connections = config
        .database_config
        .get_max_postgres_connections()
        .unwrap_or(max_pg_connection_default_value);

    let pg_client = PgClient::new(
        &config
            .database_config
            .get_database_url()
            .expect("No 'database_url' specified."),
        min_pg_connection_default_value,
        max_pg_connections,
        metrics_state.red_metrics.clone(),
    )
    .await
    .map_err(|e| e.to_string())
    .map_err(IngesterError::SqlxError)?;

    pg_client
        .run_migration(pg_migrations_path)
        .await
        .map_err(IngesterError::SqlxError)?;

    Ok(pg_client)
}

pub async fn init_primary_storage(
    config: &IngesterConfig,
    metrics_state: &MetricState,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    default_rocksdb_path: &str,
) -> Result<Storage, IngesterError> {
    let db_path = config
        .rocks_db_path_container
        .as_deref()
        .unwrap_or(default_rocksdb_path);

    Storage::open(
        db_path,
        mutexed_tasks.clone(),
        metrics_state.red_metrics.clone(),
        MigrationState::CreateColumnFamilies,
    )?;

    let migration_version_manager_dir = TempDir::new()?;
    let migration_version_manager = Storage::open_secondary(
        db_path,
        migration_version_manager_dir.path().to_str().unwrap(),
        mutexed_tasks.clone(),
        metrics_state.red_metrics.clone(),
        MigrationState::Last,
    )?;

    Storage::apply_all_migrations(
        db_path,
        &config.migration_storage_path,
        Arc::new(migration_version_manager),
    )
    .await?;

    Ok(Storage::open(
        db_path,
        mutexed_tasks.clone(),
        metrics_state.red_metrics.clone(),
        MigrationState::Last,
    )?)
}

pub async fn graceful_stop(
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    shutdown_tx: Sender<()>,
    guard: Option<ProfilerGuard<'_>>,
    profile_path: Option<String>,
    heap_path: &str,
) {
    usecase::graceful_stop::listen_shutdown().await;
    let _ = shutdown_tx.send(());

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

    usecase::graceful_stop::graceful_stop(tasks.lock().await.deref_mut()).await
}

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
        }
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
