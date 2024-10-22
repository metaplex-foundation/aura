use actix_web::{get, web, App, HttpServer, Responder};
use bs58;
use clap::Parser;
use metrics_utils::ApiMetricsConfig;
use prometheus_client::registry::Registry;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;
use serde::Deserialize;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Parser)]
struct Config {
    /// Primary DB path
    #[clap(short, long)]
    primary_db_path: String,

    /// Secondary DB path (optional)
    #[clap(short, long)]
    secondary_db_path: Option<String>,

    /// Port (defaults to 8086)
    #[clap(short, long, default_value = "8086")]
    port: u16,
}

struct AppState {
    db: Arc<Storage>,
}

#[derive(Deserialize)]
struct IterateKeysParams {
    cf_name: String,
    limit: Option<usize>,
    start_key: Option<String>,
}

#[derive(Deserialize)]
struct GetValueParams {
    cf_name: String,
    key: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse command-line arguments
    let config = Config::parse();

    // Handle secondary DB path
    let temp_dir;
    let secondary_db_path = if let Some(ref path) = config.secondary_db_path {
        path.clone()
    } else {
        temp_dir = TempDir::new().expect("Failed to create temp directory");
        temp_dir.path().to_str().unwrap().to_string()
    };

    let tasks = JoinSet::new();
    let mut registry = Registry::default();
    let metrics = Arc::new(ApiMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let storage = Storage::open_secondary(
        &config.primary_db_path,
        &secondary_db_path,
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .expect("Failed to open RocksDB storage");
    // Open the primary RocksDB database
    let db = Arc::new(storage);

    let state = web::Data::new(AppState { db });

    // Start the HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(iterate_keys)
            .service(get_value)
    })
    .bind(("0.0.0.0", config.port))?
    .run()
    .await
}

#[get("/iterate_keys")]
async fn iterate_keys(
    data: web::Data<AppState>,
    query: web::Query<IterateKeysParams>,
) -> impl Responder {
    let db = &data.db;

    // Extract parameters
    let cf_name = &query.cf_name;
    let limit = query.limit.unwrap_or(10); // Default limit if not provided

    // Decode start_key if provided
    let start_key = if let Some(ref s) = query.start_key {
        match bs58::decode(s).into_vec() {
            Ok(bytes) => Some(bytes),
            Err(_) => return web::Json(vec!["Invalid Base58 start_key".to_string()]),
        }
    } else {
        None
    };

    // Call the iterate_keys function
    match iterate_keys_function(db, cf_name, start_key.as_deref(), limit) {
        Ok(keys) => web::Json(keys),
        Err(err_msg) => web::Json(vec![err_msg]),
    }
}

#[get("/get_value")]
async fn get_value(data: web::Data<AppState>, query: web::Query<GetValueParams>) -> impl Responder {
    let db = &data.db;

    // Extract parameters
    let cf_name = &query.cf_name;

    // Decode the key from Base58
    let key_bytes = match bs58::decode(&query.key).into_vec() {
        Ok(bytes) => bytes,
        Err(_) => return web::Json("Invalid Base58 key".to_string()),
    };

    // Call the get_value function
    match get_value_function(db, cf_name, &key_bytes) {
        Ok(Some(value)) => web::Json(value),
        Ok(None) => web::Json("Key not found".to_string()),
        Err(err_msg) => web::Json(err_msg),
    }
}

/// Iterates over keys in a specified RocksDB column family, starting from an optional key,
/// and returns up to `limit` Base58-encoded keys.
///
/// # Parameters
///
/// - `db`: Reference to the RocksDB database.
/// - `cf_name`: The name of the column family to iterate over.
/// - `start_key`: Optional starting key to begin iteration.
/// - `limit`: Maximum number of keys to return.
///
/// # Returns
///
/// A `Result` containing a vector of Base58-encoded keys or an error message.
fn iterate_keys_function(
    db: &Storage,
    cf_name: &str,
    start_key: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<String>, String> {
    // Get the column family handle
    let cf_handle = db
        .db
        .cf_handle(cf_name)
        .ok_or_else(|| "Column family not found".to_string())?;

    // Create an iterator with the specified starting point
    let iter_mode = match start_key {
        Some(key) => rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward),
        None => rocksdb::IteratorMode::Start,
    };

    let iterator = db.db.iterator_cf(&cf_handle, iter_mode);

    // Collect keys up to the specified limit
    let keys: Vec<String> = iterator
        .take(limit)
        .filter_map(Result::ok)
        .map(|(key, _)| bs58::encode(key).into_string())
        .collect();

    Ok(keys)
}

/// Retrieves the value for a given key from a specified RocksDB column family,
/// and returns it as a Base58-encoded string.
///
/// # Parameters
///
/// - `db`: Reference to the RocksDB database.
/// - `cf_name`: The name of the column family.
/// - `key`: The key to retrieve the value for.
///
/// # Returns
///
/// A `Result` containing an `Option<String>` with the Base58-encoded value if the key exists,
/// or an error message.
fn get_value_function(db: &Storage, cf_name: &str, key: &[u8]) -> Result<Option<String>, String> {
    // Get the column family handle
    let cf_handle = db
        .db
        .cf_handle(cf_name)
        .ok_or_else(|| "Column family not found".to_string())?;

    // Retrieve the value for the key
    match db.db.get_cf(&cf_handle, key) {
        Ok(Some(value)) => Ok(Some(bs58::encode(value).into_string())),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("DB error: {}", e)),
    }
}
