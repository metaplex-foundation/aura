use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Extension, Query},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use clap::Parser;
use metrics_utils::ApiMetricsConfig;
use prometheus_client::registry::Registry;
use rocks_db::columns::asset;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::Deserialize;
use tempfile::TempDir;

#[derive(Parser)]
struct Config {
    /// Primary DB path
    #[clap(short('d'), long)]
    primary_db_path: String,

    /// Secondary DB path (optional)
    #[clap(short, long)]
    secondary_db_path: Option<String>,

    /// Port (defaults to 8086)
    #[clap(short, long, default_value = "8086")]
    port: u16,
}

struct AppState {
    db: Arc<DB>,
}

#[derive(Deserialize)]
struct IterateKeysParams {
    cf_name: String,
    limit: Option<usize>,
    start_key: Option<String>,
}

#[derive(Deserialize)]
struct IterateKeysPatternParams {
    cf_name: String,
    pattern: String,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct GetValueParams {
    cf_name: String,
    key: String,
}

#[tokio::main]
async fn main() {
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

    let mut registry = Registry::default();
    let metrics = Arc::new(ApiMetricsConfig::new());
    metrics.register(&mut registry);
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    red_metrics.register(&mut registry);

    let options = Options::default();

    let cf_names =
        DB::list_cf(&options, &config.primary_db_path).expect("Failed to list column families.");

    let cfs: Vec<ColumnFamilyDescriptor> = cf_names
        .into_iter()
        .map(|name| {
            let mut cf_options = Options::default();
            cf_options.set_merge_operator_associative(
                &format!("merge_fn_merge_{}", &name),
                asset::AssetStaticDetails::merge_keep_existing,
            );
            ColumnFamilyDescriptor::new(&name, cf_options)
        })
        .collect();

    let db = DB::open_cf_descriptors_as_secondary(
        &options,
        &config.primary_db_path,
        &secondary_db_path,
        cfs,
    )
    .expect("Failed to open DB.");

    // Open the primary RocksDB database
    let db = Arc::new(db);

    let app_state = AppState { db };

    // Build our application with the routes
    let app = Router::new()
        .route("/iterate_keys", get(iterate_keys))
        .route("/iterate_keys_with_pattern", get(iterate_keys_with_pattern))
        .route("/get_value", get(get_value))
        .layer(Extension(Arc::new(app_state)));

    // Run our app with hyper
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    println!("Listening on {}", addr);
    axum::Server::bind(&addr).serve(app.into_make_service()).await.unwrap();
}

async fn iterate_keys(
    Extension(state): Extension<Arc<AppState>>,
    Query(params): Query<IterateKeysParams>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let db = &state.db;

    // Extract parameters
    let cf_name = &params.cf_name;
    let limit = params.limit.unwrap_or(10); // Default limit if not provided

    // Decode start_key if provided
    let start_key = if let Some(ref s) = params.start_key {
        match bs58::decode(s).into_vec() {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                return Err((StatusCode::BAD_REQUEST, "Invalid Base58 start_key".to_string()))
            },
        }
    } else {
        None
    };

    // Call the iterate_keys function
    match iterate_keys_function(db, cf_name, start_key.as_deref(), limit) {
        Ok(keys) => Ok(Json(keys)),
        Err(err_msg) => Err((StatusCode::INTERNAL_SERVER_ERROR, err_msg)),
    }
}

async fn iterate_keys_with_pattern(
    Extension(state): Extension<Arc<AppState>>,
    Query(params): Query<IterateKeysPatternParams>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let db = &state.db;

    // Extract parameters
    let cf_name = &params.cf_name;
    let limit = params.limit.unwrap_or(10); // Default limit if not provided

    // Decode the pattern from Base58
    let pattern_bytes = match bs58::decode(&params.pattern).into_vec() {
        Ok(bytes) => bytes,
        Err(_) => return Err((StatusCode::BAD_REQUEST, "Invalid Base58 pattern".to_string())),
    };

    // Call the iterate_keys_with_pattern function
    match iterate_keys_with_pattern_function(db, cf_name, &pattern_bytes, limit) {
        Ok(keys) => Ok(Json(keys)),
        Err(err_msg) => Err((StatusCode::INTERNAL_SERVER_ERROR, err_msg)),
    }
}

async fn get_value(
    Extension(state): Extension<Arc<AppState>>,
    Query(params): Query<GetValueParams>,
) -> Result<Json<String>, (StatusCode, String)> {
    let db = &state.db;

    // Extract parameters
    let cf_name = &params.cf_name;

    // Decode the key from Base58
    let key_bytes = match bs58::decode(&params.key).into_vec() {
        Ok(bytes) => bytes,
        Err(_) => return Err((StatusCode::BAD_REQUEST, "Invalid Base58 key".to_string())),
    };

    // Call the get_value function
    match get_value_function(db, cf_name, &key_bytes) {
        Ok(Some(value)) => Ok(Json(value)),
        Ok(None) => Err((StatusCode::NOT_FOUND, "Key not found".to_string())),
        Err(err_msg) => Err((StatusCode::INTERNAL_SERVER_ERROR, err_msg)),
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
    db: &DB,
    cf_name: &str,
    start_key: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<String>, String> {
    // Get the column family handle
    let cf_handle = db.cf_handle(cf_name).ok_or_else(|| "Column family not found".to_string())?;

    // Create an iterator with the specified starting point
    let iter_mode = match start_key {
        Some(key) => rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward),
        None => rocksdb::IteratorMode::Start,
    };

    let iterator = db.iterator_cf(&cf_handle, iter_mode);

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
fn get_value_function(db: &DB, cf_name: &str, key: &[u8]) -> Result<Option<String>, String> {
    // Get the column family handle
    let cf_handle = db.cf_handle(cf_name).ok_or_else(|| "Column family not found".to_string())?;

    // Retrieve the value for the key
    match db.get_cf(&cf_handle, key) {
        Ok(Some(value)) => Ok(Some(bs58::encode(value).into_string())),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("DB error: {}", e)),
    }
}

/// Iterates over keys in a specified RocksDB column family,
/// filtering keys that include a given byte pattern,
/// and returns up to `limit` Base58-encoded keys.
///
/// # Parameters
///
/// - `db`: Reference to the RocksDB database.
/// - `cf_name`: The name of the column family to iterate over.
/// - `pattern`: Byte pattern to match within the keys.
/// - `limit`: Maximum number of keys to return.
///
/// # Returns
///
/// A `Result` containing a vector of Base58-encoded keys or an error message.
fn iterate_keys_with_pattern_function(
    db: &DB,
    cf_name: &str,
    pattern: &[u8],
    limit: usize,
) -> Result<Vec<String>, String> {
    // Get the column family handle
    let cf_handle = &db.cf_handle(cf_name).ok_or_else(|| "Column family not found".to_string())?;

    // Create an iterator starting from the beginning
    let iter_mode = rocksdb::IteratorMode::Start;
    let iterator = db.iterator_cf(cf_handle, iter_mode);

    // Collect keys up to the specified limit that match the pattern
    let keys: Vec<String> = iterator
        .filter_map(Result::ok)
        .filter_map(|(key, _)| {
            if key.windows(pattern.len()).any(|window| window == pattern) {
                Some(bs58::encode(key).into_string())
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    Ok(keys)
}
