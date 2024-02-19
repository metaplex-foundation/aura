use std::str::FromStr;
use std::sync::Arc;

use log::info;
use rocks_db::asset::{self, AssetOwnerDeprecated};
use rocks_db::column::TypedColumn;
use rocks_db::tree_seq::TreeSeqIdx;
use rocks_db::{
    cl_items, signature_client, AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use rocksdb::{Options, DB};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use std::env;
#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), String> {
    // Retrieve the database path from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} <path_to_db>", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];

    // Print the column families to be removed
    println!(
        "Will open {}",
        db_path
    );

    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);

    let db = rocks_db::Storage::open_secondary(&db_path, "./", Arc::new(Mutex::new(JoinSet::new())))
        .expect("Failed to open DB.");

    let key_to_select = Pubkey::from_str("").unwrap();

    let data = db.asset_owner_data.get(key_to_select).unwrap().unwrap();

    println!("Here is owner info for {}\n{:?}", key_to_select, data);

    Ok(())
}