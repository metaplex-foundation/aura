use std::sync::Arc;

use log::info;
use rocks_db::asset::{self, AssetOwnerDeprecated};
use rocks_db::column::TypedColumn;
use rocks_db::tree_seq::TreeSeqIdx;
use rocks_db::{
    cl_items, signature_client, AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use rocksdb::{Options, DB};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use metrics_utils::red::RequestErrorDurationMetrics;
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

    info!("Starting a column remover...");

    // Specify the column families you plan to remove
    let columns_to_remove = vec![
        AssetStaticDetails::NAME,
        AssetDynamicDetails::NAME,
        AssetAuthority::NAME,
        AssetOwnerDeprecated::NAME,
        asset::AssetLeaf::NAME,
        asset::AssetCollection::NAME,
        cl_items::ClItem::NAME,
        cl_items::ClLeaf::NAME,
        asset::AssetsUpdateIdx::NAME,
        asset::SlotAssetIdx::NAME,
        AssetOwner::NAME,
        TreeSeqIdx::NAME,
        signature_client::SignatureIdx::NAME,
    ];

    // Print the column families to be removed
    println!(
        "Columns to be removed from {}: {:?}",
        db_path, columns_to_remove
    );

    // Ask for user confirmation
    println!("Do you want to proceed with removing these column families? (y/n)");

    let mut rl = DefaultEditor::new().expect("Failed to create readline editor.");

    match rl.readline(">> ") {
        Ok(line) => {
            if line.trim().eq_ignore_ascii_case("y") {
                remove_column_families(db_path.to_owned(), &columns_to_remove);
            } else {
                println!("Operation cancelled.");
            }
        }
        Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
            println!("Operation cancelled.");
        }
        Err(err) => {
            println!("Error: {:?}", err);
        }
    }

    Ok(())
}

fn remove_column_families(db_path: String, columns_to_remove: &[&str]) {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);

    // Get the existing column families
    let cf_names = DB::list_cf(&options, &db_path).expect("Failed to list column families.");

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let db = rocks_db::Storage::open(
        &db_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
    )
    .expect("Failed to open DB.");
    let db = db.db;
    columns_to_remove.iter().for_each(|cf_name| {
        if !cf_names.contains(&cf_name.to_string()) {
            println!("Column family {} does not exist. Skipping it", cf_name);
        } else {
            db.drop_cf(cf_name)
                .unwrap_or_else(|_| panic!("Failed to drop column family {}.", cf_name));
        }
    });

    println!("Column families removed successfully.");
}
