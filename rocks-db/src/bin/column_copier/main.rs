use entities::models::AssetSignature;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::asset::MetadataMintMap;
use rocks_db::column::TypedColumn;
use rocks_db::columns::{TokenAccount, TokenAccountMintOwnerIdx, TokenAccountOwnerIdx};
use rocks_db::editions::TokenMetadataEdition;
use rocks_db::tree_seq::{TreeSeqIdx, TreesGaps};
use rocks_db::{
    asset, cl_items, signature_client, AssetAuthority, AssetDynamicDetails, AssetOwner,
    AssetStaticDetails, Storage,
};
use rocksdb::IteratorMode;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const BATCH_SIZE: usize = 100_000;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), String> {
    // Retrieve the database paths from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <source_db_path> <destination_db_path>", args[0]);
        std::process::exit(1);
    }
    let source_db_path = &args[1];
    let destination_db_path = &args[2];

    println!("Starting data migration...");

    // Specify the column families you plan to copy
    let columns_to_copy = vec![
        AssetStaticDetails::NAME,
        AssetDynamicDetails::NAME,
        AssetAuthority::NAME,
        asset::AssetLeaf::NAME,
        asset::AssetCollection::NAME,
        cl_items::ClItem::NAME,
        cl_items::ClLeaf::NAME,
        asset::AssetsUpdateIdx::NAME,
        asset::SlotAssetIdx::NAME,
        AssetOwner::NAME,
        TreeSeqIdx::NAME,
        signature_client::SignatureIdx::NAME,
        MetadataMintMap::NAME,
        TreesGaps::NAME,
        TokenMetadataEdition::NAME,
        AssetSignature::NAME,
        TokenAccount::NAME,
        TokenAccountOwnerIdx::NAME,
        TokenAccountMintOwnerIdx::NAME,
    ];

    println!(
        "Columns to be copied from {} to {}: {:?}",
        source_db_path, destination_db_path, columns_to_copy
    );

    // Copy specified column families
    if let Err(e) = copy_column_families(source_db_path, destination_db_path, &columns_to_copy) {
        println!("Failed to copy data: {}.", e);
    } else {
        println!("Data copied successfully.");
    }

    Ok(())
}

fn copy_column_families(
    source_path: &str,
    destination_path: &str,
    columns_to_copy: &[&str],
) -> Result<(), String> {
    let start = Instant::now();
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    // Open source and destination databases
    let source_db = Storage::open(
        source_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
    )
    .map_err(|e| e.to_string())?;
    let destination_db = Storage::open(
        destination_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
    )
    .map_err(|e| e.to_string())?;

    for &cf_name in columns_to_copy {
        let start_column = Instant::now();
        let iter = source_db.db.iterator_cf(
            &source_db.db.cf_handle(cf_name).unwrap(),
            IteratorMode::Start,
        );
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (key, value) in iter.flatten() {
            batch.put_cf(&destination_db.db.cf_handle(cf_name).unwrap(), key, value);
            if batch.len() >= BATCH_SIZE {
                destination_db.db.write(batch).map_err(|e| e.to_string())?;
                batch = rocksdb::WriteBatchWithTransaction::<false>::default();
            }
        }
        destination_db.db.write(batch).map_err(|e| e.to_string())?;
        println!(
            "Migrated {} column in {} seconds",
            cf_name,
            start_column.elapsed().as_secs()
        );
    }
    println!(
        "Migrated all columns in {} seconds",
        start.elapsed().as_secs()
    );

    Ok(())
}
