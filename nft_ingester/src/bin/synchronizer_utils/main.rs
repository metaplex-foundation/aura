use std::{collections::HashSet, path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use itertools::Itertools;
use nft_ingester::error::IngesterError;
use rocks_db::{
    column::TypedColumn,
    columns::asset::AssetCompleteDetails,
    generated::asset_generated::asset as fb,
    key_encoders::decode_u64x2_pubkey,
    migrator::MigrationState,
    storage_traits::{AssetIndexReader, AssetUpdateIndexStorage},
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the RocksDB instance with slots
    #[arg(short, long)]
    db_path: PathBuf,

    /// List of Pubkeys to fetch from the RPC and ingest into the RocksDB
    #[arg(short, long, value_delimiter = ',', num_args = 0..)]
    pubkeys_to_index: Option<Vec<String>>,

    #[arg(short, long)]
    index_after: Option<String>,

    /// Base58-encoded owner public key to filter assets
    #[arg(short, long)]
    owner_pubkey: Option<String>,

    #[arg(short, long, value_delimiter = ',', num_args = 0..)]
    get_asset_maps_ids: Option<Vec<String>>,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    info!("Starting sync util...");

    let args = Args::parse();

    let tx_storage_dir = tempfile::TempDir::new().unwrap();

    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

    let storage = Storage::open_secondary(
        &args.db_path,
        &tx_storage_dir.path().to_path_buf(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();
    if let Some(index_after) = args.index_after {
        // Decode the Base58 string back into Vec<u8>
        let decoded_data =
            bs58::decode(&index_after).into_vec().expect("index after should be base58 encoded");
        let starting_key = decode_u64x2_pubkey(decoded_data).expect("Failed to decode index after");
        let (updated_keys, _last_included_key) =
            storage.fetch_nft_asset_updated_keys(Some(starting_key), None, 500, None).unwrap();
        let index = storage
            .get_nft_asset_indexes(updated_keys.into_iter().collect_vec().as_slice())
            .await
            .expect("Failed to get indexes");
        println!("{:?}", index);
    }
    if let Some(owner_pubkey) = args.owner_pubkey {
        let owner_pubkey = Pubkey::from_str(&owner_pubkey).expect("Failed to parse owner pubkey");
        let owner_bytes = owner_pubkey.to_bytes();
        let mut matching_pubkeys = HashSet::new();
        let mut total_assets_processed = 0;

        let mut it =
            storage.db.raw_iterator_cf(&storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap());
        it.seek_to_first();
        while it.valid() {
            total_assets_processed += 1;
            if let Some(value_bytes) = it.value() {
                let data = fb::root_as_asset_complete_details(value_bytes)
                    .expect("Failed to deserialize asset");

                // Check if owner matches
                if data
                    .owner()
                    .and_then(|o| o.owner())
                    .and_then(|ow| ow.value())
                    .filter(|owner| owner.bytes() == owner_bytes.as_slice())
                    .is_some()
                {
                    let asset: AssetCompleteDetails = AssetCompleteDetails::from(data);
                    println!("Matching asset: {:?}", asset);
                    matching_pubkeys.insert(asset.pubkey);
                }
            }
            it.next();
        }
        info!("Iteration completed.");
        info!(
            "Total assets processed: {}. Matches found: {}.",
            total_assets_processed,
            matching_pubkeys.len()
        );

        println!("Matching public keys:");
        for pubkey in &matching_pubkeys {
            println!("{}", pubkey);
        }
    }
    if let Some(pubkeys_to_index) = args.pubkeys_to_index {
        let keys = pubkeys_to_index
            .iter()
            .map(|pk| Pubkey::from_str(pk).expect("invalid pubkey"))
            .collect_vec();
        let index =
            storage.get_nft_asset_indexes(keys.as_slice()).await.expect("Failed to get indexes");
        println!("{:?}", index);
    }
    if let Some(get_asset_maps_ids) = args.get_asset_maps_ids {
        let keys = get_asset_maps_ids
            .iter()
            .map(|pk| Pubkey::from_str(pk).expect("invalid pubkey"))
            .collect_vec();
        let maps = storage
            .get_asset_selected_maps_async(keys, &None, &Default::default())
            .await
            .expect("Failed to get asset selected maps");
        println!("{:?}", maps);
    }
    Ok(())
}
