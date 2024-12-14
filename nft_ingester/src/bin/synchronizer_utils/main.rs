use clap::Parser;
use entities::models::TreeState;
use itertools::Itertools;
use nft_ingester::error::IngesterError;
use nft_ingester::index_syncronizer::shard_pubkeys;
use rocks_db::asset::AssetCompleteDetails;
use rocks_db::asset_generated::asset as fb;
use rocks_db::column::TypedColumn;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::migrator::MigrationState;
use rocks_db::storage_traits::AssetIndexReader;
use rocks_db::storage_traits::AssetUpdateIndexStorage;
use rocks_db::tree_seq::TreeSeqIdx;
use rocks_db::Storage;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::u64;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::info;
use tracing::warn;

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

    #[arg(short='g', long, value_delimiter = ',', num_args = 0..)]
    get_asset_maps_ids: Option<Vec<String>>,

    /// Checks the database for gaps with the specified number of shards
    #[arg(short, long)]
    check_for_gaps_with_shards_count: Option<u64>,

    /// Slot to check the database for gaps to. Required if `check_for_gaps_with_shards_count` is specified.
    #[arg(short, long)]
    slot_until_for_gap_checks: Option<u64>,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    info!("Starting sync util...");

    let args = Args::parse();

    let tx_storage_dir = tempfile::TempDir::new().unwrap();

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

    let storage = Storage::open_secondary(
        &args.db_path,
        &tx_storage_dir.path().to_path_buf(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();
    if let Some(index_after) = args.index_after {
        // Decode the Base58 string back into Vec<u8>
        let decoded_data = bs58::decode(&index_after)
            .into_vec()
            .expect("index after should be base58 encoded");
        let starting_key = decode_u64x2_pubkey(decoded_data).expect("Failed to decode index after");
        let (updated_keys, last_included_key) = storage
            .fetch_nft_asset_updated_keys(Some(starting_key), None, 500, None)
            .unwrap();
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

        let mut it = storage
            .db
            .raw_iterator_cf(&storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap());
        it.seek_to_first();
        while it.valid() {
            total_assets_processed += 1;
            if let Some(value_bytes) = it.value() {
                let data = fb::root_as_asset_complete_details(&value_bytes)
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
                    matching_pubkeys.insert(asset.pubkey.clone());
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
        let index = storage
            .get_nft_asset_indexes(keys.as_slice())
            .await
            .expect("Failed to get indexes");
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
    if let Some(num_shards) = args.check_for_gaps_with_shards_count {
        let last_slot = args
            .slot_until_for_gap_checks
            .expect("slot required for gap checker");
        let shards = shard_pubkeys(num_shards);
        let mut js = JoinSet::new();
        let storage = Arc::new(storage);
        for (start, end) in shards.into_iter() {
            let storage = storage.clone();
            let end_key = TreeSeqIdx::encode_key((end, u64::MAX));

            js.spawn_blocking(move || {
                let mut iter = storage
                    .db
                    .raw_iterator_cf(&storage.db.cf_handle(TreeSeqIdx::NAME).unwrap());
                iter.seek(TreeSeqIdx::encode_key((start, 0)));
                let mut prev_state = TreeState::default();
                let mut gaps = Vec::new();
                while iter.valid() {
                    let key = iter.key().unwrap().to_vec();
                    if key > end_key {
                        break;
                    }
                    let (tree, seq) = TreeSeqIdx::decode_key(iter.key().unwrap().to_vec()).unwrap();
                    let TreeSeqIdx { slot } =
                        bincode::deserialize::<TreeSeqIdx>(iter.value().unwrap().as_ref()).unwrap();
                    let current_state = TreeState { tree, seq, slot };
                    if slot > last_slot {
                        iter.next();
                        continue;
                    }
                    if current_state.tree == prev_state.tree
                        && current_state.seq != prev_state.seq + 1
                    {
                        warn!(
                            "Gap found for {} tree. Sequences: [{}, {}], slots: [{}, {}]",
                            prev_state.tree,
                            prev_state.seq,
                            current_state.seq,
                            prev_state.slot,
                            current_state.slot
                        );
                        gaps.push((
                            prev_state.tree,
                            prev_state.seq,
                            current_state.seq,
                            prev_state.slot,
                            current_state.slot,
                        ));
                    }
                    prev_state = current_state;

                    iter.next();
                }
                gaps
            });
        }
        let mut gaps = Vec::new();
        while let Some(task) = js.join_next().await {
            let mut g = task.expect("should join");
            gaps.append(&mut g);
        }
        if gaps.len() == 0 {
            info!("No gaps found.");
        } else {
            warn!("Gaps found: {:?}", gaps);
        }
    }
    Ok(())
}
