use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use entities::models::{RawBlock, RawBlockDeprecated};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::{column::TypedColumn, SlotStorage};
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, help = "path to the slots database you want to migrate")]
    slots_db_path: String,
}

const WRITE_BATCH_SIZE: usize = 10_000;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let tasks = Arc::new(Mutex::new(JoinSet::<Result<(), JoinError>>::new()));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let slot_storage = SlotStorage::open(args.slots_db_path.clone(), tasks, red_metrics)
        .expect("open slots db in primary mode for migration");
    eprintln!("Opened slots database in primary mode at {}", args.slots_db_path);
    let mut iter = slot_storage.db.raw_iterator_cf(
        &slot_storage.db.cf_handle(RawBlockDeprecated::NAME).expect("get raw blocks cf handle"),
    );
    let mut batch: Vec<(u64, RawBlock)> = Vec::with_capacity(WRITE_BATCH_SIZE);
    iter.seek_to_first();
    while iter.valid() {
        let (k, v) = iter.item().unwrap();
        let slot = RawBlockDeprecated::decode_key(k.to_vec()).expect("decode raw block key");
        let raw_block_deprecated =
            RawBlockDeprecated::decode(v).expect("decode deprecated raw block value");
        let raw_block: RawBlock = raw_block_deprecated.into();
        if batch.len() < WRITE_BATCH_SIZE {
            batch.push((slot, raw_block));
        } else {
            let ((start_slot, _), (end_slot, _)) = (batch.first().unwrap(), batch.last().unwrap());
            eprintln!("Writing slots {} through {} to rocksdb...", start_slot, end_slot);
            let map: HashMap<u64, RawBlock> = std::mem::take(&mut batch).into_iter().collect();
            slot_storage.raw_blocks.put_batch(map).await.expect("raw blocks batch");
        }
        iter.next();
    }

    // flush what's left if the iterator has ended
    if !batch.is_empty() {
        let ((start_slot, _), (end_slot, _)) = (batch.first().unwrap(), batch.last().unwrap());
        eprintln!("Writing slots {} through {} to rocksdb...", start_slot, end_slot);
        let map: HashMap<u64, RawBlock> = std::mem::take(&mut batch).into_iter().collect();
        slot_storage.raw_blocks.put_batch(map).await.expect("raw blocks batch");
    }

    slot_storage.db.drop_cf(RawBlockDeprecated::NAME).expect("delete raw blocks deprecated cf");
    slot_storage.db.flush().expect("flush data after having finished the migration");

    eprintln!("Finished migration successfully.");
}
