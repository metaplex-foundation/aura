use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use entities::models::{RawBlock, RawBlockDeprecated};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::{column::TypedColumn, errors::StorageError, SlotStorage, Storage};
use rocksdb::DB;
use tokio::{
    sync::{Mutex, Semaphore},
    task::{JoinError, JoinSet},
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, help = "path to the slots database you want to migrate")]
    slots_db_path: String,
    #[clap(long, help = "optional starting slot for the migration")]
    start_slot: Option<u32>,
}

const PARALLEL_WORKERS: u16 = 1024;
const WRITE_BATCH_SIZE: usize = 10_000;
/// An interval which serves to await all tasks in the joinset & drain it, deallocating task
/// memory
const JOINSET_CLEARANCE_CLOCK: u32 = 1_000_000;

fn put_batch_vec<C: TypedColumn>(
    backend: Arc<DB>,
    values: &mut Vec<(C::KeyType, C::ValueType)>,
) -> Result<(), StorageError> {
    let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
    for (k, v) in values.drain(..) {
        let serialized_value = C::encode(&v).map_err(|e| StorageError::Common(e.to_string()))?;
        batch.put_cf(
            &backend.cf_handle(C::NAME).unwrap(),
            C::encode_key(k.clone()),
            serialized_value,
        )
    }
    backend.write(batch)?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let tasks = Arc::new(Mutex::new(JoinSet::<Result<(), JoinError>>::new()));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let cf_descriptors = Storage::cfs_to_column_families(
        SlotStorage::cf_names()
            .into_iter()
            .chain(std::iter::once(RawBlockDeprecated::NAME))
            .collect(),
    );
    let db = Arc::new(
        DB::open_cf_descriptors(
            &Storage::get_db_options(),
            args.slots_db_path.clone(),
            cf_descriptors,
        )
        .expect("open rocks slot storage to migrate"),
    );
    let slot_storage = SlotStorage::new(db, tasks, red_metrics);
    eprintln!("Opened slots database in primary mode at {}", args.slots_db_path);
    let mut iter = slot_storage.db.raw_iterator_cf(
        &slot_storage.db.cf_handle(RawBlockDeprecated::NAME).expect("get raw blocks cf handle"),
    );
    let batch: Arc<Mutex<Vec<(u64, RawBlock)>>> =
        Arc::new(Mutex::new(Vec::with_capacity(WRITE_BATCH_SIZE)));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, RawBlock)>(WRITE_BATCH_SIZE);
    if let Some(start_slot) = args.start_slot {
        iter.seek(RawBlockDeprecated::encode_key(start_slot as u64));
    } else {
        iter.seek_to_first();
    }
    let start = std::time::Instant::now();
    let worker_semaphore = Arc::new(Semaphore::new(PARALLEL_WORKERS as usize));
    let mut js = tokio::task::JoinSet::<()>::new();
    let handle = tokio::task::spawn({
        let batch = batch.clone();
        let slot_storage = slot_storage.clone();
        async move {
            while let Some((slot, raw_block)) = rx.recv().await {
                let mut batch = batch.lock().await;
                batch.push((slot, raw_block));
                if batch.len() >= WRITE_BATCH_SIZE {
                    let ((start_slot, _), (end_slot, _)) =
                        (batch.first().unwrap(), batch.last().unwrap());
                    eprintln!("Writing slots {} through {} to rocksdb...", start_slot, end_slot);
                    let db = slot_storage.db.clone();
                    put_batch_vec::<RawBlock>(db, &mut *batch)
                        .expect("raw blocks batch to be inserted into rocksdb");
                }
            }
        }
    });

    let mut keys_processed = 0u32;

    while iter.valid() {
        let (k, v) = iter.item().unwrap();
        let (k, v) = (k.to_vec(), v.to_vec());
        let permit =
            worker_semaphore.clone().acquire_owned().await.expect("acquire owned semaphore permit");
        let tx = tx.clone();
        js.spawn(async move {
            let slot = RawBlockDeprecated::decode_key(k).expect("decode raw block key");
            let raw_block_deprecated =
                RawBlockDeprecated::decode(&v).expect("decode deprecated raw block value");
            let raw_block = raw_block_deprecated.into();
            tx.send((slot, raw_block)).await.expect("rx to not be closed");
            drop(permit);
        });
        keys_processed += 1;
        if keys_processed >= JOINSET_CLEARANCE_CLOCK {
            while js.join_next().await.is_some() {}
            js.detach_all();
            keys_processed = 0;
        }
        iter.next();
    }

    drop(tx);

    while js.join_next().await.is_some() {}
    handle.await.expect("rx to receive and process everything");

    let mut batch = Arc::into_inner(batch).unwrap().into_inner();
    // flush what's left if the iterator has ended
    if !batch.is_empty() {
        let ((start_slot, _), (end_slot, _)) = (batch.first().unwrap(), batch.last().unwrap());
        eprintln!("Writing slots {} through {} to rocksdb...", start_slot, end_slot);
        let map: HashMap<u64, RawBlock> = std::mem::take(&mut batch).into_iter().collect();
        slot_storage.raw_blocks.put_batch(map).await.expect("raw blocks batch");
    }

    slot_storage.db.drop_cf(RawBlockDeprecated::NAME).expect("delete raw blocks deprecated cf");
    slot_storage.db.flush().expect("flush data after having finished the migration");
    let elapsed = start.elapsed().as_secs_f32();

    eprintln!("Finished migration successfully. Took {} seconds", elapsed);
}
