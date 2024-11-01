use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use entities::api_req_params::Options;
use metrics_utils::SynchronizerMetricsConfig;
use rocks_db::{
    storage_traits::{AssetIndexReader, Dumper},
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

async fn bench_batch_get_keys(storage: Arc<Storage>, pubkeys: Vec<Pubkey>) {
    storage.asset_dynamic_data.batch_get(pubkeys).await.unwrap();
    // storage.asset_data.batch_get(pubkeys)
    //     .await
    //     .unwrap();
}

// async fn simple_iterate(storage: Arc<Storage>) {
//     for k in storage.asset_data.iter_start() {
//         let _ = k;
//     }
// }

// async fn deserialized_iterate(storage: Arc<Storage>) {
//     for k in storage
//         .asset_data
//         .pairs_iterator(storage.asset_data.iter_start())
//     {
//         let (_k, _v) = k;
//     }
// }

// async fn deserialized_only_value_iterate(storage: Arc<Storage>) {
//     for k in storage
//         .asset_data
//         .values_iterator(storage.asset_data.iter_start())
//     {
//         let _ = k;
//     }
// }

// async fn collect_simple(storage: Arc<Storage>) {
//     let core_collections: HashMap<Pubkey, Pubkey> = storage
//             .asset_data
//             .values_iterator(storage.asset_data.iter_start())
//             .filter_map(|a| {
//                 a.static_details
//                     .filter(|sd| {
//                         sd.specification_asset_class == SpecificationAssetClass::MplCoreCollection
//                     })
//                     .map(|_| a.collection)
//                     .flatten()
//             })
//             .filter_map(|a| a.authority.value.map(|v| (a.pubkey, v)))
//             .collect();
// }

// async fn collect_plain(storage: Arc<Storage>) {
//     let core_collections: HashMap<Pubkey, Pubkey> = storage
//             .asset_data
//             .values_iterator(storage.asset_data.iter_start())
//             .filter(|a|a.static_details.as_ref().is_some_and(|sd|
//                 sd.specification_asset_class == SpecificationAssetClass::MplCoreCollection) && a.collection.as_ref().is_some_and(|c| c.authority.value.is_some()))
//             .map(|a| (a.pubkey, a.collection.unwrap().authority.value.unwrap()))
//             .collect();
// }
async fn bench_get_assets(storage: Arc<Storage>, pubkeys: Vec<Pubkey>) {
    storage
        .get_asset_selected_maps_async(
            pubkeys,
            &None,
            &Options {
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

async fn bench_get_asset_indexes(storage: Arc<Storage>, pubkeys: Vec<Pubkey>) {
    storage.get_asset_indexes(&pubkeys).await.unwrap();
}

async fn bench_get_assets_individually(storage: Arc<Storage>, pubkeys: Vec<Pubkey>) {
    for pubkey in pubkeys {
        storage
            .get_asset_selected_maps_async(vec![pubkey], &None, &Options::default())
            .await
            .unwrap();
    }
}

async fn bench_dump(storage: Arc<Storage>, batch_size: usize) {
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let temp_dir_path = temp_dir.path();

    let sync_metrics = Arc::new(SynchronizerMetricsConfig::new());
    storage
        .dump_db(temp_dir_path, batch_size, &rx, sync_metrics)
        .await
        .unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_dump() {
    println!("Starting the test");
    info!("Starting the test");
    let env = setup::rocks::RocksTestEnvironment::new(&[]);
    let cnt = 1_000;
    println!("env created, generating assets");
    env.generate_assets(cnt, 25).await;
    println!("assets generated");
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let temp_dir_path = temp_dir.path();
    let sync_metrics = Arc::new(SynchronizerMetricsConfig::new());
    env.storage
        .dump_db(temp_dir_path, batch_size, &rx, sync_metrics)
        .await
        .expect("Failed to dump the database");
    println!("dump complete");
}
fn noop_benchmark(_c: &mut Criterion) {}

fn dump_benchmark(c: &mut Criterion) {
    let env = setup::rocks::RocksTestEnvironment::new(&[]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let assets = rt.block_on(async {
        let cnt = 1_000_000;
        env.generate_assets(cnt, 25).await
    });
    let sampled_pubkeys: Vec<Pubkey> = (99..assets.pubkeys.len())
        .step_by(100)
        .take(1000)
        .map(|i| assets.pubkeys[i].clone())
        .collect();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Dumping Group");
    group.sample_size(10);
    // group.measurement_time(std::time::Duration::from_secs(60));

    let storage = env.storage;
    group.bench_function("get_assets", |b| {
        b.iter(|| rt.block_on(bench_get_assets(storage.clone(), sampled_pubkeys.clone())))
    });
    group.bench_function("get_assets_individually", |b| {
        b.iter(|| {
            rt.block_on(bench_get_assets_individually(
                storage.clone(),
                sampled_pubkeys.clone(),
            ))
        })
    });
    group.bench_function("get_asset_indexes", |b| {
        b.iter(|| {
            rt.block_on(bench_get_asset_indexes(
                storage.clone(),
                sampled_pubkeys.clone(),
            ))
        })
    });
    // group.bench_function("batch_get_keys", |b| {
    //     b.iter(|| rt.block_on(bench_batch_get_keys(storage.clone(), sampled_pubkeys.clone())))
    // });
    // group.bench_function("collect_simple", |b| {
    //     b.iter(|| rt.block_on(collect_simple(storage.clone())))
    // });
    // group.bench_function("collect_plain", |b| {
    //     b.iter(|| rt.block_on(collect_plain(storage.clone())))
    // });
    // group.bench_function("simple_iterate", |b| {
    //     b.iter(|| rt.block_on(simple_iterate(storage.clone())))
    // });
    // group.bench_function("deserialized_iterate", |b| {
    //     b.iter(|| rt.block_on(deserialized_iterate(storage.clone())))
    // });
    // group.bench_function("deserialized_only_value_iterate", |b| {
    //     b.iter(|| rt.block_on(deserialized_only_value_iterate(storage.clone())))
    // });
    // group.bench_function("500 batch size", |b| {
    //     b.iter(|| rt.block_on(bench_dump(storage.clone(), 500)))
    // });
    // group.bench_function("1k batch size", |b| {
    //     b.iter(|| rt.block_on(bench_dump(storage.clone(), 1_000)))
    // });
    group.bench_function("2k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 2_000)))
    });
    // group.bench_function("5k batch size", |b| {
    //     b.iter(|| rt.block_on(bench_dump(storage.clone(), 5_000)))
    // });
    // group.bench_function("10k batch size", |b| {
    //     b.iter(|| rt.block_on(bench_dump(storage.clone(), 10_000)))
    // });
    // group.bench_function("20k batch size", |b| {
    //     b.iter(|| rt.block_on(bench_dump(storage.clone(), 20_000)))
    // });
}

criterion_group!(benches, dump_benchmark);
criterion_main!(benches);
