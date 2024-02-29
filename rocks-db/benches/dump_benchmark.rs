use std::{collections::HashSet, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use rocks_db::{storage_traits::Dumper, Storage};
use tempfile::TempDir;

async fn bench_dump(storage: Arc<Storage>, batch_size: usize) {
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let temp_dir_path = temp_dir.path();

    storage
        .dump_db(temp_dir_path, HashSet::new(), batch_size, rx)
        .await
        .unwrap();
}

fn dump_benchmark(c: &mut Criterion) {
    let env = setup::rocks::RocksTestEnvironment::new(&[]);
    let cnt = 1_000_000;
    _ = env.generate_assets(cnt, 25);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Dumping Group");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));

    let storage = env.storage;
    group.bench_function("500 batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 500)))
    });
    group.bench_function("1k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 1_000)))
    });
    group.bench_function("2k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 2_000)))
    });
    group.bench_function("5k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 5_000)))
    });
    group.bench_function("10k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 10_000)))
    });
    group.bench_function("20k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump(storage.clone(), 20_000)))
    });
}

async fn bench_dump_on_complete(storage: Arc<Storage>, batch_size: usize) {
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let temp_dir_path = temp_dir.path();

    storage
        .dump_db_complete_data(temp_dir_path, HashSet::new(), batch_size, rx)
        .await
        .unwrap();
}

fn dump_benchmark_on_complete_data(c: &mut Criterion) {
    let env = setup::rocks::RocksTestEnvironment::new(&[]);
    let cnt = 1_000_000;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(env.generate_complete_assets(cnt, 25));
    let mut group = c.benchmark_group("Dumping Group with complete data");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));

    let storage = env.storage;
    group.bench_function("500 batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 500)))
    });
    group.bench_function("1k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 1_000)))
    });
    group.bench_function("2k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 2_000)))
    });
    group.bench_function("5k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 5_000)))
    });
    group.bench_function("10k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 10_000)))
    });
    group.bench_function("20k batch size", |b| {
        b.iter(|| rt.block_on(bench_dump_on_complete(storage.clone(), 20_000)))
    });
}
criterion_group!(benches, dump_benchmark, dump_benchmark_on_complete_data);
criterion_main!(benches);
