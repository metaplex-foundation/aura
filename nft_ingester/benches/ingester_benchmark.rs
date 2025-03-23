use std::{fs::File, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use metrics_utils::{
    red::RequestErrorDurationMetrics, BackfillerMetricsConfig, IngesterMetricsConfig,
};
use nft_ingester::{
    backfiller::DirectBlockParser, buffer::Buffer,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
    transaction_ingester,
};
use rocks_db::{migrator::MigrationState, Storage};
use setup::TestEnvironment;
use testcontainers::clients::Cli;
use tokio::{sync::Mutex, task::JoinSet};

async fn setup_environment<'a>(
    cli: &'a Cli,
) -> (TestEnvironment<'a>, setup::rocks::GeneratedAssets) {
    setup::TestEnvironment::create(cli, 0, 100).await
}

async fn bench_ingest(
    rocks_client_raw: Arc<rocks_db::Storage>,
    rocks_dest: Arc<rocks_db::Storage>,
    workers_count: usize,
    chunk_size: usize,
    permits: usize,
) {
    let buffer = Arc::new(Buffer::new());

    let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        rocks_dest.clone(),
        Arc::new(IngesterMetricsConfig::new()),
    ));

    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        bubblegum_updates_processor.clone(),
    ));

    let consumer = Arc::new(DirectBlockParser::new(
        tx_ingester.clone(),
        rocks_dest.clone(),
        Arc::new(BackfillerMetricsConfig::new()),
    ));

    let transactions_parser = Arc::new(TransactionsParser::new(
        rocks_client_raw.clone(),
        Arc::new(BubblegumSlotGetter::new(rocks_client_raw.clone())),
        consumer,
        rocks_client_raw,
        Arc::new(BackfillerMetricsConfig::new()),
        workers_count,
        chunk_size,
    ));
    let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
    transactions_parser.parse_raw_transactions(rx, permits, None).await;
}

fn ingest_benchmark(c: &mut Criterion) {
    let tx_storage_dir = tempfile::TempDir::new().unwrap();

    let storage_archieve = File::open("./tests/artifacts/test_rocks.zip").unwrap();

    zip_extract::extract(storage_archieve, tx_storage_dir.path(), false).unwrap();
    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let transactions_storage = Storage::open(
        &format!("{}{}", tx_storage_dir.path().to_str().unwrap(), "/test_rocks"),
        red_metrics,
        MigrationState::Last,
    )
    .unwrap();

    let rocks_storage = Arc::new(transactions_storage);

    let cli: Cli = Cli::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (env, _generated_assets) = rt.block_on(setup_environment(&cli));
    let mut group = c.benchmark_group("Ingestion Group");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));
    group.bench_function("10 worker mode, 1 in chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(
                rocks_storage.clone(),
                env.rocks_env.storage.clone(),
                10,
                1,
                1,
            ))
        })
    });
    group.bench_function("20 worker mode, 1 in chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(
                rocks_storage.clone(),
                env.rocks_env.storage.clone(),
                20,
                1,
                1,
            ))
        })
    });
    group.bench_function("50 worker mode, 1 in a chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(
                rocks_storage.clone(),
                env.rocks_env.storage.clone(),
                50,
                1,
                1,
            ))
        })
    });
    group.bench_function("100 worker mode, 1 in a chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(
                rocks_storage.clone(),
                env.rocks_env.storage.clone(),
                100,
                1,
                1,
            ))
        })
    });
    group.bench_function("5 workers mode, 1 in a chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(rocks_storage.clone(), env.rocks_env.storage.clone(), 5, 1, 1))
        })
    });
    group.bench_function("10 workers mode, 10 in a chunk", |b| {
        b.iter(|| {
            rt.block_on(bench_ingest(
                rocks_storage.clone(),
                env.rocks_env.storage.clone(),
                10,
                10,
                1,
            ))
        })
    });
    rt.block_on(env.teardown());
}

criterion_group!(benches, ingest_benchmark);
criterion_main!(benches);
