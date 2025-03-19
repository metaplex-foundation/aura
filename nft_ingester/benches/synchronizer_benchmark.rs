use std::sync::{atomic::AtomicBool, Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use metrics_utils::SynchronizerMetricsConfig;
use setup::TestEnvironment;
use sqlx::Executor;
use testcontainers::clients::Cli;

async fn setup_environment<'a>(
    cli: &'a Cli,
) -> (TestEnvironment<'a>, setup::rocks::GeneratedAssets) {
    let (env, _) = setup::TestEnvironment::create(cli, 0, 100).await;
    let cnt = 1_000_000; // Number of records for the setup
    let assets = env.rocks_env.generate_assets(cnt, 100).await;
    (env, assets)
}

async fn bench_synchronize(env: Arc<TestEnvironment<'_>>, batch_size: usize) {
    sqlx::query("update last_synced_key set last_synced_asset_update_key = null where id = 1;")
        .execute(&env.pg_env.pool)
        .await
        .unwrap();
    let metrics = Arc::new(SynchronizerMetricsConfig::new());
    let synchronizer = nft_ingester::index_synchronizer::Synchronizer::new(
        env.rocks_env.storage.clone(),
        env.pg_env.client.clone(),
        env.pg_env.client.clone(),
        batch_size,
        "".to_string(),
        metrics.clone(),
        1,
        false,
    );

    let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
    synchronizer.synchronize_asset_indexes(&rx, 0).await.unwrap();
}

fn sync_benchmark(c: &mut Criterion) {
    let cli: Cli = Cli::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (env, _generated_assets) = rt.block_on(setup_environment(&cli));
    let mut group = c.benchmark_group("Synchronizer Group");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));
    let env = Arc::new(env);
    group.bench_function("200k batch size", |b| {
        b.iter(|| rt.block_on(bench_synchronize(env.clone(), 200_000)))
    });
    group.bench_function("1M batch size", |b| {
        b.iter(|| rt.block_on(bench_synchronize(env.clone(), 1_000_000)))
    });
    group.bench_function("10k batch size", |b| {
        b.iter(|| rt.block_on(bench_synchronize(env.clone(), 10_000)))
    });
    group.bench_function("small batches of 1000 records, as before", |b| {
        b.iter(|| rt.block_on(bench_synchronize(env.clone(), 1000)))
    });
    group.bench_function("100k batch size", |b| {
        b.iter(|| rt.block_on(bench_synchronize(env.clone(), 100_000)))
    });

    rt.block_on(env.teardown());
}

criterion_group!(benches, sync_benchmark);
criterion_main!(benches);
