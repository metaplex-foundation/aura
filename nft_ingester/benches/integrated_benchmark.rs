use criterion::{criterion_group, criterion_main, Criterion};
use nft_ingester::api::SearchAssets;
use setup::TestEnvironment;
use std::{sync::Arc, time::Duration};

use metrics_utils::ApiMetricsConfig;
use testcontainers::clients::Cli;

const SLOT_UPDATED: u64 = 100;

async fn benchmark_search_assets(api: Arc<nft_ingester::api::api_impl::DasApi>, limit: u32) {
    let payload = SearchAssets {
        limit: Some(limit),
        ..Default::default()
    };
    let _res = api.search_assets(payload).await.unwrap();
    // You can add more assertions or processing here as needed
}

async fn setup_environment<'a>(cli: &'a Cli) -> TestEnvironment<'a> {
    let cnt = 100_000; // Number of records for setup
    let (env, _generated_assets) = setup::TestEnvironment::create(cli, cnt, SLOT_UPDATED).await;
    env
}

fn criterion_benchmark(c: &mut Criterion) {
    let cli: Cli = Cli::default();
    let limit = 1000; // Number of records to fetch
    let rt = tokio::runtime::Runtime::new().unwrap();
    let env = rt.block_on(setup_environment(&cli));
    let api = nft_ingester::api::api_impl::DasApi::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(ApiMetricsConfig::new()),
    );

    let api = Arc::new(api);
    c.bench_function("search_assets", |b| {
        b.iter(|| rt.block_on(benchmark_search_assets(api.clone(), limit)))
    });
    rt.block_on(env.teardown());
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
