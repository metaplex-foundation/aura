use criterion::{criterion_group, criterion_main, Criterion};
use entities::api_req_params::SearchAssets;
use nft_ingester::index_syncronizer::Synchronizer;
use rocks_db::storage_traits::AssetIndexReader;
use setup::TestEnvironment;
use std::sync::Arc;
use usecase::proofs::MaybeProofChecker;

use metrics_utils::ApiMetricsConfig;
use testcontainers::clients::Cli;

const SLOT_UPDATED: u64 = 100;

async fn benchmark_search_assets(
    api: Arc<nft_ingester::api::api_impl::DasApi<MaybeProofChecker>>,
    limit: u32,
) {
    let payload = SearchAssets {
        limit: Some(limit),
        ..Default::default()
    };
    let _res = api.search_assets(payload).await.unwrap();
    // You can add more assertions or processing here as needed
}

async fn setup_environment<'a>(
    cli: &'a Cli,
) -> (TestEnvironment<'a>, setup::rocks::GeneratedAssets) {
    let cnt = 1_000_000; // Number of records for setup
    setup::TestEnvironment::create(cli, cnt, SLOT_UPDATED).await
}

fn search_assets_benchmark(c: &mut Criterion) {
    let cli: Cli = Cli::default();
    let limit: u32 = 1000; // Number of records to fetch
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (env, _generated_assets) = rt.block_on(setup_environment(&cli));
    let api = nft_ingester::api::api_impl::DasApi::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(ApiMetricsConfig::new()),
        None,
    );

    let api = Arc::new(api);
    c.bench_function("search_assets", |b| {
        b.iter(|| rt.block_on(benchmark_search_assets(api.clone(), limit)))
    });
    rt.block_on(env.teardown());
}

async fn bench_delete_op(
    pg_client: Arc<postgre_client::PgClient>,
    rocks_db: Arc<rocks_db::Storage>,
    assets: setup::rocks::GeneratedAssets,
) {
    let pubkeys = assets.pubkeys[50000..51000].to_vec();
    Synchronizer::syncronize_batch(
        rocks_db.clone(),
        pg_client.clone(),
        &pubkeys,
        Default::default(),
    )
    .await
    .unwrap();
}

async fn bench_get_asset_indexes(
    rocks_db: Arc<rocks_db::Storage>,
    assets: setup::rocks::GeneratedAssets,
) {
    let pubkeys = assets.pubkeys[50000..51000].to_vec();

    rocks_db.get_asset_indexes(&pubkeys).await.unwrap();
}

async fn bench_get_dynamic_data_batch(
    rocks_db: Arc<rocks_db::Storage>,
    assets: setup::rocks::GeneratedAssets,
) {
    let pubkeys = assets.pubkeys[50000..51000].to_vec();
    rocks_db
        .asset_dynamic_data
        .batch_get(pubkeys.clone())
        .await
        .unwrap();
}

fn pg_delete_benchmark(c: &mut Criterion) {
    let cli: Cli = Cli::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (env, generated_assets) = rt.block_on(setup_environment(&cli));
    let mut group = c.benchmark_group("My Group");

    group.bench_function("delete_creators_with_select", |b| {
        b.iter(|| {
            rt.block_on(bench_delete_op(
                env.pg_env.client.clone(),
                env.rocks_env.storage.clone(),
                generated_assets.clone(),
            ))
        })
    });

    group.bench_function("get asset indexes", |b| {
        b.iter(|| {
            rt.block_on(bench_get_asset_indexes(
                env.rocks_env.storage.clone(),
                generated_assets.clone(),
            ))
        })
    });

    group.bench_function("get dynamic details batch", |b| {
        b.iter(|| {
            rt.block_on(bench_get_dynamic_data_batch(
                env.rocks_env.storage.clone(),
                generated_assets.clone(),
            ))
        })
    });
    group.finish();
    rt.block_on(env.teardown());
}

criterion_group!(benches, search_assets_benchmark, pg_delete_benchmark,);
criterion_main!(benches);
