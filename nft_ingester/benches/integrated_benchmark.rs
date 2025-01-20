use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use entities::api_req_params::{Options, SearchAssets};
use interface::price_fetcher::TokenPriceFetcher;
use metrics_utils::{red::RequestErrorDurationMetrics, ApiMetricsConfig};
use nft_ingester::api::account_balance::AccountBalanceGetterImpl;
use rocks_db::{storage_traits::AssetIndexReader, Storage};
use setup::TestEnvironment;
use solana_sdk::pubkey::Pubkey;
use testcontainers::clients::Cli;
use tokio::task::{JoinError, JoinSet};

const SLOT_UPDATED: u64 = 100;

async fn benchmark_search_assets(
    storage: Arc<Storage>,
    asset_ids: &[Pubkey],
    owner_address: Pubkey,
) {
    let _res = storage
        .get_asset_selected_maps_async(
            asset_ids.to_vec(),
            &Some(owner_address),
            &Options {
                show_unverified_collections: true,
                show_collection_metadata: true,
                show_inscription: true,
                show_fungible: true,
            },
        )
        .await
        .unwrap();
    // You can add more assertions or processing here as needed
}

async fn setup_environment<'a>(
    cli: &'a Cli,
) -> (TestEnvironment<'a>, setup::rocks::GeneratedAssets) {
    let cnt = 1_000_000; // Number of records for setup
    setup::TestEnvironment::create(cli, cnt, SLOT_UPDATED).await
}

fn search_assets_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Search Assets");
    group.warm_up_time(Duration::from_secs(60)).sample_size(100);
    let rt = tokio::runtime::Runtime::new().unwrap();
    // let rpc_url = std::env::var("BENCHMARK_RPC_URL").unwrap();
    // let client = Arc::new(RpcClient::new(rpc_url.to_string()));
    // let api = nft_ingester::api::api_impl::DasApi::new(
    //     env.pg_env.client.clone(),
    //     env.rocks_env.storage.clone(),
    //     Arc::new(ApiMetricsConfig::new()),
    //     None,
    //     None,
    //     1000,
    //     None,
    //     None,
    //     JsonMiddlewareConfig::default(),
    //     Arc::new(AccountBalanceGetterImpl::new(client.clone())),
    //     None,
    //     Arc::new(RaydiumTokenPriceFetcher::new(
    //         "".to_string(), // API url, is not used in tests
    //         raydium_price_fetcher::CACHE_TTL,
    //         None,
    //     )),
    //     "11111111111111111111111111111111".to_string(),
    // );
    let red_metrics = RequestErrorDurationMetrics::new();
    let primary_path = std::env::var("BENCHMARK_PRIMARY_PATH")
        .expect("primary path to be provided via BENCHMARK_PRIMARY_PATH");
    let secondary_path = std::env::var("BENCHMARK_SECONDARY_PATH")
        .expect("secondary path to be provided via BENCHMARK_SECONDARY_PATH");
    let owner_address = Pubkey::from_str(
        &std::env::var("BENCHMARK_OWNER_ADDRESS")
            .expect("owner address to be provided via BENCHMARK_OWNER_ADDRESS"),
    )
    .unwrap();
    let asset_ids_csv_path = std::env::var("BENCHMARK_ASSET_IDS_PATH")
        .expect("asset ids csv path to be provided via BENCHMARK_ASSET_IDS_PATH");
    let reader =
        csv::ReaderBuilder::new().has_headers(false).from_path(asset_ids_csv_path).unwrap();
    let asset_ids = reader
        .into_records()
        .map(|r| Pubkey::from_str(&r.unwrap().into_iter().next().unwrap()).unwrap())
        .collect::<Vec<_>>();
    let storage = Arc::new(
        Storage::open_secondary(
            PathBuf::from_str(&primary_path).expect("primary path to be valid"),
            PathBuf::from_str(&secondary_path).expect("secondary path to be valid"),
            Arc::new(tokio::sync::Mutex::new(JoinSet::<Result<(), JoinError>>::new())),
            Arc::new(red_metrics),
            rocks_db::migrator::MigrationState::Last,
        )
        .expect("open secondary storage"),
    );

    group.bench_function("search_assets", |b| {
        b.iter(|| rt.block_on(benchmark_search_assets(storage.clone(), &asset_ids, owner_address)))
    });
    group.finish();
}

// async fn bench_delete_op(
//     pg_client: Arc<postgre_client::PgClient>,
//     rocks_db: Arc<rocks_db::Storage>,
//     assets: setup::rocks::GeneratedAssets,
// ) {
//     let pubkeys = assets.pubkeys[50000..51000].to_vec();
//     Synchronizer::syncronize_batch(
//         rocks_db.clone(),
//         pg_client.clone(),
//         &pubkeys,
//         Default::default(),
//     )
//     .await
//     .unwrap();
// }

async fn bench_get_asset_indexes(
    rocks_db: Arc<rocks_db::Storage>,
    assets: setup::rocks::GeneratedAssets,
) {
    let pubkeys = assets.pubkeys[50000..51000].to_vec();

    rocks_db.get_nft_asset_indexes(&pubkeys).await.unwrap();
}

async fn bench_get_dynamic_data_batch(
    rocks_db: Arc<rocks_db::Storage>,
    assets: setup::rocks::GeneratedAssets,
) {
    let pubkeys = assets.pubkeys[50000..51000].to_vec();
    rocks_db.asset_dynamic_data.batch_get(pubkeys.clone()).await.unwrap();
}

fn pg_delete_benchmark(c: &mut Criterion) {
    let cli: Cli = Cli::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (env, generated_assets) = rt.block_on(setup_environment(&cli));
    let mut group = c.benchmark_group("My Group");

    // group.bench_function("delete_creators_with_select", |b| {
    //     b.iter(|| {
    //         rt.block_on(bench_delete_op(
    //             env.pg_env.client.clone(),
    //             env.rocks_env.storage.clone(),
    //             generated_assets.clone(),
    //         ))
    //     })
    // });

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

criterion_group!(benches, search_assets_benchmark);
criterion_main!(benches);
