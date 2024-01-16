use bincode::{deserialize, serialize};
use criterion::{criterion_group, criterion_main, Criterion};
use rocks_db::AssetDynamicDetails;
use solana_sdk::pubkey::Pubkey;

fn bincode_decode_benchmark(c: &mut Criterion) {
    let cnt = 1_000;
    let pubkeys = (0..cnt).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
    let slot = 100;
    let assets = pubkeys
        .iter()
        .map(|pk| setup::rocks::create_test_dynamic_data(*pk, slot, "solana".to_string()))
        .map(|a| serialize(&a).unwrap())
        .collect::<Vec<_>>();

    c.bench_function("decode bincode (1000 items)", |b| {
        b.iter(|| {
            assets
                .iter()
                .map(|s| deserialize::<AssetDynamicDetails>(s).unwrap())
                .collect::<Vec<_>>()
        })
    });
}

criterion_group!(benches, bincode_decode_benchmark);
criterion_main!(benches);
