use bincode::{deserialize, serialize};
use criterion::{criterion_group, criterion_main, Criterion};
use rocks_db::{ AssetDynamicDetails};
use setup::rocks::RocksTestEnvironmentSetup;
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

// fn cbor_vs_bincode_decode_benchmark(c: &mut Criterion) {
//     let cnt = 1_000;
//     let slot = 100;
//     let pubkeys = (0..cnt).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
//     let static_details = RocksTestEnvironmentSetup::static_data_for_nft(&pubkeys, slot);
//     let authorities = RocksTestEnvironmentSetup::with_authority(&pubkeys);
//     let owners = RocksTestEnvironmentSetup::test_owner(&pubkeys);
//     let dynamic_details: Vec<AssetDynamicDetails> = RocksTestEnvironmentSetup::dynamic_data(&pubkeys, slot);
//     let collections = RocksTestEnvironmentSetup::collection_without_authority(&pubkeys);
// let dclone = dynamic_details.clone();
//     let assets = pubkeys.iter()
//         .zip(static_details)
//         .zip(authorities)
//         .zip(owners)
//         .zip(dynamic_details)
//         .zip(collections)
//         .map(
//             |(((((pk, static_data), authority), owner), dynamic), collection)| AssetCompleteDetails {
//                 pubkey: *pk,
//                 static_details: Some(static_data),
//                 dynamic_details: Some(dynamic),
//                 authority: Some(authority),
//                 owner: Some(owner),
//                 collection: Some(collection),
//             },
//         )
//         .collect::<Vec<_>>();

//     c.bench_function("encode/decode AssetCompleteDetails (1000 items)", |b| {
//         b.iter(|| {
//             assets
//                 .iter()
//                 .map(|a| serialize(&a).unwrap())
//                 .map(|b| deserialize::<AssetCompleteDetails>(&b).unwrap())
//                 .collect::<Vec<_>>()
//         })
//     });
//     c.bench_function("encode/decode dynamic details (1000 items)", |b| {
//         b.iter(|| {
//             dclone
//                 .iter()
//                 .map(|a| serialize(&a).unwrap())
//                 .map(|b| deserialize::<AssetDynamicDetails>(&b).unwrap())
//                 .collect::<Vec<_>>()
//         })
//     });
// }

criterion_group!(benches, bincode_decode_benchmark);
criterion_main!(benches);
