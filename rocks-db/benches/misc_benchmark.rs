use std::io::Read;

use bincode::{deserialize, serialize};
use criterion::{criterion_group, criterion_main, Criterion};
use rocks_db::{
    asset::{self, AssetCompleteDetails},
    AssetDynamicDetails,
};
use setup::rocks::RocksTestEnvironmentSetup;
use solana_sdk::pubkey::Pubkey;

fn bincode_decode_benchmark(c: &mut Criterion) {
    let cnt = 1_000;
    let pubkeys = (0..cnt)
        .map(|_| Pubkey::from(rand::random::<[u8; 32]>()))
        .collect::<Vec<_>>();
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
fn flatbuffer_vs_bincode_merge_functions_benchmark(c: &mut Criterion) {
    let cnt = 1;
    let updates_number = 1000;
    let slot = 100;
    let pubkeys = (0..cnt)
        .map(|_| Pubkey::from(rand::random::<[u8; 32]>()))
        .collect::<Vec<_>>();
    let static_details = RocksTestEnvironmentSetup::static_data_for_nft(&pubkeys, slot);
    let authorities = (0..updates_number)
        .map(|_| RocksTestEnvironmentSetup::with_authority(&pubkeys))
        .collect::<Vec<_>>();
    let owners = (0..updates_number)
        .map(|_| RocksTestEnvironmentSetup::test_owner(&pubkeys))
        .collect::<Vec<_>>();
    let dynamic_details = (0..updates_number)
        .map(|_| RocksTestEnvironmentSetup::dynamic_data(&pubkeys, slot))
        .collect::<Vec<_>>();
    let collections = (0..updates_number)
        .map(|_| RocksTestEnvironmentSetup::collection_without_authority(&pubkeys))
        .collect::<Vec<_>>();

    let assets_versions = (0..updates_number)
        .map(|i| AssetCompleteDetails {
            pubkey: *pubkeys.get(0).unwrap(),
            static_details: static_details.get(0).map(|v| v.to_owned()),
            dynamic_details: dynamic_details
                .get(i)
                .and_then(|d| d.get(0))
                .map(|v| v.to_owned()),
            authority: authorities
                .get(i)
                .and_then(|d| d.get(0))
                .map(|v| v.to_owned()),
            owner: owners.get(i).and_then(|d| d.get(0)).map(|v| v.to_owned()),
            collection: collections
                .get(i)
                .and_then(|d| d.get(0))
                .map(|v| v.to_owned()),
        })
        .collect::<Vec<_>>();
    let bincode_bytes_versions = assets_versions
        .iter()
        .map(|a| serialize(&a).unwrap())
        .collect::<Vec<_>>();
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let fb_bytes_versions = assets_versions
        .iter()
        .map(|asset| {
            builder.reset();
            let asset_fb = asset.convert_to_fb(&mut builder);
            builder.finish_minimal(asset_fb);
            builder.finished_data().to_vec()
        })
        .collect::<Vec<_>>();
    let key = rand::random::<[u8; 32]>();

    c.bench_function("merge with a bincode object", |b| {
        b.iter(|| {
            let mut existing_val: Option<Vec<u8>> = None;
            bincode_bytes_versions.iter().for_each(|bincode_bytes| {
                let new_val = AssetCompleteDetails::merge_complete_details_raw(
                    &key,
                    existing_val.as_ref().map(|v| v.as_slice()),
                    vec![bincode_bytes.as_slice()].into_iter(),
                );
                existing_val = Some(new_val.expect("should merge"))
            });
        })
    });
    c.bench_function("merge with a flatbuffer object", |b| {
        b.iter(|| {
            let mut existing_val: Option<Vec<u8>> = None;
            fb_bytes_versions.iter().for_each(|fb_bytes| {
                let new_val = asset::merge_complete_details_fb_raw(
                    &key,
                    existing_val.as_ref().map(|v| v.as_slice()),
                    vec![fb_bytes.as_slice()].into_iter(),
                );
                existing_val = Some(new_val.expect("should merge"))
            });
        })
    });
    c.bench_function("merge with a flatbuffer object via simple objects", |b| {
        b.iter(|| {
            let mut existing_val: Option<Vec<u8>> = None;
            fb_bytes_versions.iter().for_each(|fb_bytes| {
                let new_val = asset::merge_complete_details_fb_through_proxy(
                    &key,
                    existing_val.as_ref().map(|v| v.as_slice()),
                    vec![fb_bytes.as_slice()].into_iter(),
                );
                existing_val = Some(new_val.expect("should merge"))
            });
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

criterion_group!(
    benches,
    bincode_decode_benchmark,
    flatbuffer_vs_bincode_merge_functions_benchmark
);
criterion_main!(benches);
