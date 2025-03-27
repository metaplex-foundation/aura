use std::{collections::HashMap, str::FromStr, sync::Arc};

use entities::{
    api_req_params::{GetAsset, GetAssetBatch, GetAssetsByGroup, GetNftEditions, SearchAssets},
    enums::{AssetType, AssetType::Fungible, TokenMetadataEdition},
    models::{EditionMetadata, EditionV1, MasterEdition},
};
use function_name::named;
use itertools::Itertools;
use metrics_utils::IngesterMetricsConfig;
use nft_ingester::{
    api::dapi::response::AssetList,
    consts::RAYDIUM_API_HOST,
    processors::account_based::mplx_updates_processor::MplxAccountsProcessor,
    raydium_price_fetcher::{RaydiumTokenPriceFetcher, CACHE_TTL},
    scheduler::{update_fungible_token_static_details, Scheduler},
};
use rocks_db::batch_savers::BatchSaveStorage;
use serial_test::serial;
use solana_sdk::pubkey::Pubkey;
use tokio_util::sync::CancellationToken;
use AssetType::NonFungible;

use super::common::*;

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_reg_get_asset() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: None,
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["CMVuYDS9nTeujfTPJb8ik7CRhAqZv4DfjfdamFLkJgxE"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "CMVuYDS9nTeujfTPJb8ik7CRhAqZv4DfjfdamFLkJgxE"
    }
    "#;

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_reg_get_asset_batch() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: None,
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "HTKAVZZrDdyecCxzm3WEkCsG1GUmiqKm73PvngfuYRNK",
        "2NqdYX6kJmMUoChnDXU2UrP9BsoPZivRw3uJG8iDhRRd",
        "5rEeYv8R25b8j6YTHJvYuCKEzq44UCw1Wx1Wx2VPPLz1",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    for (request, individual_test_name) in [
        (
            r#"
        {
            "ids": ["HTKAVZZrDdyecCxzm3WEkCsG1GUmiqKm73PvngfuYRNK", "2NqdYX6kJmMUoChnDXU2UrP9BsoPZivRw3uJG8iDhRRd"]
        }
        "#,
            "only-2",
        ),
        (
            r#"
        {
            "ids": ["2NqdYX6kJmMUoChnDXU2UrP9BsoPZivRw3uJG8iDhRRd", "5rEeYv8R25b8j6YTHJvYuCKEzq44UCw1Wx1Wx2VPPLz1"]
        }
        "#,
            "only-2-different-2",
        ),
        (
            r#"
        {
            "ids": [
                "2NqdYX6kJmMUoChnDXU2UrP9BsoPZivRw3uJG8iDhRRd",
                "JECLQnbo2CCL8Ygn6vTFn7yeKn8qc7i51bAa9BCAJnWG",
                "5rEeYv8R25b8j6YTHJvYuCKEzq44UCw1Wx1Wx2VPPLz1"
            ]
        }
        "#,
            "2-and-a-missing-1",
        ),
    ] {
        let request: GetAssetBatch = serde_json::from_str(request).unwrap();
        let response = setup.das_api.get_asset_batch(request).await.unwrap();
        insta::assert_json_snapshot!(format!("{}-{}", name, individual_test_name), response);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_reg_get_asset_by_group() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: None,
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "7jFuJ73mBPDdLMvCYxzrpFTD9FeDudRxdXGDALP5Cp2W",
        "BioVudBTjJnuDW22q62XPhGP87sVwZKcQ46MPSNz4gqi",
        "Fm9S3FL23z3ii3EBBv8ozqLninLvhWDYmcHcHaZy6nie",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "groupKey": "collection",
        "groupValue": "8Rt3Ayqth4DAiPnW9MDFi63TiQJHmohfTWLMQFHi4KZH",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "page": 1,
        "limit": 1
    }
    "#;

    let request: GetAssetsByGroup = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets_by_group(request).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_reg_search_assets() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: None,
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "2PfAwPb2hdgsf7xCKyU2kAWUGKnkxYZLfg5SMf4YP1h2",
        "Dt3XDSAdXAJbHqvuycgCTHykKCC7tntMFGMmSvfBbpTL",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "ownerAddress": "6Cr66AabRYymhZgYQSfTCo6FVpH18wXrMZswAbcErpyX",
        "page": 1,
        "limit": 2
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_regular_nft_collection() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w"
    }
    "#;

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();
    insta::assert_json_snapshot!(name.clone(), response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_search_by_owner_with_show_zero_balance() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: well_known_fungible_tokens(),
        },
    )
    .await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",  // Fungible token MPLX
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "3yMfqHsajYFw2Yw6C4kwrvHRESMg9U7isNVJuzNETJKG", // Super Sweet NFT Collection
        "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj", // Super Degen Ape Collection
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "3rzjtWZcZyvADaT5rrkRwGKWjnuzvK3PDedGMUwpnrrP", // empty token acc from NFT ( Super Sweet NFT Collection )
        "3oiDhCQMDzxj8NcLfRBCQj3R9mQkE1DnDZfrNbAgruQk", // Existing NFT for current owner (Degen Ape Collection)
        "8Tf7Pj7UnF7KMcsQNUrr3MqYZjvPuRUrPeQRc8Dkr9pA", // MPLX Fungible token with non zero balance.
        "94eSnb5qBWTvxj3gqP6Ukq8bPhRTNNVZrE7zR5yTZd9E", // Hxro (Wormhole) Fungible token with zero balance.
        "sFxPHhiQWptvrc2YA2HyfNjCvqxpsEtmjmREBvWf7NJ",  // Super Sweet NFT with another owner.
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "all",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    // API shouldn't return zero NonFungible accounts ("3rzjtWZc"). "showZeroBalance": true is working only for Fungible tokens
    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 3);
    assert_eq!(
        res_obj.items[0].id, "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
        " Hxro (Wormhole) Fungible token account"
    );
    assert_eq!(
        res_obj.items[1].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );
    assert_eq!(
        res_obj.items[2].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_all", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "nonFungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_non_fungible", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "fungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 2);
    assert_eq!(
        res_obj.items[0].id, "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
        " Hxro (Wormhole) Fungible token account"
    );
    assert_eq!(
        res_obj.items[1].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_fungible", name), response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_search_by_owner_with_show_zero_balance_false() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: well_known_fungible_tokens(),
        },
    )
    .await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",  // Fungible token MPLX
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "3yMfqHsajYFw2Yw6C4kwrvHRESMg9U7isNVJuzNETJKG", // Super Sweet NFT Collection
        "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj", // Super Degen Ape Collection
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "3rzjtWZcZyvADaT5rrkRwGKWjnuzvK3PDedGMUwpnrrP", // empty token acc from NFT ( Super Sweet NFT Collection )
        "3oiDhCQMDzxj8NcLfRBCQj3R9mQkE1DnDZfrNbAgruQk", // Existing NFT for current owner (Degen Ape Collection)
        "8Tf7Pj7UnF7KMcsQNUrr3MqYZjvPuRUrPeQRc8Dkr9pA", // MPLX Fungible token with non zero balance.
        "94eSnb5qBWTvxj3gqP6Ukq8bPhRTNNVZrE7zR5yTZd9E", // Hxro (Wormhole) Fungible token with zero balance.
        "sFxPHhiQWptvrc2YA2HyfNjCvqxpsEtmjmREBvWf7NJ",  // Super Sweet NFT with another owner.
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "all",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 2);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );
    assert_eq!(
        res_obj.items[1].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_all", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "nonFungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_non_fungible", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "fungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_fungible", name), response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_search_by_owner_with_show_zero_balance_with_reverse_data_processing_sequence() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: well_known_fungible_tokens(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "3rzjtWZcZyvADaT5rrkRwGKWjnuzvK3PDedGMUwpnrrP", // empty token acc from NFT ( Super Sweet NFT Collection )
        "3oiDhCQMDzxj8NcLfRBCQj3R9mQkE1DnDZfrNbAgruQk", // Existing NFT for current owner (Degen Ape Collection)
        "8Tf7Pj7UnF7KMcsQNUrr3MqYZjvPuRUrPeQRc8Dkr9pA", // MPLX Fungible token with non zero balance.
        "94eSnb5qBWTvxj3gqP6Ukq8bPhRTNNVZrE7zR5yTZd9E", // Hxro (Wormhole) Fungible token with zero balance.
        "sFxPHhiQWptvrc2YA2HyfNjCvqxpsEtmjmREBvWf7NJ",  // Super Sweet NFT with another owner.
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",  // Fungible token MPLX
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "3yMfqHsajYFw2Yw6C4kwrvHRESMg9U7isNVJuzNETJKG", // Super Sweet NFT Collection
        "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj", // Super Degen Ape Collection
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "all",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    // API shouldn't return zero NonFungible accounts ("3rzjtWZc"). "showZeroBalance": true is working only for Fungible tokens
    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 3);
    assert_eq!(
        res_obj.items[0].id, "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
        " Hxro (Wormhole) Fungible token account"
    );
    assert_eq!(
        res_obj.items[1].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );
    assert_eq!(
        res_obj.items[2].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_all", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "nonFungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_non_fungible", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "fungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": true
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 2);
    assert_eq!(
        res_obj.items[0].id, "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
        " Hxro (Wormhole) Fungible token account"
    );
    assert_eq!(
        res_obj.items[1].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_fungible", name), response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_search_by_owner_with_show_zero_balance_false_with_reverse_data_processing_sequence() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "3rzjtWZcZyvADaT5rrkRwGKWjnuzvK3PDedGMUwpnrrP", // empty token acc from NFT ( Super Sweet NFT Collection )
        "3oiDhCQMDzxj8NcLfRBCQj3R9mQkE1DnDZfrNbAgruQk", // Existing NFT for current owner (Degen Ape Collection)
        "8Tf7Pj7UnF7KMcsQNUrr3MqYZjvPuRUrPeQRc8Dkr9pA", // MPLX Fungible token with non zero balance.
        "94eSnb5qBWTvxj3gqP6Ukq8bPhRTNNVZrE7zR5yTZd9E", // Hxro (Wormhole) Fungible token with zero balance.
        "sFxPHhiQWptvrc2YA2HyfNjCvqxpsEtmjmREBvWf7NJ",  // Super Sweet NFT with another owner.
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",  // Fungible token MPLX
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        "3yMfqHsajYFw2Yw6C4kwrvHRESMg9U7isNVJuzNETJKG", // Super Sweet NFT Collection
        "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj", // Super Degen Ape Collection
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "all",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 2);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );
    assert_eq!(
        res_obj.items[1].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_all", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "nonFungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "BFjgKzLNKZEbZoDrESi79ai8jXgyBth1HXCJPXBGs8sj",
        "Degen Ape NFT account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_non_fungible", name), response);

    let request = r#"
    {
        "page": 1,
        "limit": 500,
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "tokenType": "fungible",
        "options": {
            "showNativeBalance": true, "showZeroBalance": false
        }
    }
    "#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();
    let res_obj: AssetList = serde_json::from_value(response.clone()).unwrap();

    assert_eq!(res_obj.items.is_empty(), false);
    assert_eq!(res_obj.items.len(), 1);
    assert_eq!(
        res_obj.items[0].id, "METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m",
        "MPLX Fungible token account"
    );

    insta::assert_json_snapshot!(format!("{}_token_type_fungible", name), response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_search_assets_by_owner_with_pages() {
    let test_name = trim_test_name(function_name!());

    let setup = TestSetup::new_with_options(
        test_name.clone(),
        TestSetupOptions {
            network: Some(Network::EclipseMainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds = seed_token_mints([
        "DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ",
        "9qA21TR9QTsQeR5sP6L2PytjgxXcVRSyqUY5vRcUogom",
        "8WKGo1z9k3PjTsQw5GDQmvAbKwuRGtb4APkCneH8AVY1",
        "7ZkXycbrAhVzeB9ngnjcCdjk5bxTJYzscSZMhRRBx3QB",
        "75peBtH5MwfA5t9uhr51AYL7MR5DbPJ5xQ7wizzvowUH",
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "4pRQs1xZdASeL65PHTa1C8GnYCWtX18Lx98ofJB3SZNC",
        "5ok1Zv557DAnichMsWE4cfURYbr1D2yWfcaqehydHo9R",
        "JCnRA9ALhDYC5SWhBrw19JVWnDxnrGMYTmkfLsLkbpzV",
        "2TQDwULQDdpisGssKZeRw2qcCTiZnsAmi6cnR89YYxSg",
        "44vjE7bDpwA2nFp5KbjWHjG2RHBWi5z1pP5ehY9t6p8V",
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
        {
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "all",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    let name_all_assets = format!("{}_all_assets", test_name);

    insta::assert_json_snapshot!(name_all_assets, response);

    let request = r#"
        {
            "page": 1,
            "limit": 2,
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "all",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    let name_page_1 = format!("{}_page_1", test_name);

    insta::assert_json_snapshot!(name_page_1, response);

    let request = r#"
        {
            "page": 2,
            "limit": 2,
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "all",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    let name_page_2 = format!("{}_page_2", test_name);

    insta::assert_json_snapshot!(name_page_2, response);

    let request = r#"
        {
            "page": 3,
            "limit": 2,
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "all",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    let name_page_3 = format!("{}_page_3", test_name);

    insta::assert_json_snapshot!(name_page_3, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn get_asset_nft_token_22_with_metadata() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["Cpy4TfoLi1qtcx1grKx373NVksQ2xA3hMyNQvT2HFfQn"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "Cpy4TfoLi1qtcx1grKx373NVksQ2xA3hMyNQvT2HFfQn"
    }
    "#;

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_requested_non_fungibles_are_non_fungibles() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::EclipseMainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds = seed_token_mints([
        "DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ",
        "9qA21TR9QTsQeR5sP6L2PytjgxXcVRSyqUY5vRcUogom",
        "8WKGo1z9k3PjTsQw5GDQmvAbKwuRGtb4APkCneH8AVY1",
        "7ZkXycbrAhVzeB9ngnjcCdjk5bxTJYzscSZMhRRBx3QB",
        "75peBtH5MwfA5t9uhr51AYL7MR5DbPJ5xQ7wizzvowUH",
        "87K3PtGNihT6dKjxULK25MVapZKXQWN4zXqC1BEshHKd",
        "LaihKXA47apnS599tyEyasY2REfEzBNe4heunANhsMx", // Fungible
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "4pRQs1xZdASeL65PHTa1C8GnYCWtX18Lx98ofJB3SZNC",
        "5ok1Zv557DAnichMsWE4cfURYbr1D2yWfcaqehydHo9R",
        "JCnRA9ALhDYC5SWhBrw19JVWnDxnrGMYTmkfLsLkbpzV",
        "2TQDwULQDdpisGssKZeRw2qcCTiZnsAmi6cnR89YYxSg",
        "44vjE7bDpwA2nFp5KbjWHjG2RHBWi5z1pP5ehY9t6p8V",
        "CJL5wC5ouAhnQ7jkCPkfKSyjHJQAHNWPJKDHB5VojSug",
        "Ar5YKeZgzEG1RxosWJuS1BWVX7odSdkS6CBVpwqef7fo",
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
        {
            "limit": 500,
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "nonFungible",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    assert_eq!(response["items"].as_array().unwrap().len(), 5);

    response["items"].as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "V1_NFT");
        true
    });

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_requested_fungibles_are_fungibles() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::EclipseMainnet),
            clear_db: true,
            well_known_fungible_accounts: well_known_fungible_tokens(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["7qfEt4otpcr1LHPVZ2hjCB1d77wSZJfSDgwiXcUCneaT"]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds = seed_token_mints([
        "DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ",
        "9qA21TR9QTsQeR5sP6L2PytjgxXcVRSyqUY5vRcUogom",
        "8WKGo1z9k3PjTsQw5GDQmvAbKwuRGtb4APkCneH8AVY1",
        "7ZkXycbrAhVzeB9ngnjcCdjk5bxTJYzscSZMhRRBx3QB",
        "75peBtH5MwfA5t9uhr51AYL7MR5DbPJ5xQ7wizzvowUH",
        "87K3PtGNihT6dKjxULK25MVapZKXQWN4zXqC1BEshHKd",
        "LaihKXA47apnS599tyEyasY2REfEzBNe4heunANhsMx", // Fungible
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
        {
            "limit": 500,
            "ownerAddress": "EcxjN4mea6Ah9WSqZhLtSJJCZcxY73Vaz6UVHFZZ5Ttz",
            "tokenType": "fungible",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request).await.unwrap();

    assert_eq!(response["items"].as_array().unwrap().len(), 1);

    response["items"].as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "FungibleToken");
        true
    });

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_recognise_popular_fungible_tokens() {
    let name = trim_test_name(function_name!());

    let token_price_fetcher =
        RaydiumTokenPriceFetcher::new(RAYDIUM_API_HOST.to_string(), CACHE_TTL, None);
    token_price_fetcher.warmup().await.unwrap();
    let well_known_fungible_accounts = token_price_fetcher.get_all_token_symbols().await.unwrap();
    assert!(well_known_fungible_accounts.len() > 0);

    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts,
        },
    )
    .await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // Fungible token USDC
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
         {
            "ids": [
                "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            ]
        }"#;

    let request = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset_batch(request).await.unwrap();

    assert_eq!(response.as_array().unwrap().len(), 2);
    response.as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "FungibleToken");
        true
    });

    insta::assert_json_snapshot!(name, response);
}
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_update_well_known_fungible_tokens() {
    let name = trim_test_name(function_name!());

    let token_price_fetcher =
        RaydiumTokenPriceFetcher::new(RAYDIUM_API_HOST.to_string(), CACHE_TTL, None);
    token_price_fetcher.warmup().await.unwrap();
    let well_known_fungible_accounts = token_price_fetcher.get_all_token_symbols().await.unwrap();
    assert!(well_known_fungible_accounts.len() > 0);

    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // Fungible token USDC
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request_str = r#"
         {
            "ids": [
                "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            ]
        }"#;

    let request = serde_json::from_str(request_str).unwrap();
    let response = setup.das_api.get_asset_batch(request).await.unwrap();

    assert_eq!(response.as_array().unwrap().len(), 2);
    response.as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "Custom");
        true
    });

    let _ = update_fungible_token_static_details(
        &setup.rocks_db,
        well_known_fungible_accounts.keys().cloned().collect(),
    );

    setup.synchronizer.full_syncronize(CancellationToken::new(), Fungible).await.unwrap();
    setup.synchronizer.full_syncronize(CancellationToken::new(), NonFungible).await.unwrap();

    let request = serde_json::from_str(request_str).unwrap();
    let response = setup.das_api.get_asset_batch(request).await.unwrap();

    assert_eq!(response.as_array().unwrap().len(), 2);
    response.as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "FungibleToken");
        true
    });

    insta::assert_json_snapshot!(name, response);
}

#[named]
#[serial]
#[tokio::test(flavor = "multi_thread")]
#[tracing_test::traced_test]
async fn test_update_fungible_token_static_details_job() {
    let name = trim_test_name(function_name!());

    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    // Add Asset Hxro (Wormhole)
    let seeds: Vec<SeedEvent> = seed_token_mints([
        "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK", // Fungible token Hxro (Wormhole)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // Fungible token USDC
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request_str = r#"
         {
            "ids": [
                "HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            ]
        }"#;

    let request = serde_json::from_str(request_str).unwrap();
    let response = setup.das_api.get_asset_batch(request).await.unwrap();

    assert_eq!(response.as_array().unwrap().len(), 2);
    response.as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "Custom");
        true
    });

    let well_known_fungible_pks = vec![
        String::from("HxhWkVpk5NS4Ltg5nij2G671CKXFRKPK8vy271Ub4uEK"),
        String::from("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
    ];
    let sut = Scheduler::new(setup.rocks_db.clone(), Some(well_known_fungible_pks));
    Scheduler::run_in_background(sut, CancellationToken::new()).await;

    setup.synchronizer.full_syncronize(CancellationToken::new(), Fungible).await.unwrap();
    setup.synchronizer.full_syncronize(CancellationToken::new(), NonFungible).await.unwrap();

    let request_2 = serde_json::from_str(request_str).unwrap();
    let response_2 = setup.das_api.get_asset_batch(request_2).await.unwrap();

    assert_eq!(response_2.as_array().unwrap().len(), 2);
    response_2.as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "FungibleToken");
        true
    });

    insta::assert_json_snapshot!(name, response);
}

#[named]
#[serial]
#[tokio::test(flavor = "multi_thread")]
#[tracing_test::traced_test]
async fn test_get_master_editions() {
    let name = trim_test_name(function_name!());
    let first_mint_master_edition =
        Pubkey::from_str("Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL").unwrap();
    let second_mint_edition =
        Pubkey::from_str("GJvFDcBWf6aDncd1TBzx2ou1rgLFYaMBdbYLBa9oTAEw").unwrap();
    let third_mint_edition =
        Pubkey::from_str("9yQecKKYSHxez7fFjJkUvkz42TLmkoXzhyZxEf2pw8pz").unwrap();
    let fourth_mint_edition =
        Pubkey::from_str("7AeRUkukNCpWFtxK2QBZr1PymzPde6qtQYND6CajrE2B").unwrap();
    let supply = 123;
    let max_supply = 10000;

    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
            well_known_fungible_accounts: HashMap::new(),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts([
        first_mint_master_edition.to_string(), // Master Edition
        second_mint_edition.to_string(),
        third_mint_edition.to_string(),
        fourth_mint_edition.to_string(),
    ]);
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let first_mint_master_edition_to_save = EditionMetadata {
        edition: TokenMetadataEdition::MasterEdition {
            0: MasterEdition {
                key: first_mint_master_edition,
                supply,
                max_supply: Some(max_supply),
                write_version: 1,
            },
        },
        write_version: 1,
        slot_updated: 1,
    };
    let second_mint_edition_to_save =
        create_edition_metadata(second_mint_edition, first_mint_master_edition, 1);
    let third_mint_edition_to_save =
        create_edition_metadata(third_mint_edition, first_mint_master_edition, 2);
    let fourth_mint_edition_to_save =
        create_edition_metadata(fourth_mint_edition, first_mint_master_edition, 3);

    let mut batch_storage =
        BatchSaveStorage::new(setup.rocks_db, 10, Arc::new(IngesterMetricsConfig::new()));
    let mplx_accs_parser = MplxAccountsProcessor::new(Arc::new(IngesterMetricsConfig::new()));

    mplx_accs_parser
        .transform_and_store_edition_account(
            &mut batch_storage,
            first_mint_master_edition,
            &first_mint_master_edition_to_save.edition,
        )
        .unwrap();
    mplx_accs_parser
        .transform_and_store_edition_account(
            &mut batch_storage,
            second_mint_edition,
            &second_mint_edition_to_save.edition,
        )
        .unwrap();
    mplx_accs_parser
        .transform_and_store_edition_account(
            &mut batch_storage,
            third_mint_edition,
            &third_mint_edition_to_save.edition,
        )
        .unwrap();
    mplx_accs_parser
        .transform_and_store_edition_account(
            &mut batch_storage,
            fourth_mint_edition,
            &fourth_mint_edition_to_save.edition,
        )
        .unwrap();

    batch_storage.flush().unwrap();

    let request = r#"
        {
            "page": 1,
            "limit": 100,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["page"], 1);
    assert_eq!(response["total"], 3);
    assert_eq!(response["limit"], 100);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 3);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 1);
    assert_eq!(response["editions"].as_array().unwrap()[1]["edition"], 2);
    assert_eq!(response["editions"].as_array().unwrap()[2]["edition"], 3);
    insta::assert_json_snapshot!(format!("{}_all_items_in_sorted_order", name), response);

    let request = r#"
        {
            "page": 1,
            "limit": 2,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["page"], 1);
    assert_eq!(response["total"], 2);
    assert_eq!(response["limit"], 2);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 2);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 1);
    assert_eq!(response["editions"].as_array().unwrap()[1]["edition"], 2);
    insta::assert_json_snapshot!(format!("{}_few_items_in_sorted_order", name), response);

    let request = r#"
        {
            "page": 1,
            "limit": 1,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["page"], 1);
    assert_eq!(response["total"], 1);
    assert_eq!(response["limit"], 1);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 1);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 1);
    insta::assert_json_snapshot!(format!("{}_first_page", name), response);

    let request = r#"
        {
            "page": 2,
            "limit": 1,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["page"], 2);
    assert_eq!(response["total"], 1);
    assert_eq!(response["limit"], 1);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 1);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 2);
    insta::assert_json_snapshot!(format!("{}_second_page", name), response);

    let request = r#"
        {
            "page": 50,
            "limit": 1,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["page"], 50);
    assert_eq!(response["total"], 0);
    assert_eq!(response["limit"], 1);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 0);
    insta::assert_json_snapshot!(format!("{}_page_out_of_range", name), response);

    let request = r#"
        {
            "after": "1",
            "limit": 100,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["after"], "1");
    assert_eq!(response["total"], 2);
    assert_eq!(response["limit"], 100);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 2);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 2);
    assert_eq!(response["editions"].as_array().unwrap()[1]["edition"], 3);
    insta::assert_json_snapshot!(format!("{}_page_after_first", name), response);

    let request = r#"
        {
            "before": "3",
            "limit": 100,
            "mint": "Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL"
        }"#;
    let request: GetNftEditions = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_nft_editions(request).await.unwrap();

    assert_eq!(response["before"], "3");
    assert_eq!(response["total"], 2);
    assert_eq!(response["limit"], 100);
    assert_eq!(response["supply"], supply);
    assert_eq!(response["max_supply"], max_supply);
    assert_eq!(response["editions"].as_array().unwrap().len(), 2);
    assert_eq!(response["editions"].as_array().unwrap()[0]["edition"], 1);
    assert_eq!(response["editions"].as_array().unwrap()[1]["edition"], 2);
    insta::assert_json_snapshot!(format!("{}_page_before_third", name), response);
}

fn create_edition_metadata(key: Pubkey, parent: Pubkey, edition: u64) -> EditionMetadata {
    EditionMetadata {
        edition: TokenMetadataEdition::EditionV1 {
            0: EditionV1 { key, parent, edition, write_version: 1 },
        },
        write_version: 1,
        slot_updated: 1,
    }
}
