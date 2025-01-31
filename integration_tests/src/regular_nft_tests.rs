use std::sync::Arc;

use entities::{
    api_req_params::{GetAsset, GetAssetBatch, GetAssetsByGroup, SearchAssets},
    enums::AssetType,
};
use function_name::named;
use itertools::Itertools;
use nft_ingester::api::dapi::response::AssetList;
use rocks_db::storage_traits::AssetIndexReader;
use serial_test::serial;
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};

use super::common::*;

#[tokio::test]
#[serial]
#[named]
async fn test_reg_get_asset() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: None, clear_db: true },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["CMVuYDS9nTeujfTPJb8ik7CRhAqZv4DfjfdamFLkJgxE"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "CMVuYDS9nTeujfTPJb8ik7CRhAqZv4DfjfdamFLkJgxE"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_reg_get_asset_batch() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: None, clear_db: true },
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
        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

        let request: GetAssetBatch = serde_json::from_str(request).unwrap();
        let response = setup.das_api.get_asset_batch(request, mutexed_tasks.clone()).await.unwrap();
        insta::assert_json_snapshot!(format!("{}-{}", name, individual_test_name), response);
    }
}

#[tokio::test]
#[serial]
#[named]
async fn test_reg_get_asset_by_group() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: None, clear_db: true },
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

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAssetsByGroup = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets_by_group(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_reg_search_assets() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: None, clear_db: true },
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

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_regular_nft_collection() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Mainnet), clear_db: true },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name.clone(), response);
}

#[tokio::test]
#[serial]
#[named]
async fn get_asset_nft_token_22_with_metadata() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet), clear_db: true },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_nfts(["Cpy4TfoLi1qtcx1grKx373NVksQ2xA3hMyNQvT2HFfQn"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "Cpy4TfoLi1qtcx1grKx373NVksQ2xA3hMyNQvT2HFfQn"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request, mutexed_tasks.clone()).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_requested_non_fungibles_are_non_fungibles() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::EclipseMainnet), clear_db: true },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "7qfEt4otpcr1LHPVZ2hjCB1d77wSZJfSDgwiXcUCneaT",
        "9cyPkra7ANoDmMM4Abw8rYKxv3aWC3jZUjz8NtWaYo6D",
        "JCnRA9ALhDYC5SWhBrw19JVWnDxnrGMYTmkfLsLkbpzV",
        "44vjE7bDpwA2nFp5KbjWHjG2RHBWi5z1pP5ehY9t6p8V",
        "2TQDwULQDdpisGssKZeRw2qcCTiZnsAmi6cnR89YYxSg",
        "4pRQs1xZdASeL65PHTa1C8GnYCWtX18Lx98ofJB3SZNC",
        "5ok1Zv557DAnichMsWE4cfURYbr1D2yWfcaqehydHo9R",
    ]);
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
            "tokenType": "nonFungible",
            "options": {
                "showCollectionMetadata": true,
                "showGrandTotal": true,
                "showInscription": true,
                "showNativeBalance": true
            }
        }"#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request, mutexed_tasks.clone()).await.unwrap();

    assert_eq!(response["items"].as_array().unwrap().len(), 5);

    response["items"].as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "V1_NFT");
        true
    });

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_requested_fungibles_are_fungibles() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::EclipseMainnet), clear_db: true },
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

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: SearchAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.search_assets(request, mutexed_tasks.clone()).await.unwrap();

    assert_eq!(response["items"].as_array().unwrap().len(), 1);

    response["items"].as_array().unwrap().iter().all(|i| {
        let interface = i["interface"].as_str().unwrap();
        assert_eq!(interface, "FungibleToken");
        true
    });

    insta::assert_json_snapshot!(name, response);
}
