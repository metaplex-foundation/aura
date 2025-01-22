use std::sync::Arc;

use entities::api_req_params::{GetAsset, GetAssetBatch, GetAssetsByGroup, SearchAssets};
use function_name::named;
use itertools::Itertools;
use serial_test::serial;
use tokio::{sync::Mutex, task::JoinSet};

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
