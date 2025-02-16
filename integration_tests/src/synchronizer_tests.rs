use std::collections::HashMap;
use std::sync::Arc;

use entities::{
    api_req_params::{
        GetAsset, GetAssetBatch, GetAssetsByAuthority, GetAssetsByGroup, GetAssetsByOwner,
        SearchAssets,
    },
    enums::AssetType,
};
use function_name::named;
use itertools::Itertools;
use serial_test::serial;
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};

use super::common::*;

#[tokio::test]
#[serial]
#[named]
async fn test_full_sync_core_get_assets_by_authority() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet),  clear_db: true , wellknown_fungible_accounts: HashMap::new() },
    )
    .await;

    // one is Core asset, one is Core collection
    // both have same authority
    let seeds: Vec<SeedEvent> = seed_accounts([
        "9CSyGBw1DCVZfx621nb7UBM9SpVDsX1m9MaN6APCf1Ci",
        "4FcFVJVPRsYoMjt8ewDGV5nipoK63SNrJzjrBHyXvhcz",
    ]);

    single_db_index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    setup
        .synchronizer
        .full_syncronize(&shutdown_rx.resubscribe(), AssetType::NonFungible)
        .await
        .unwrap();

    let request = r#"
    {
        "authorityAddress": "APrZTeVysBJqAznfLXS71NAzjr2fCVTSF1A66MeErzM7",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "page": 1,
        "limit": 50
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAssetsByAuthority = serde_json::from_str(request).unwrap();
    let response =
        setup.das_api.get_assets_by_authority(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_full_sync_core_get_assets_by_group() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet),  clear_db: true , wellknown_fungible_accounts: HashMap::new() },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "JChzyyp1CnNz56tJLteQ5BsbngmWQ3JwcxLZrmuQA5b7",
        "kTMCCKLTaZsnSReer12HsciwScUwhHyZyd9D9BwQF8k",
        "EgzsppfYJmUet4ve8MnuHMyvSnj6R7LRmwsGEH5TuGhB",
        "J2kazVRuZ33Po4PVyZGxiDYUMQ1eZiT5Xa13usRYo264",
    ]);

    single_db_index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    setup
        .synchronizer
        .full_syncronize(&shutdown_rx.resubscribe(), AssetType::NonFungible)
        .await
        .unwrap();

    let request = r#"
    {
        "groupKey": "collection",
        "groupValue": "JChzyyp1CnNz56tJLteQ5BsbngmWQ3JwcxLZrmuQA5b7",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "page": 1,
        "limit": 50
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
async fn test_full_sync_core_get_assets_by_owner() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet),  clear_db: true , wellknown_fungible_accounts: HashMap::new() },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "4FFhh184GNqh3LEK8UhMY7KBuCdNvvhU7C23ZKrKnofb",
        "9tsHoBrkSqBW5uMxKZyvxL6m9CCaz1a7sGEg8SuckUj",
    ]);

    single_db_index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    setup
        .synchronizer
        .full_syncronize(&shutdown_rx.resubscribe(), AssetType::NonFungible)
        .await
        .unwrap();

    let request = r#"
    {
        "ownerAddress": "7uScVQiT4vArB88dHrZoeVKWbtsRJmNp9r5Gce5VQpXS",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "page": 1,
        "limit": 50
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAssetsByOwner = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets_by_owner(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_full_sync_core_and_regular_nfts_get_assets_by_owner() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet),  clear_db: true , wellknown_fungible_accounts: HashMap::new() },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "4FFhh184GNqh3LEK8UhMY7KBuCdNvvhU7C23ZKrKnofb",
        "9tsHoBrkSqBW5uMxKZyvxL6m9CCaz1a7sGEg8SuckUj",
        // below are account related to regular NFT
        "8qbRNh9Q9pcksZVnmQemoh7is2NqsRNTx4jmpv75knC6", // mint account
        "CoeHPhsozRMmvJTg2uaNrAmQmjVvLk6PJvoEWDJavuBd", // token account
        "DHFGrBUK1Ctgr8RBftsWH952hS69hzesBpnyThWC6MjR", // metadata account
        "HEsxPaf6QFNBaN3LiVQAke99WaFMhT8JC2bWityF7mwZ", // master edition account
    ]);

    single_db_index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    setup
        .synchronizer
        .full_syncronize(&shutdown_rx.resubscribe(), AssetType::NonFungible)
        .await
        .unwrap();

    let request = r#"
    {
        "ownerAddress": "7uScVQiT4vArB88dHrZoeVKWbtsRJmNp9r5Gce5VQpXS",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "page": 1,
        "limit": 50
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAssetsByOwner = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets_by_owner(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(name, response);
}
