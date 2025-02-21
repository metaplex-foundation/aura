use std::{collections::HashMap, sync::Arc};

use entities::{
    api_req_params::{GetAsset, GetAssetsByOwner, SearchAssets},
    enums::Interface,
};
use function_name::named;
use itertools::Itertools;
use nft_ingester::api::dapi::response::AssetList;
use serial_test::serial;
use tokio::{sync::Mutex, task::JoinSet};

use crate::common::{
    index_seed_events, seed_accounts, seed_nfts, trim_test_name, Network, SeedEvent, TestSetup,
    TestSetupOptions,
};

#[tokio::test]
#[serial]
#[named]
async fn test_asset_parsing() {
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

    let seeds: Vec<SeedEvent> = seed_nfts(["843gdpsTE4DoJz3ZoBsEjAqT8UgAcyF5YojygGgGZE1f"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "843gdpsTE4DoJz3ZoBsEjAqT8UgAcyF5YojygGgGZE1f"
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
async fn test_get_different_assets_by_owner() {
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

    let seeds: Vec<SeedEvent> = seed_nfts([
        "91tabY8dzm3HfSgDujeyEfT6p94sV39iW2T8R9u5CMMo", // NFT without collection
        "9prAPyPdbd75U5uLvACjPpyfSp7EbHPwPdtxZihYetjh", // NFT with unverified collection
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "J9skkg9hzZFzNkQ55sQy6R4uozqfkQ8MRMmnCV7hgq5q", // Core collection
        "CRqexZSPcuiYGJuTM68tC48bhC4ZSRK9Gk178wcopo42", // Core asset
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
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
    insta::assert_json_snapshot!(name.clone(), response);

    let request = r#"
    {
        "ownerAddress": "3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM",
        "sortBy": {
            "sortBy": "updated",
            "sortDirection": "asc"
        },
        "options": {
            "showUnverifiedCollections": true
        },
        "page": 1,
        "limit": 50
    }
    "#;

    let request: GetAssetsByOwner = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets_by_owner(request, mutexed_tasks.clone()).await.unwrap();
    insta::assert_json_snapshot!(format!("{}_show_unverif_coll", name), response);
}

// context: this account had an issue with mpl-core < 0.9.0, where
// it could not be parsed because of a bug in mpl-core.
#[tokio::test]
#[named]
async fn test_process_previously_unparseable_account() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions { network: Some(Network::Devnet), clear_db: true, well_known_fungible_accounts: HashMap::new() },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["FfYZYTVhCEK3YmZm1is59ove86eiXWU2yizpqRLVGSN"]);

    // it is sufficient to make sure the test does not panic here
    // and therefore the nft is successfully processed.
    index_seed_events(&setup, seeds.iter().collect_vec()).await;
}
