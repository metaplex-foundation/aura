use function_name::named;

use entities::api_req_params::{
    GetAsset, GetAssetsByAuthority, GetAssetsByGroup, GetAssetsByOwner,
};

use itertools::Itertools;

use serial_test::serial;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use super::common::*;

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["x3hJtpU4AUsGejNvxzX9TKjcyNB1eYtDdDPWdeF6opr"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "x3hJtpU4AUsGejNvxzX9TKjcyNB1eYtDdDPWdeF6opr"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_collection() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["DHciVfQxHHM7t2asQJRjjkKbjvZ4PuG3Y3uiULMQUjJQ"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "DHciVfQxHHM7t2asQJRjjkKbjvZ4PuG3Y3uiULMQUjJQ"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[ignore = "TODO: must recheck snapshot incompatibility"]
#[serial]
#[named]
async fn test_mpl_core_get_assets_by_authority() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    // one is Core asset, one is Core collection
    // both have same authority
    let seeds: Vec<SeedEvent> = seed_accounts([
        "9CSyGBw1DCVZfx621nb7UBM9SpVDsX1m9MaN6APCf1Ci",
        "4FcFVJVPRsYoMjt8ewDGV5nipoK63SNrJzjrBHyXvhcz",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

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
    let response = setup
        .das_api
        .get_assets_by_authority(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_assets_by_group() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "JChzyyp1CnNz56tJLteQ5BsbngmWQ3JwcxLZrmuQA5b7",
        "kTMCCKLTaZsnSReer12HsciwScUwhHyZyd9D9BwQF8k",
        "EgzsppfYJmUet4ve8MnuHMyvSnj6R7LRmwsGEH5TuGhB",
        "J2kazVRuZ33Po4PVyZGxiDYUMQ1eZiT5Xa13usRYo264",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

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
    let response = setup
        .das_api
        .get_assets_by_group(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_assets_by_owner() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "4FFhh184GNqh3LEK8UhMY7KBuCdNvvhU7C23ZKrKnofb",
        "9tsHoBrkSqBW5uMxKZyvxL6m9CCaz1a7sGEg8SuckUj",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

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
    let response = setup
        .das_api
        .get_assets_by_owner(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_edition() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["AejY8LGKAbQsrGZS1qgN4uFu99dJD3f8Js9Yrt7K3tCc"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "AejY8LGKAbQsrGZS1qgN4uFu99dJD3f8Js9Yrt7K3tCc"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_pubkey_in_rule_set() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["8H71x9Bhh9E9o3MZK4QnVC5MRFn1WZRf2Mc9w2wEbG5V"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "8H71x9Bhh9E9o3MZK4QnVC5MRFn1WZRf2Mc9w2wEbG5V"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_two_oracle_external_plugins() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["4aarnaiMVtGEp5nToRqBEUGtqY2F1gW2V8bBQe1rN5V9"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "4aarnaiMVtGEp5nToRqBEUGtqY2F1gW2V8bBQe1rN5V9"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_oracle_external_plugin_on_collection() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["Hvdg2FjMEndC4jxF2MJgKCaj5omLLZ19LNfD4p9oXkpE"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "Hvdg2FjMEndC4jxF2MJgKCaj5omLLZ19LNfD4p9oXkpE"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_oracle_multiple_lifecycle_events() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["3puHPHUHFXxhS7qPQa5YYTngzPbetoWbu7y2UxxB6xrF"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "3puHPHUHFXxhS7qPQa5YYTngzPbetoWbu7y2UxxB6xrF"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_oracle_custom_offset_and_base_address_config() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["9v2H5sDBXKmYkGHebfaWwdgBWuMTBVWQom3QeEcV8oJj"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "9v2H5sDBXKmYkGHebfaWwdgBWuMTBVWQom3QeEcV8oJj"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_oracle_no_offset() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["2TZpUiBiyMdwLFTKRshVMHK8anQK2W8XXbfUfyxR8yvc"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "2TZpUiBiyMdwLFTKRshVMHK8anQK2W8XXbfUfyxR8yvc"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_assets_by_group_with_oracle_and_custom_pda_all_seeds() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "Do7rVGmVNa9wjsKNyjoa5phqriLER6HCqUQm5zyoTX3f",
        "CWJDcrzxSDE7FeNRzMK1aSia7qoaUPrrGQ81E7vkQpq4",
    ]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "groupKey": "collection",
        "groupValue": "Do7rVGmVNa9wjsKNyjoa5phqriLER6HCqUQm5zyoTX3f",
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
    let response = setup
        .das_api
        .get_assets_by_group(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_multiple_internal_and_external_plugins() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["Aw7KSaeRECbjLW7BYTUtMwGkaiAGhxrQxdLnpLYRnmbB"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "Aw7KSaeRECbjLW7BYTUtMwGkaiAGhxrQxdLnpLYRnmbB"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_autograph_plugin() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["Hz4MSHgevYkpwF3cerDLuPJLQE3GZ5yDWu7vqmQGpRMU"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "Hz4MSHgevYkpwF3cerDLuPJLQE3GZ5yDWu7vqmQGpRMU"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_autograph_plugin_with_signature() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["4MCuZ5WNCgFnb7YJ2exj34qsLscmwd23WcoLBXBkaB7d"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "4MCuZ5WNCgFnb7YJ2exj34qsLscmwd23WcoLBXBkaB7d"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_verified_creators_plugin() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["AGyjcG9mBfYJFMZiJVkXr4iX7re6vkQ1Fw5grukA6Hiu"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "AGyjcG9mBfYJFMZiJVkXr4iX7re6vkQ1Fw5grukA6Hiu"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_verified_creators_plugin_with_signature() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["4iVX1oZj6nLAMerjXFw3UeGD4QU7BEaCscsWqD3zEH37"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "4iVX1oZj6nLAMerjXFw3UeGD4QU7BEaCscsWqD3zEH37"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_app_data_with_binary_data_and_owner_is_data_authority() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["6tqX4RuPCoD9dVKEJ51jykwBwjKh6runcHJSuSHpDPJU"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "6tqX4RuPCoD9dVKEJ51jykwBwjKh6runcHJSuSHpDPJU"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_app_data_with_json_data_and_update_authority_is_data_authority(
) {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["39XrhcVGuyq4HwTarxMCwDEMFtPBY5Nctxrvpvpdpe3g"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "39XrhcVGuyq4HwTarxMCwDEMFtPBY5Nctxrvpvpdpe3g"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_app_data_with_msg_pack_data_and_address_is_data_authority() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["2pY3t29uxpBotbmKbCsQNjYfML5DBoBshDgB7hpHu3XA"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "2pY3t29uxpBotbmKbCsQNjYfML5DBoBshDgB7hpHu3XA"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_collection_with_linked_app_data_with_binary_data_and_address_is_data_authority(
) {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["41thppJ4z9HnBNbFMLnztXS7seqBptYV1jG8UhxR4vK8"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "41thppJ4z9HnBNbFMLnztXS7seqBptYV1jG8UhxR4vK8"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_data_section_with_binary_data() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["BVjK8uvqUuH5YU6ThX6A7gznx2xi8BxshawbuFe1Y5Vr"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "BVjK8uvqUuH5YU6ThX6A7gznx2xi8BxshawbuFe1Y5Vr"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_collection_with_linked_app_data_with_json_data_and_owner_is_data_authority(
) {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["2aUn89GKuSjfYTeCH6GL1Y6CiUYqjvcgZehFGDJbhNeW"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "2aUn89GKuSjfYTeCH6GL1Y6CiUYqjvcgZehFGDJbhNeW"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_data_section_with_json_data() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["9vqNxe6M6t7PYo1gXrY18hVgDvCpouHSZ6vdDEFbybeA"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "9vqNxe6M6t7PYo1gXrY18hVgDvCpouHSZ6vdDEFbybeA"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_collection_with_linked_app_data_with_msg_pack_data_and_update_authority_is_data_authority(
) {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["53q1PCBy5KgzZfoHu6bnLWQFVmJtKyceP8DqNMhXWUaA"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "53q1PCBy5KgzZfoHu6bnLWQFVmJtKyceP8DqNMhXWUaA"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_mpl_core_get_asset_with_data_section_with_msg_pack_data() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
            clear_db: true,
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["EuXEcqHhF9jPxV9CKB5hjHC2TRo3xprdgk5vJTc9qRaY"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "EuXEcqHhF9jPxV9CKB5hjHC2TRo3xprdgk5vJTc9qRaY"
    }
    "#;

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}
