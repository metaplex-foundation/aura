use crate::common::index_seed_events;
use crate::common::seed_nfts;
use crate::common::trim_test_name;
use crate::common::SeedEvent;
use crate::common::TestSetup;
use crate::common::TestSetupOptions;
use entities::api_req_params::GetAsset;
use function_name::named;
use itertools::Itertools;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

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
    let response = setup
        .das_api
        .get_asset(request, mutexed_tasks.clone())
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, response);
}
