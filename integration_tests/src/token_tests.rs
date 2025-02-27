use std::collections::HashMap;

use entities::api_req_params::GetAsset;
use function_name::named;
use itertools::Itertools;
use nft_ingester::api::dapi::rpc_asset_models::Asset;
use serial_test::serial;

use crate::common::{
    index_seed_events, seed_token_mints, trim_test_name, well_known_fungible_tokens, Network,
    SeedEvent, TestSetup, TestSetupOptions,
};

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_fungible_token_mint_freeze_authority() {
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

    // USDC token
    let seeds: Vec<SeedEvent> = seed_token_mints(&["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]);

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
        "id": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    }
    "#;

    let request: GetAsset = serde_json::from_str(request).unwrap();
    let response_value = setup.das_api.get_asset(request).await.unwrap();
    let asset: Asset =
        serde_json::from_value::<Asset>(response_value.clone()).expect("Cannot parse 'Asset'.");

    assert_eq!(asset.clone().id, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string());
    assert_eq!(
        asset.clone().token_info.unwrap().mint_authority.unwrap(),
        "BJE5MMbqXjVwjAF7oxwPYXnTXDyspzZyt4vwenNw5ruG".to_string()
    );
    assert_eq!(
        asset.clone().token_info.unwrap().freeze_authority.unwrap(),
        "7dGbd2QZcCKcTndnHcTL8q7SMVXAkp688NTQYwrRCrar".to_string()
    );

    insta::assert_json_snapshot!(name, response_value.clone());
}
