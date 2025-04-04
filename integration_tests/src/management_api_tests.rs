use std::{collections::HashMap, str::FromStr};

use entities::api_req_params::GetAsset;
use function_name::named;
use nft_ingester::management_api::handlers::asset_fix::FixAccountAssetsRequest;
use serial_test::serial;
use solana_sdk::pubkey::Pubkey;
use tokio_util::sync::CancellationToken;

use super::common::*;

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_management_api_fix_account_assets() {
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
    let account = Pubkey::from_str("124YCZJGFhyiB9fVjmQdqxGrmpFMSxekNndWCxRTftbP").unwrap();
    // Start the management API
    let cancellation_token = CancellationToken::new();
    let metrics = setup.metrics.ingester_metrics.clone();
    let rocks_storage = setup.rocks_db.clone();
    let postgre_client = setup.db.clone();
    let cloned_rpc_client = setup.client.clone();
    let well_known_fungible_accounts = HashMap::new();
    let port = 3333;
    usecase::executor::spawn({
        let cancellation_token = cancellation_token.child_token();
        async move {
            nft_ingester::management_api::start_management_api(
                cancellation_token,
                metrics,
                rocks_storage,
                postgre_client,
                cloned_rpc_client,
                well_known_fungible_accounts,
                port,
            )
            .await
        }
    });
    // Wait for the management API to start
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Post the management API with the account
    let request = FixAccountAssetsRequest { asset_ids: vec![account.to_string()] };
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://localhost:{}/management/fix-account-assets", port))
        .json(&request)
        .send()
        .await;
    assert!(response.is_ok());
    let asset_response = setup
        .das_api
        .get_asset(GetAsset { id: account.to_string(), options: Default::default() })
        .await;
    assert!(asset_response.is_ok());
    let asset = asset_response.unwrap();
    let asset =
        serde_json::from_value::<nft_ingester::api::dapi::rpc_asset_models::Asset>(asset).unwrap();
    // assert the owner is present in the response
    assert_ne!(asset.ownership.owner, "".to_string());
    cancellation_token.cancel();
}
