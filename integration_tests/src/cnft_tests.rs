use std::{collections::HashMap, str::FromStr};

use entities::api_req_params::{GetAsset, Options, SearchAssets};
use function_name::named;
use itertools::Itertools;
use nft_ingester::api::error::DasApiError;
use serde_json::Value;
use serial_test::serial;
use solana_sdk::signature::Signature;

use super::common::*;

// TODO: Adjust this so that it can be run from anywhere.
// Do not move this test name or tests will break because the snapshot name and location will change.

pub async fn run_get_asset_scenario_test(
    setup: &TestSetup,
    asset_id: &str,
    seeds: Vec<SeedEvent>,
    order: Order,
    options: Options,
) -> Result<Value, DasApiError> {
    let seed_permutations: Vec<Vec<&SeedEvent>> = match order {
        Order::AllPermutations => seeds.iter().permutations(seeds.len()).collect::<Vec<_>>(),
        Order::Forward => vec![seeds.iter().collect_vec()],
    };
    for events in seed_permutations {
        index_seed_events(setup, events).await;
    }
    let request = GetAsset { id: asset_id.to_string(), options: options.clone() };

    setup.das_api.get_asset(request).await
}

pub async fn run_get_asset_scenario_test_with_assert(
    setup: &TestSetup,
    asset_id: &str,
    seeds: Vec<SeedEvent>,
    order: Order,
    options: Options,
) {
    let seed_permutations: Vec<Vec<&SeedEvent>> = match order {
        Order::AllPermutations => seeds.iter().permutations(seeds.len()).collect::<Vec<_>>(),
        Order::Forward => vec![seeds.iter().collect_vec()],
    };

    for events in seed_permutations {
        index_seed_events(setup, events).await;
        let request = GetAsset { id: asset_id.to_string(), options: options.clone() };

        let response = setup.das_api.get_asset(request).await.unwrap();
        insta::assert_json_snapshot!(setup.name.clone(), response);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[tracing_test::traced_test]
#[serial]
#[named]
async fn test_asset_decompress() {
    env_logger::init();

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
    let asset_id = "Az9QTysJj1LW1F7zkYF21HgBj3FRpq3zpxTFdPnAJYm8";

    // Mint a compressed NFT and then decompress it. In production, we would receive account updates for the newly minted NFT.
    // This test guarentees consistent results if we index the events in different orders.
    let seeds: Vec<SeedEvent> = vec![
        // mint cNFT
        seed_txn("55tQCoLUtHyu4i6Dny6SMdq4dVD61nuuLxXvRLeeQqE6xdm66Ajm4so39MXcJ2VaTmCNDEFBpitzLkiFaF7rNtHi"),
        // redeem
        seed_txn("4FQRV38NSP6gDo8qDbTBfy8UDHUd6Lzu4GXbHtfvWbtCArkVcbGQwinZ7M61eCmPEF5L8xu4tLAXL7ozbh5scfRi"),
        // decompress
        seed_txn("3Ct9n9hv5PWEYbsrgDdUDqegzsnX2n5jYRxkq5YafFAueup8mTYmN4nHhNCaEwVyVAVqNssr4fizdg9wRavT7ydE"),
        // regular nft mint
        seed_nft("Az9QTysJj1LW1F7zkYF21HgBj3FRpq3zpxTFdPnAJYm8"),
    ];

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_cnft_scenario_mint_update_metadata() {
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

    // Mint a compressed NFT and then update its metadata. Verify correct state regardless of order.
    let asset_id = "FLFoCw2RBbxiw9rbEeqPWJ5rasArD9kTCKWEJirTexsU";
    let seeds: Vec<SeedEvent> = vec![
        // mint cNFT
        seed_txn("2DP84v6Pi3e4v5i7KSvzmK4Ufbzof3TAiEqDbm9gg8jZpBRF9f1Cy6x54kvZoHPX9k1XfqbsG1FTv2KVP9fvNrN6"),
        // update metadata
        seed_txn("3bsL5zmLKvhN9Je4snTKxjFSpmXEEg2cvMHm2rCNgaEYkNXBqJTA4N7QmvBSWPiNUQPtzJSYzpQYX92NowV3L7vN"),
    ];

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_cnft_scenario_mint_update_metadata_remove_creators() {
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

    // Mint a compressed NFT and then update its metadata to remove creators.
    // Creator removal inserts a placeholder creator to handle out-of-order updates.
    // This test explicitly verifies this behaviour.
    let asset_id = "Gi4fAXJdnWYrEPjQm3wnW9ctgG7zJjB67zHDQtRGRWyZ";
    let seeds: Vec<SeedEvent> = vec![
        // mint cNFT
        seed_txn("2qMQrXfRE7pdnjwobWeqDkEhsv6MYmv3JdgvNxTVaL1VrMCZ4JYkUnu7jiJb2etX3W9WyQgSxktUgn9skxCeqTo5"),
        // update metadata (no creators)
        seed_txn("41YW187sn6Z2dXfqz6zSbnPtQoE826cCSgTLnMLKa9rH1xrCqAXBQNwKnzjGc9wjU5RtMCqKhy2eMN2TjuYC8veB"),
    ];

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_cnft_owners_table() {
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

    let transactions = vec![
        "25djDqCTka7wEnNMRVwsqSsVHqQMknPReTUCmvF4XGD9bUD494jZ1FsPaPjbAK45TxpdVuF2RwVCK9Jq7oxZAtMB",
        "3UrxyfoJKH2jvVkzZZuCMXxtyaFUtgjhoQmirwhyiFjZXA8oM3QCBixCSBj9b53t5scvsm3qpuq5Qm4cGbNuwQP7",
        "4fzBjTaXmrrJReLLSYPzn1fhPfuiU2EU1hGUddtHV1B49pvRewGyyzvMMpssi7K4Y5ZYj5xS9DrJuxqJDZRMZqY1",
    ];
    for txn in transactions {
        index_transaction(&setup, Signature::from_str(txn).unwrap()).await;
    }

    for (request, individual_test_name) in [
        (
            SearchAssets {
                owner_address: Some("F3MdnVQkRSy56FSKroYawfMk1RJFo42Quzz8VTmFzPVz".to_string()),
                page: Some(1),
                limit: Some(5),
                ..SearchAssets::default()
            },
            "base",
        ),
        (
            SearchAssets {
                owner_address: Some("3jnP4utL1VvjNhkxstYJ5MNayZfK4qHjFBDHNKEBpXCH".to_string()),
                page: Some(1),
                limit: Some(5),
                ..SearchAssets::default()
            },
            "with_different_owner",
        ),
    ] {
        let response = setup.das_api.search_assets(request.clone()).await.unwrap();
        insta::assert_json_snapshot!(format!("{}-{}", name, individual_test_name), response);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_no_json_uri() {
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
    let seeds = vec![seed_txn(
        "4ASu45ELoTmvwhNqokGQrh2VH8p5zeUepYLbkcULMeXSCZJGrJa7ojgdVh5JUxBjAMF9Lrp55EgUUFPaPeWKejNQ",
    )];
    run_get_asset_scenario_test_with_assert(
        &setup,
        "DFRJ4PwAze1mMQccRmdyc46yQpEVd4FPiwtAVgzGCs7g",
        seeds,
        Order::Forward,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_delegate_transfer() {
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

    let asset_id = "77wWrvhgEkkQZQVA2hoka1JTsjG3w7BVzvcmqxDrVPWE";

    let seeds: Vec<SeedEvent> = seed_txns([
        "KNWsAYPo3mm1HuFxRyEwBBMUZ2hqTnFXjoPVFo7WxGTfmfRwz6K8eERc4dnJpHyuoDkAZu1czK55iB1SbtCsdW2",
        "3B1sASkuToCWuGFRG47axQDm1SpgLi8qDDGnRFeR7LB6oa5C3ZmkEuX98373gdMTBXED44FkwT227kBBAGSw7e8M",
        "5Q8TAMMkMTHEM2BHyD2fp2sVdYKByFeATzM2mHF6Xbbar33WaeuygPKGYCWiDEt3MZU1mUrq1ePnT9o4Pa318p8w",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_redeem_cancel_redeem() {
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

    let asset_id = "5WaPA7HLZKGg56bcKiroMXAzHmB1mdxK3QTeCDepLkiK";

    let seeds: Vec<SeedEvent> = seed_txns([
        "3uzWoVgLGVd9cGXaF3JW7znpWgKse3obCa2Vvdoe59kaziX84mEXTwecUoZ49PkJDjReRMSXksKzyfj7pf3ekAGR",
        "49bJ8U3cK9htmLvA1mhXXcjKdpV2YN5JQBrb3Quh7wxENz1BP9F8fE9CKsje41aMbZwzgomnkXirKx2Xpdvprtak",
        "32FpSe6r9jnFNjjvbx2PPQdZqs5KpMoF6yawiRW1F6ctu1kmx2B4sLDBGjsthVQtmnhaJVrqdtmUP893FwXCbqY5",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_redeem() {
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

    let asset_id = "Az9QTysJj1LW1F7zkYF21HgBj3FRpq3zpxTFdPnAJYm8";

    let seeds: Vec<SeedEvent> = seed_txns([
        "55tQCoLUtHyu4i6Dny6SMdq4dVD61nuuLxXvRLeeQqE6xdm66Ajm4so39MXcJ2VaTmCNDEFBpitzLkiFaF7rNtHi",
        "4FQRV38NSP6gDo8qDbTBfy8UDHUd6Lzu4GXbHtfvWbtCArkVcbGQwinZ7M61eCmPEF5L8xu4tLAXL7ozbh5scfRi",
        // Purpose of this test is to check flow mint, redeem. But this last transaction is decompress.
        // Doesn't make sense to execute it and also such as Aura node implementation is deleting data from asset leaf
        // column family API response is different from expected, after parsing this last tx.
        // For comparison reference implementation doesn't drop asset leaf data.
        // Leave this signature hash here for future in case we need it.
        // "3Ct9n9hv5PWEYbsrgDdUDqegzsnX2n5jYRxkq5YafFAueup8mTYmN4nHhNCaEwVyVAVqNssr4fizdg9wRavT7ydE",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_transfer_burn() {
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

    let asset_id = "8vw7tdLGE3FBjaetsJrZAarwsbc8UESsegiLyvWXxs5A";

    let seeds: Vec<SeedEvent> = seed_txns([
        "5coWPFty37s7haT3SVyMf6PkTaABEnhCRhfDjXeMNS58czHB5dCFPY6VrsZNwxBnqypmNic1LbLp1j5qjbdnZAc8",
        "k6jmJcurgBQ6F2bVa86Z1vGb7ievzxwRZ8GAqzFEG8HicDizxceYPUm1KTzWZ3QKtGgy1EuFWUGCRqBeKU9SAoJ",
        "KHNhLijkAMeKeKm6kpbk3go6q9uMF3zmfCoYSBgERe8qJDW8q5ANpnkyBuyVkychXCeWzRY8i5EtKfeGaDDU23w",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_transfer_v2() {
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

    let asset_id = "75YC6nCeRDTQcHyPiXAXC3qov4u8gyBYCR4tci3ZrCPt";
    let seeds: Vec<SeedEvent> = seed_txns([
        "3juMkgXHHKbjUuiDfDiodyEG7jQWarKR481FjRVpvCx1Vh7UJiSRkRmnvSa4vjG3FiEYTjTJy79Gb2vXfAa8cfNZ", // Create Collection
        "WsRbKgNXubCZUB4Pv9QmZDnxFJeZWxcvwbCnUUG7KgbByRML6pAosSYTdFddNkU1puqRG3E3evNqmWNB5EX4m2g", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["ownership"]["owner"], "AAMwN4VGzRfTz5SYPZPBAqfkpWYtQfiyRCKPVyzszXip");
    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "4MxRc1TakjW3cDtX7ZSMdGu5owDTSWTvfFepBLi3x9SE6xfeD1parzQC7hmfN664NgLocPdfpHTADNWg3n8VbatZ",
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["ownership"]["owner"], "FmNJUQFbJ1JhTyP8cbJebnDxZ4ESWUm7h7dwC53SAE5a");

    insta::assert_json_snapshot!(format!("{}__transfer", setup.name.clone()), asset_after);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_burn_v2() {
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

    let asset_id = "74jWt9uZrs6aJ1eEqswhSMSbJYTnysiAuBjYgh6ikcAD";
    let seeds: Vec<SeedEvent> = seed_txns([
        "5sENdmHEVdyAt36XmQzdpAJNCMasLrbzniTW9yqbHgjXyo6DU1qGddmsEVinmKfcsrgmaf5dMXtLnc1hckwPqfpo", // Create Collection
        "29Sfa4vsABmq8PAgoNmAmJ3ittgJGjNjoZa5hnwPwwsm7ctdnFLa2ug81whhiN7mXyvwFZWyGzNU7n8dd55ktn3H", // Mint Asset
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["burnt"], false);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "utGWqqYXpU2XjNPHyfP8t3J5CYLyfEEpiXrNrvtDdoUQLcA6uB1zkXwNcPGydckdZWXnsS3TRkE5d2TV9GGjK8E", // Burn
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["burnt"], true);

    insta::assert_json_snapshot!(format!("{}__burn", setup.name.clone()), asset_after);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_delegate_and_freeze_v2() {
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

    let asset_id = "39yBMNhayqjPPoyCoaHsMTpzZEqHSh6j9UmHG1ZeJvNm";
    let seeds: Vec<SeedEvent> = seed_txns([
        "5EGtGA3pys7en1m79CFpRPieTshEtkiYrqfgpZUHxbU2AmzcjsjTpGq8aop4FW4A53JnuvZcqLnC9a3vZiKvkJh7", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["compression"]["flags"], 0);
    assert_eq!(asset["ownership"]["frozen"], false);
    assert_eq!(asset["ownership"]["non_transferable"], false);
    assert_eq!(asset["ownership"]["delegated"], false);
    assert_eq!(asset["ownership"]["delegate"], Value::Null);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "3FEjLPikuzqL67MYJyjYQLGyLyVRYQ1TcxDdEPQQjhLZBKgWcHNB2RiYqYSmozwjYvgoGLBHo7fcY5Ja4YPDLGAe", // delegateAndFreezeV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["compression"]["flags"], 1);
    assert_eq!(asset_after["ownership"]["frozen"], true);
    assert_eq!(asset_after["ownership"]["non_transferable"], false);
    assert_eq!(asset_after["ownership"]["delegated"], true);
    assert_eq!(
        asset_after["ownership"]["delegate"],
        "HqC5ZwbE6pN8vPG7Uo5tz37FdsKeSTrwmUZRVvD6idej"
    );

    insta::assert_json_snapshot!(
        format!("{}__delegate_and_freeze", setup.name.clone()),
        asset_after
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_freeze_v2() {
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

    let asset_id = "CTs4CB81qEZCfkJeBNc4K4XyRKvAGQK5mQmmfdLenqVP";
    let seeds: Vec<SeedEvent> = seed_txns([
        "4xRavuyJ8Sa1nEhyqrUeNpYHgopkpnEWWSYYoTut48CDJRrT46aN1CnU2KCgNUBfpLngNvG9SUkMfHGGNR5x5pZa", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["compression"]["flags"], 0);
    assert_eq!(asset["ownership"]["frozen"], false);
    assert_eq!(asset["ownership"]["non_transferable"], false);
    assert_eq!(asset["ownership"]["delegated"], false);
    assert_eq!(asset["ownership"]["delegate"], Value::Null);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "5tRgiuLGebPVQu6ytPNsW6RVWyvM3pkvSPgRc72Qkdvu7rwoBKvaenHkND4qD3JQAWL1CkuCMwnAm91w42tUYg77", // freeze_v2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 3);
    assert_eq!(asset_after["compression"]["flags"], 1);
    assert_eq!(asset_after["ownership"]["frozen"], true);
    assert_eq!(asset_after["ownership"]["non_transferable"], false);
    assert_eq!(asset_after["ownership"]["delegated"], true);
    assert_eq!(
        asset_after["ownership"]["delegate"],
        "5AnsVodEgWktRJZ4b2vdfoErSQe4QnKsqx9pDavYpwUB"
    );

    insta::assert_json_snapshot!(format!("{}__freeze", setup.name.clone()), asset_after);
}
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_delegate_v2() {
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

    let asset_id = "YnBykqbzHvNsC4q3L9DK1W9J1te57FV3eB3NMERvyB8";
    let seeds: Vec<SeedEvent> = seed_txns([
        "2sZbpWUHiL2TEA48CsYGKwYBeefo54PTMVT8Pc9PMrRNnPBpJm4uUHUsfob6FtU58pEpnYhrKmhqEcYwDJYjntW7", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["ownership"]["delegated"], false);
    assert_eq!(asset["ownership"]["delegate"], Value::Null);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "38ErTmuwXXX3NZ3pwK81hTzjHyMQH7yC65mppPHUMW83KE6cSZrYtp23nmv58U7QUMQy4WmfvEQZ8GHQibt1y2hz", // delegateV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["ownership"]["delegated"], true);
    assert_eq!(
        asset_after["ownership"]["delegate"],
        "A2EtDNcfnKnKQ3yqP5yJMVVx3oAi1k1aaWb3qNZxxLPJ"
    );

    insta::assert_json_snapshot!(format!("{}__delegate", setup.name.clone()), asset_after);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_thaw_and_revoke_v2() {
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

    let asset_id = "E59c8LNXhYD9Gh93UqKoQXdKD7qCUmKnzT3p3EQLYeHj";
    let seeds: Vec<SeedEvent> = seed_txns([
        "rBMrxLUJqy2veGRSy82MLEYqcAo6FrvsovSkFTwRoKWJJXjzgZkrrU7YqGXitLNAaPoiaf7ETCyENcQdxe3qyfL", // Mint
        "3bMpRY9Eq8eLMsq4WzpVTPXM8PHFauPH7DjNQCQca8qtrRihmSgCUrsUaVx7Nydn76m2mCUzDMKs327TeQps2jdP", // delegateAndFreezeV2
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 2);
    assert_eq!(asset["compression"]["flags"], 1);
    assert_eq!(asset["ownership"]["frozen"], true);
    assert_eq!(asset["ownership"]["non_transferable"], false);
    assert_eq!(asset["ownership"]["delegated"], true);
    assert_eq!(asset["ownership"]["delegate"], "5LgXYsnFpBC7mhS9aSbKSRc7sUmyFPun6kHazgvnuBvE");

    insta::assert_json_snapshot!(format!("{}__delegate", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "5QMtikA2DUNWrAESBgwvS8zTHhj6rAejpgQg3HCuURiYdAB4XLvJT7xrj9NkKPN7vTXcDjXWzLP9J4oxiW2JipMJ", // thawAndRevokeV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset_after["compression"]["seq"], 3);
    assert_eq!(asset_after["ownership"]["frozen"], false);
    assert_eq!(asset_after["compression"]["flags"], 0);
    assert_eq!(asset_after["ownership"]["non_transferable"], false);
    assert_eq!(asset_after["ownership"]["delegated"], false);
    assert_eq!(asset_after["ownership"]["delegate"], Value::Null);

    insta::assert_json_snapshot!(format!("{}__revoke", setup.name.clone()), asset);
}

// FREEZE_V2 USING PERMANENT DELEGATE (RESULTS IN PERMANENT LVL FREEZE BIT SET)
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_freeze_v2_with_permanent_delegate() {
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

    let asset_id = "GE4t6bZza7p8dx7aRmR6S5NcbNjmZ8SePZwgoSVSuWds";
    let seeds: Vec<SeedEvent> = seed_txns([
        "reermdRYZ5DNWCZ4BqKLSua5EwjPQVXwRcmL9tjph5es5wMjuzfec6fQXDFsZV2qsDZnwgj4Y41iYmaYcDjyjXt", // Create Collection
        "256W5Pf9mvKEnLZdFZCZiXaLhLUveJw8KYxUz7pj4AGAvNV1G8XsuFL99PTKSjJg8VaNfuEG3ybkah2D6xBWszDp", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["compression"]["flags"], 0);
    assert_eq!(asset["ownership"]["frozen"], false);
    assert_eq!(asset["ownership"]["non_transferable"], false);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "3ScdX5stXc8xvLRdQdVVZxms1iqC9srVEGiRbJsEGM5nieizCvpCSJx1Fgz53DBt34ZxcX7T1ePbDtCmVTVfZ374", // FreezeV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["compression"]["flags"], 2);
    assert_eq!(asset_after["ownership"]["frozen"], true);
    assert_eq!(asset_after["ownership"]["non_transferable"], false);

    insta::assert_json_snapshot!(format!("{}__freeze", setup.name.clone()), asset_after);
}

// REMOVE FROM COLLECTION USING SET_COLLECTION_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_set_collection_v2() {
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

    let asset_id = "Beter35aQLYBh1JbaNX8XM4qiTpwHDRiAxCCNKcS3goF";
    let seeds: Vec<SeedEvent> = seed_txns([
        "3Cf1oJSHgyCmeCUqTrHjWAxWHENRzrKVy6ZDNKiYiiwBFxP8uozgor2iFo8AXg9Gisq5zvYD59Gn5MKcpvzQBa9x", // Create Collection
        "4mSnGCxijXiLySxnotCDgVfU8b3xh6zskKvRiHktDxaJ38SnKpvtosz6vKNNqMAYRHTmtnkhS6LFN6hQYVYyM6pp", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["grouping"][0]["group_value"], "6KTEexidvMaYSTyWrjmpjMQy1naRGX24sKiPQCAdD1Vs");

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "3LEkoAGe5ZL2bN2BbiV2np66X15Zr1TfzBGXG9fMXwBRswFL5GLHGSZvBFjpzJKxtQutMh5sVbuZcKgdbgckym8n", // SetCollection
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(
        asset_after["grouping"][0]["group_value"],
        "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"
    );

    insta::assert_json_snapshot!(format!("{}__set_collection", setup.name.clone()), asset_after);
}

// ADD ASSET NOT IN A COLLECTION TO A COLLECTION USING SET_COLLECTION_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_set_collection_v2_add_asset_not_in_collection() {
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

    let asset_id = "9HYtHehrvfADZzzqWy7uYGPPK1xsYRHAyXo6z89Pxsd7";
    let seeds: Vec<SeedEvent> = seed_txns([
        "648K759Xvk2vW195qTc7B99tadL2AeyXVV31sd72cUwGMKRG6tUcR1mrMhZJXddZwvNiQxwY8wEYE5XHscRrsHU6", // Mint
        "2J6eZjVzV4UjEhkcFAFy4eNUiVqLSebDZ4WgcaMK1ZKHdvS29BiuevDmJ4K5XBfyMEUrzbQkYJccV4pFtysNhfLQ", // Create Collection
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert!(asset["grouping"].as_array().map_or(false, |grouping| grouping.is_empty()));

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "2xgp6N8KJwbxtPeqam8xVQJweCgQw52f8eJc8FY9sBbZmFFMhLYZzSC7whqhsxBkN2iYwD8dftbusDSQkP46hCgv", // setCollectionV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(
        asset_after["grouping"][0]["group_value"],
        "EVHJyhM5mTsVWpw62KnrSoBTB7rPaRcWay6fZr1iEsru"
    ); // core collection

    insta::assert_json_snapshot!(format!("{}__set_collection", setup.name.clone()), asset_after);
}

// MOVE FROM ONE COLLECTION TO ANOTHER USING SET_COLLECTION_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_set_collection_v2_from_one_to_another_collection() {
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

    let asset_id = "DBLxvRsN2awoW2ybGxuAupFxq5dna2CCXHqvv2MdfsM";
    let seeds: Vec<SeedEvent> = seed_txns([
        "HjSEyP7FtHs6XkpxcYTA8dntfDVDvBaSefW4jtAWJ3itWwTwTspqYxHGpPYPjunfUahb8DfE4bdbyzV4ZGC8eAb", // Create Collection
        "4DLwTikgbXfo77QABqVXLxESDEeKFwgBf7fVbSQ8seedPkYTEs151N2L91GJr192NGmzV2NpqDG6C1QxY1CnJf94", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["grouping"][0]["group_value"], "2aNJcrqdT8Y3MwUvPVTc5bKxxiiSJVMoaHH9RYVoUWZk");

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "4gGpH1Y4bJ9YVLnNLcQ6UBcPMwYRt4yMybJrukTxda1hnqVy78nzUpMPC28Y2xZGBxwd1EQpRZZ7z4F81LCLdeiV", // New Collection
        "is5PvsVrXuJz8iYPNudTd7WHRomuG3aCuQoepwQtJaenjcsrf7v3XDFz9Nh5wyjkCzSxV21QNHznXgoPJrjoU3N", // setCollectionV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(
        asset_after["grouping"][0]["group_value"],
        "4JhroY9LZieMAUyfNKKPmhJV3tSBQzLJNYdRdiQAiBjw"
    );

    insta::assert_json_snapshot!(format!("{}__set_collection", setup.name.clone()), asset_after);
}

// MINT_V2 AND TRANSFER_V2 (NOT MINTING TO COLLECTION)
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_transfer_v2_without_collection() {
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

    let asset_id = "6H3vEmt8dy8cZnsikDzkZSSvuH7yJvbeKAWTYYvvt2es";
    let seeds: Vec<SeedEvent> = seed_txns([
        "LAfk7AmWYGV7UC3vGnWok1ctFSDWT4Extch7YQzTbeX8LFp52UkceMavzqnqN3fxHakKfiFHbcnLjpJi9nwLzTo", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["ownership"]["owner"], "8xHGP2J1PBN2b1Vc87VJfbWyhAuhYbawD1SRWV864ro3");
    assert!(asset["grouping"].as_array().map_or(false, |grouping| grouping.is_empty()));

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "3puKBnzpwnG7FcyAanib3MTHimz3hdSjBVUhQXMGiW4Jj79V2WaMHVp83D9E2HrCiNvkrjxq1Po17rAVHAp1NLGG", // Transfer
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["ownership"]["owner"], "ALk8WMqaGd8chsKt3eLoZwoLgQ3CJuXVELPYVw2HFVt2");
    assert!(asset_after["grouping"].as_array().map_or(false, |grouping| grouping.is_empty()));

    insta::assert_json_snapshot!(format!("{}__transfer", setup.name.clone()), asset_after);
}

// MINT_V2 ONLY - NOT MINTING TO A COLLECTION
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_without_collection() {
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

    let asset_id = "CbECnkXvuNBoLsLXyo2ui3LgeZBWRoPQ4eosXoGWRYrD";
    let seeds: Vec<SeedEvent> = seed_txns([
        "s8xQ2krANSLnxsT5Kjpyi5hfC5WL5TEHWmZJrLhe7xDUhYnDwuGx9xeRFJEFUMEtbeztzZucCgPNPxUgpXsrPF1", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["ownership"]["owner"], "GoDXnQE42EjcsuFLh4tYCJ4BJ5mA7t189uqU2MiLxUTX");
    assert!(asset["grouping"].as_array().map_or(false, |grouping| grouping.is_empty()));

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);
}

// MINT_V2 ONLY - MINTING TO A COLLECTION
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_to_collection() {
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

    let asset_id = "D3DM89kvKqYhnnjqF8G5PYoEvtFZd2QP1ShCsNjuYhDY";
    let seeds: Vec<SeedEvent> = seed_txns([
        "WxohxWaa59Q4Y9syhxwPyfCen2rLvK6r3G6L8CFAd9jo1g4mjNwdT4N5HC8mgbVKK51NSKcWVeWVc6EpxkWSYHn", // Create Collection
        "5dVi4vSqCxQej1x82FUStsXjS7yGUNzx5v82FnKMMNX4VYf5NUxJeVRyshB37tDh62c6fAEMGJYphGh9ofz366FT", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["ownership"]["owner"], "64SX2SeTgVubJzHrTq3a6TrbHy3Y1DBr3FKfKAu2K7PU");
    assert_eq!(asset["grouping"][0]["group_value"], "843a72ky3UtfQkkGfUm2khSRLHia7GsM52RQ1CWGRJxb");

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);
}

// VERIFY_CREATOR_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_verify_creator_v2() {
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

    let asset_id = "xBSehSHSvAFfyFfKNVdUnfKAQRT6yWvhbKogJTsGDDS";
    let seeds: Vec<SeedEvent> = seed_txns([
        "3WnSUUv3w3abdRiEKoxxnNV2CEoSFA7uxQRQHPPmi917AS5vea6d8XeioGXok9eUsxxNEW7ZXPpAvEJxMjjMPCgq", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["creators"][0]["verified"], false);

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "Z8cRXqVVKuTe25xkFFwkPYB5hCoFgGZC4vZRuVsUp8LdKSczBqJaDESZgs4BJQAnPJzySwrSja9xevSfiqcjRxp", // Verify creator
    ]);

    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["creators"][0]["verified"], true);

    insta::assert_json_snapshot!(format!("{}__verify", setup.name.clone()), asset);
}

// UNVERIFY_CREATOR_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_unverify_creator_v2() {
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

    let asset_id = "AmV5tqeSncCzdMVQPFVBz9q1Jh6VgCv6WDBAMwKGgBsh";
    let seeds: Vec<SeedEvent> = seed_txns([
        "31eEawaWc6NffQr4w1fKrtjBqjjNFyFHUnnBxox4vap5M7kqC6z1QH3EMCserGPVWd5yu8nYPSy5nUbd5u4d43Pf", // Mint
        "3yerUamgZLAsDHbp4WuPcBxpjfEbFtkUXuum1GgHdPKk2hKb7auPYb7i6gVJmHNeQ9ocMW1JFN5pY9hucssP6y4B", // Verify Creators
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 3);
    assert_eq!(asset["creators"][0]["verified"], true);
    assert_eq!(asset["creators"][1]["verified"], true);

    insta::assert_json_snapshot!(format!("{}__mint_verify", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "27dyWCkK9z6KfY6Z1EP2Bkw7uR1JqFyGpnAQPDvFL7SYiRDvA231idzVMHAiMGsRtcjBwJd7tJe3dbcqGTeufwYa", // Unverify creator
    ]);

    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset_after["compression"]["seq"], 4);
    assert_eq!(asset_after["creators"][0]["verified"], false);
    assert_eq!(asset_after["creators"][1]["verified"], true);

    insta::assert_json_snapshot!(format!("{}__unverify", setup.name.clone()), asset);
}

// UPDATE_METADATA_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_v2_and_update_metadata_v2() {
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

    let asset_id = "4TBxZUkV9eDjFoi4HeJX924fkExCFXLSTKkdoNy7iiT8";
    let seeds: Vec<SeedEvent> = seed_txns([
        "3t9a7eknQGqqtcHvQuEhPr388RsMqqCQToYbCPm9vkyQFusaT7EqyNc4wakmF1LDYWDV46BtGSmSdYSZivKXuh2o", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    assert_eq!(asset["content"]["json_uri"], "https://example.com/my-nft.json");
    assert_eq!(asset["content"]["metadata"]["name"], "My NFT");

    insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "5vJJgNV4tbS3ChjwGZg5SzM21K1xNA5wRH3xtbc2nifEDDv6CWP64ocE6Qgqk6f4gGEWWZEWLXz2qbUJCzQGLCie", // updateMetadataV2
    ]);

    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset_after["compression"]["seq"], 2);
    assert_eq!(asset_after["content"]["json_uri"], "https://updated-example.com/my-nft.json");
    assert_eq!(asset_after["content"]["metadata"]["name"], "New name");

    insta::assert_json_snapshot!(format!("{}__update", setup.name.clone()), asset_after);
}

// SET_NON_TRANSFERABLE_V2
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
#[ignore] //  TransactionParsingError "Failed to parse transaction:  arsing_result.leaf_update is None, (Debug/FIX)
async fn test_mint_v2_set_non_transferable_v2() {
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

    let asset_id = "3TPJyEin3WFrH7X8bWQ6TqVNZV4XkiRhYJ3nWJRM62aT";
    let seeds: Vec<SeedEvent> = seed_txns([
        "PLmNi1QceegcoyiNkJgRRmXvRcLEaHhMJxBrJaVmuB9kykQvvzTaqMJxCfCggCKVAVXFJqKSh4us8Gnd3ak5SPL", // Create Collection
        "5Abumvi9rL7y7igRbKZjAqRfgNu4fZCAoBSpL3aiP5NrwBSouRSweA1JvUkrhBniK3yVWL7xdW2Fu2twLQskyqQV", // Mint
    ]);

    let asset = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();

    assert_eq!(asset["compression"]["seq"], 1);
    //
    // insta::assert_json_snapshot!(format!("{}__mint", setup.name.clone()), asset);

    let seeds: Vec<SeedEvent> = seed_txns([
        "3kqiQBhDxX3vbaDWyAs3q9kNemFj4Uh7ot66FihjZsTcaL76D4H48tkVxBq4864LMkzEVibt3rV8HuRqE2Gtcj6t", // setNonTransferableV2
    ]);
    let asset_after = run_get_asset_scenario_test(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await
    .unwrap();
    assert_eq!(asset_after["compression"]["seq"], 2);
    //
    // insta::assert_json_snapshot!(format!("{}__set_non_transferable", setup.name.clone()), asset_after);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_transfer_noop() {
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

    let asset_id = "7myVr8fEG52mZ3jAwgz88iQRWsuzuVR2nfH8n2AXnBxE";

    let seeds: Vec<SeedEvent> = seed_txns([
        "4nKDSvw2kGpccZWLEPnfdP7J1SEexQFRP3xWc9NBtQ1qQeGu3bu5WnAdpcLbjQ4iyX6BQ5QGF69wevE8ZeeY5poA",
        "4URwUGBjbsF7UBUYdSC546tnBy7nD67txsso8D9CR9kGLtbbYh9NkGw15tEp16LLasmJX5VQR4Seh8gDjTrtdpoC",
        "5bNyZfmxLVP9cKc6GjvozExrSt4F1QFt4PP992pQwT8FFHdWsX3ZFNvwurfU2xpDYtQ7qAUxVahGCraXMevRH8p1",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_transfer_transfer() {
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

    let asset_id = "EcLv3bbLYr2iH5PVEuf9pJMRdDCvCqwSx3Srz6AeKjAe";

    let seeds: Vec<SeedEvent> = seed_txns([
        "5bq936UgGs4RnxM78iXp1PwVhr8sTYoEsHCWpr8QBFtc2YtS3ieYHcsPG46G2ikwrS3tXYnUK93PzseT52AR81RR",
        "5VC3Jqr5X1N8NB8zuSahHpayekLVozYkDiPjJLqU6H5M6fq9ExVLGYYKKCPbeksMPXTjy65sdEQGPzDWAYPs8QjP",
        "34xjcNf3rZFKz381hKpFLqxpojaDgXEpCqH5qcpTXLaJnDbtqRz35wiuMF1cAgvJGLzYYrwaMvCK1D7LxYsdpMU1",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_verify_creator() {
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

    let asset_id = "5rmTyghEuZhRTB77L3KqGMy6h5RpSNWNLj14avbxGNKB";

    let seeds: Vec<SeedEvent> = seed_txns([
        "37ts5SqpNazPTp26VfC4oeuXpXezKYkD9oarczPNaE8TUGG8msifnTYTBJiBZNBeAUGrNw85EEfwnR1t9SieKTdq",
        "4xrw5UwQSxxPzVxge6fbtmgLNsT2amaGrwpZFE95peRbnHGpxWtS2fF7whXW2xma4i2KDXdneztJZCAtgGZKTw11",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_verify_collection() {
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

    let asset_id = "2WjoMU1hBGXv8sKcxQDGnu1tgMduzdZEmEEGjh8MZYfC";

    let seeds: Vec<SeedEvent> = seed_txns([
        "63xhs5bXcuMR3uMACXWkkFMm7BJ9Thknh7WNMPzV8HJBNwpyxJTr98NrLFHnTZDHdSUFD42VFQx8rjSaGynWbaRs",
        "5ZKjPxm3WAZzuqqkCDjgKpm9b5XjB9cuvv68JvXxWThvJaJxcMJgpSbYs4gDA9dGJyeLzsgNtnS6oubANF1KbBmt",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_transfer_mpl_programs() {
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

    let asset_id = "ZzTjJVwo66cRyBB5zNWNhUWDdPB6TqzyXDcwjUnpSJC";

    let seeds: Vec<SeedEvent> = seed_txns([
        "3iJ6XzhUXxGQYEEUnfkbZGdrkgS2o9vXUpsXALet3Co6sFQ2h7J21J4dTgSka8qoKiUFUzrXZFHfkqss1VFivnAG",
        "4gV14HQBm8GCXjSTHEXjrhUNGmsBiyNdWY9hhCapH9cshmqbPKxn2kUU1XbajZ9j1Pxng95onzR6dx5bYqxQRh2a",
        "T571TWE76frw6mWxYoHDrTdxYq7hJSyCtVEG4qmemPPtsc1CCKdknn9rTMAVcdeukLfwB1G97LZLH8eHLvuByoA",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options::default(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_to_collection_unverify_collection() {
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

    let asset_id = "2gEbvG3Cb6JRaGWAx5e85Bf5z4u37EURBeyPBqXDzZoY";

    let seeds: Vec<SeedEvent> = seed_txns([
        "tzXASugk8578bmtA3JAFQLEfcVQp3Np3rU9fyFas2Svk8nyBHXJnf7PdqebGNsSTwx6CEWpDCP5oLoCDcmbP35B",
        "7nK9a2DSDZ4Gh6DatmxGJmuLiDEswaY9bYSSPTtQppk7PtLKXYE84jWzm7AC4G1fpa831GaXuXcn5n5ybWqB4e5",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options {
            show_unverified_collections: true,
            show_collection_metadata: false,
            show_inscription: false,
            show_fungible: false,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[named]
async fn test_mint_verify_collection_unverify_collection() {
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

    let asset_id = "BiHHJ1gKV4exTjPe7PE6aydgMVqRUzzz8aeWYCGhZJ4s";

    let seeds: Vec<SeedEvent> = seed_txns([
        "5uWXt8JAhuP2XQ2nYJTq8Ndp34fdG3vmJ7DJnb3bE6iyrZZ6jeuN9w5jZvKrduMDu4zKyQU7A3JtswhKxE3hjKBk",
        "4hQQsDKgDx5PpZR7nGvxKsLSvX4J7voaiJC3ag7dPuu4HY5kbvaqD2gyeHbdja1f22ypmzouRNpuo6sbyGDSSgya",
        "5k71fZRpRagY45ZYu13Q8C3Bmw6KFPBkRmbBx2NuYk7roVtvM8P16WouCZtnkhRCyKyQHSgHKyTY92t9aq2tyLdd",
    ]);

    run_get_asset_scenario_test_with_assert(
        &setup,
        asset_id,
        seeds,
        Order::AllPermutations,
        Options {
            show_unverified_collections: true,
            show_collection_metadata: false,
            show_inscription: false,
            show_fungible: false,
        },
    )
    .await;
}
