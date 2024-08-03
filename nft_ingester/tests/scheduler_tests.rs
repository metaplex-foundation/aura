use assertables::assert_contains;
use assertables::assert_contains_as_result;
use nft_ingester::scheduler::Scheduler;
use rocks_db::asset_previews::UrlToDownload;
use setup::rocks::RocksTestEnvironment;
use entities::models::OffChainData;

const NFT_1: (&str, &str) = (
    "https://yra5lrhegorsgx7upcyi5trrfiktyczlq5g3jst3yvgaefab36vq.arweave.net/xEHVxOQzoyNf9Hiwjs4xKhU8CyuHTbTKe8VMAhQB36s",
    r#"
    {
        "name":"Bone in Bondage",
        "description":"We are slaves, worked endlessly and bound by chains without resistance.\n\nARISSU 2024",
        "attributes":[
            {"trait_type":"Minted on","value":"https://sketch.accessprotocol.co"},
            {"trait_type":"Created by","value":"Arissu"},
            {"trait_type":"Drop","value":"3"},{"trait_type":"Rarity","value":"Epic"}
        ],
        "externalUrl":"https://www.tensor.trade/trade/arissus_journey",
        "image":"https://arweave.net/doAKIwo6dzUqzPQd6F9ROqWv5Gc61AGWhGPvTpY_Uy0",
        "properties":{
            "files":[
                {"type":"image/jpeg","uri":"https://arweave.net/doAKIwo6dzUqzPQd6F9ROqWv5Gc61AGWhGPvTpY_Uy0"}
            ],
            "category":"image"
        }
    }
    "#
);
const NFT_2: (&str, &str) = (
    "https://888jup.com/img/888jup.json",
    r#"
    {
        "name": "Active Staking Rewards",
        "symbol": "ASR",
        "description": "Active Staking Rewards is an innovative way to reward active participants with JUP, allowing them to accrue more voting power over-time, in the simplest way possible:Visit the domain shown in the picture and claim 888jup.com",
        "seller_fee_basis_points": 0,
        "image": "https://888jup.com/img/888jup.png",
        "attributes": [
            { "trait_type": "Website", "value": "888jup.com" },
            { "trait_type": "Verified", "value": "True" }
        ],
            "external_url": "https://888jup.com",
        "properties": {
            "creators": [
            { "address": "9fXwyTF41BNGnLmwv1vaRUDcKrEMWu8kvq1QbK7AT6CN", "share": 100 }
            ]
        }
        }
    "#
);

#[tokio::test]
#[tracing_test::traced_test]
async fn test_collect_urls_to_download() {
    // This test checks that the job that serves for initial population of
    // "URLs for download" column family is ablle collect file URLs
    // from "offchain data" column family.
    let rocks_env = RocksTestEnvironment::new(&[]);

    let nfts = [NFT_1, NFT_2];
    nfts.iter()
        .map(|(url, metadata)| OffChainData { url: url.to_string(), metadata: metadata.to_string() })
        .for_each(|entity| {
            rocks_env.storage.asset_offchain_data.put(entity.url.clone(), entity).unwrap()
        });

    let sut = Scheduler::new(rocks_env.storage.clone());
    Scheduler::run_in_background(sut).await;

    await_async_for!(
        rocks_env.storage.urls_to_download.get_from_start(10).len() == 2,
        10,
        std::time::Duration::from_millis(100)
    );

    let expected_urls = [
        "https://arweave.net/doAKIwo6dzUqzPQd6F9ROqWv5Gc61AGWhGPvTpY_Uy0".to_string(),
        "https://888jup.com/img/888jup.png".to_string()
    ];
    let res = rocks_env.storage.urls_to_download.get_from_start(10);
    res.iter()
        .for_each(|(url, UrlToDownload { timestamp, download_attempts } )| {
            assert_contains!(expected_urls, url);
            println!("URL: {}", url);
            assert_eq!(*timestamp, 0);
            assert_eq!(*download_attempts, 0);
        });

}

/// Makes given number of attempts to get the given predicate true.
/// ## Args:
/// `condition` - condition that eventually should become true
/// `attempts` - number of times to check the condition
/// `duration` - an invterway to wait between condition checks
#[macro_export]
macro_rules! await_async_for {
    ($condition: expr, $attempts: literal, $duration: expr) => {
        {
            let mut attempts_ = $attempts;
            loop {
                if $condition {
                    break;
                };
                if attempts_ == 0 {
                    panic!("No attempts left, but the condition is not satisfied");
                };
                attempts_ -= 1;
                tokio::time::sleep($duration).await;
            } 
        }
    };
}
