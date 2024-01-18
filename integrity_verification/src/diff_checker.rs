use crate::api::IntegrityVerificationApi;
use crate::error::IntegrityVerificationError;
use crate::params::{
    generate_get_asset_params, generate_get_asset_proof_params,
    generate_get_assets_by_authority_params, generate_get_assets_by_creator_params,
    generate_get_assets_by_group_params, generate_get_assets_by_owner_params,
};
use crate::requests::Body;
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use metrics_utils::IntegrityVerificationMetricsConfig;
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
use regex::Regex;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

pub const GET_ASSET_METHOD: &str = "getAsset";
pub const GET_ASSET_PROOF_METHOD: &str = "getAssetProof";
pub const GET_ASSET_BY_OWNER_METHOD: &str = "getAssetsByOwner";
pub const GET_ASSET_BY_AUTHORITY_METHOD: &str = "getAssetsByAuthority";
pub const GET_ASSET_BY_GROUP_METHOD: &str = "getAssetsByGroup";
pub const GET_ASSET_BY_CREATOR_METHOD: &str = "getAssetsByCreator";

const REQUESTS_INTERVAL_MILLIS: u64 = 1500;

pub struct DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    reference_host: String,
    testing_host: String,
    api: IntegrityVerificationApi,
    keys_fetcher: T,
    metrics: Arc<IntegrityVerificationMetricsConfig>,
    regexes: Vec<Regex>,
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    pub fn new(
        reference_host: String,
        testing_host: String,
        keys_fetcher: T,
        metrics: Arc<IntegrityVerificationMetricsConfig>,
    ) -> Self {
        // Regular expressions, that purposed to filter out some difference between
        // testing and reference hosts that we already know about
        // Using unwraps is safe, because we pass correct patterns into Regex::new
        let regexes = vec![
            // token_standard field presented in new DAS-API spec, but we do not updated our implementation for now
            Regex::new(r#"json atom at path \".*?\.token_standard\" is missing from rhs\n*"#)
                .unwrap(),
            // cdn_uri field added by Helius, that do not presented in our impl
            Regex::new(r#"json atom at path \".*?\.cdn_uri\" is missing from rhs\n*"#).unwrap(),

            // Below placed regexes for ignoring errors that we must to fix, but already know about
            // TODO: remove after all fixes
            Regex::new(r#"json atoms at path \"(.*?\.compression\.seq)\" are not equal:\n\s*lhs:\n\s*\d+\n\s*rhs:\n\s*\d+\n*"#).unwrap(),
            Regex::new(r#"json atoms at path \"(.*?\.ownership\.delegate)\" are not equal:\n\s*lhs:\n\s*(null|\".*?\"|\d+)\n\s*rhs:\n\s*(null|\".*?\"|\d+)\n*"#).unwrap(),
            Regex::new(r#"json atoms at path \"(.*?\.ownership\.delegated)\" are not equal:\n\s*lhs:\n\s*(true|false|null|\".*?\"|\d+)\n\s*rhs:\n\s*(true|false|null|\".*?\"|\d+)\n*"#).unwrap(),

        ];

        Self {
            reference_host,
            testing_host,
            api: IntegrityVerificationApi::new(),
            keys_fetcher,
            metrics,
            regexes,
        }
    }
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    pub fn compare_responses(
        &self,
        reference_response: &Value,
        testing_response: &Value,
    ) -> Option<String> {
        if let Err(diff) = assert_json_matches_no_panic(
            &reference_response,
            &testing_response,
            Config::new(CompareMode::Strict),
        ) {
            let diff = self
                .regexes
                .iter()
                .fold(diff, |acc, re| re.replace_all(&acc, "").to_string());
            if diff.is_empty() {
                return None;
            }

            return Some(diff);
        }

        None
    }

    async fn check_requests<F, G>(
        &self,
        requests: Vec<Body>,
        metrics_inc_total_fn: F,
        metrics_inc_failed_fn: G,
    ) where
        F: Fn() -> u64,
        G: Fn() -> u64,
    {
        for req in requests.iter() {
            metrics_inc_total_fn();

            let request = json!(req).to_string();
            let reference_response_fut = self.api.make_request(&self.reference_host, &request);
            let testing_response_fut = self.api.make_request(&self.testing_host, &request);
            let (reference_response, testing_response) =
                tokio::join!(reference_response_fut, testing_response_fut);

            let reference_response = match reference_response {
                Ok(reference_response) => reference_response,
                Err(e) => {
                    self.metrics.inc_network_errors_reference_host();
                    error!("Reference host network error: {}", e);
                    continue;
                }
            };
            let testing_response = match testing_response {
                Ok(testing_response) => testing_response,
                Err(e) => {
                    self.metrics.inc_network_errors_testing_host();
                    error!("Testing host network error: {}", e);
                    continue;
                }
            };

            if let Some(diff) = self.compare_responses(&reference_response, &testing_response) {
                metrics_inc_failed_fn();
                error!(
                    "{}: mismatch responses: req: {:#?}, diff: {}",
                    req.method, req, diff
                );
            }

            // Prevent rate-limit errors
            tokio::time::sleep(Duration::from_millis(REQUESTS_INTERVAL_MILLIS)).await;
        }
    }

    pub async fn check_get_asset(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_assets_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| Body::new(GET_ASSET_METHOD, json!(generate_get_asset_params(key))))
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_asset_tested(),
            || self.metrics.inc_failed_get_asset_tested(),
        )
        .await;

        Ok(())
    }

    pub async fn check_get_asset_proof(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_assets_proof_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_PROOF_METHOD,
                    json!(generate_get_asset_proof_params(key)),
                )
            })
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_asset_proof_tested(),
            || self.metrics.inc_failed_get_asset_proof_tested(),
        )
        .await;

        Ok(())
    }

    pub async fn check_get_asset_by_authority(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_authorities_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_AUTHORITY_METHOD,
                    json!(generate_get_assets_by_authority_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_assets_by_authority_tested(),
            || self.metrics.inc_failed_get_assets_by_authority_tested(),
        )
        .await;

        Ok(())
    }

    pub async fn check_get_asset_by_owner(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_owners_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_OWNER_METHOD,
                    json!(generate_get_assets_by_owner_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_assets_by_owner_tested(),
            || self.metrics.inc_failed_get_assets_by_owner_tested(),
        )
        .await;

        Ok(())
    }

    pub async fn check_get_asset_by_group(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_groups_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_GROUP_METHOD,
                    json!(generate_get_assets_by_group_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_assets_by_group_tested(),
            || self.metrics.inc_failed_failed_get_assets_by_group_tested(),
        )
        .await;

        Ok(())
    }

    pub async fn check_get_asset_by_creator(&self) -> Result<(), IntegrityVerificationError> {
        let verification_required_keys = self
            .keys_fetcher
            .get_verification_required_creators_keys()
            .await
            .map_err(IntegrityVerificationError::FetchKeys)?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_CREATOR_METHOD,
                    json!(generate_get_assets_by_creator_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.check_requests(
            requests,
            || self.metrics.inc_total_get_assets_by_creator_tested(),
            || self.metrics.inc_failed_get_assets_by_creator_tested(),
        )
        .await;

        Ok(())
    }
}

#[tokio::test]
async fn test_regex() {
    let reference_response = json!({
        "jsonrpc": "2.0",
        "result": {
                "files": [
                    {
                        "uri": "https://assets.pinit.io/3Qru1Gjz9SFd4nESynRQytL65nXNcQGwc1eVbZz24ijG/ZyFU9Lt94Rb57y2hZpAssPCRQU6qXoWzkPhd6bEHKep/731.jpeg",
                        "cdn_uri": "https://cdn.helius-rpc.com/cdn-cgi/image//https://assets.pinit.io/3Qru1Gjz9SFd4nESynRQytL65nXNcQGwc1eVbZz24ijG/ZyFU9Lt94Rb57y2hZpAssPCRQU6qXoWzkPhd6bEHKep/731.jpeg",
                        "mime": "image/jpeg"
                    }
                ],
                "metadata": {
                    "description": "GK #731 - Generated and deployed on LaunchMyNFT.",
                    "name": "NFT #731",
                    "symbol": "SYM",
                    "token_standard": "NonFungible"
                },
            },
        "id": 0
    });

    let testing_response1 = json!({
    "jsonrpc": "2.0",
    "result": {
            "files": [
                {
                    "uri": "https://assets.pinit.io/3Qru1Gjz9SFd4nESynRQytL65nXNcQGwc1eVbZz24ijG/ZyFU9Lt94Rb57y2hZpAssPCRQU6qXoWzkPhd6bEHKep/731.jpeg",
                    "mime": "image/jpeg"
                }
            ],
            "metadata": {
                "description": "GK #731 - Generated and deployed on LaunchMyNFT.",
                "name": "NFT #731",
                "symbol": "SYM",
            },
        },
        "id": 0
    });

    let res = assert_json_matches_no_panic(
        &reference_response,
        &testing_response1,
        Config::new(CompareMode::Strict),
    )
    .err()
    .unwrap();

    let re1 =
        Regex::new(r#"json atom at path \".*?\.token_standard\" is missing from rhs\n*"#).unwrap();
    let re2 = Regex::new(r#"json atom at path \".*?\.cdn_uri\" is missing from rhs\n*"#).unwrap();
    let res = re1.replace_all(&res, "").to_string();
    let res = re2.replace_all(&res, "").to_string();

    assert_eq!(0, res.len());

    let testing_response2 = json!({
    "jsonrpc": "2.0",
    "result": {
            "files": [
                {
                    "uri": "https://assets.pinit.io/3Qru1Gjz9SFd4nESynRQytL65nXNcQGwc1eVbZz24ijG/ZyFU9Lt94Rb57y2hZpAssPCRQU6qXoWzkPhd6bEHKep/731.jpeg",
                    "mime": "image/jpeg"
                }
            ],
            "mutable": false,
            "metadata": {
                "description": "GK #731 - Generated and deployed on LaunchMyNFT.",
                "name": "NFT #731",
                "symbol": "SYM",
            },
        },
        "id": 0
    });

    let res = assert_json_matches_no_panic(
        &reference_response,
        &testing_response2,
        Config::new(CompareMode::Strict),
    )
    .err()
    .unwrap();

    let res = re1.replace_all(&res, "").to_string();
    let res = re2.replace_all(&res, "").to_string();

    assert_eq!(
        "json atom at path \".result.mutable\" is missing from lhs\n\n",
        res.trim()
    );
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_new_regex() {
    let helius = json!({
        "jsonrpc": "2.0",
        "result": {
            "interface": "V1_NFT",
            "id": "3Z6vaWvgwCb1835NoaFp6qMVb4jUhEMRqdQuuvxH5ubU",
            "content": {
                "$schema": "https://schema.metaplex.com/nft1.0.json",
                "json_uri": "https://arweave.net/1gaiazBrvTLSvOrjx0EkeTO6ATkqafdDlIq4j9TJM9c",
                "files": [
                    {
                        "uri": "https://arweave.net/DcZJWUkU54Q7cbO_D3x6nA-hsmq47EVlN7T5ZlFRRXg?ext=jpg",
                        "cdn_uri": "https://cdn.helius-rpc.com/cdn-cgi/image//https://arweave.net/DcZJWUkU54Q7cbO_D3x6nA-hsmq47EVlN7T5ZlFRRXg?ext=jpg",
                        "mime": "image/jpeg"
                    },
                    {
                        "uri": "",
                        "cdn_uri": "https://cdn.helius-rpc.com/cdn-cgi/image//",
                        "mime": "image/png"
                    }
                ],
                "metadata": {
                    "attributes": [
                        {
                            "value": "25",
                            "trait_type": "drop"
                        },
                        {
                            "value": "2023-10-01",
                            "trait_type": "created"
                        },
                        {
                            "value": "jpeg",
                            "trait_type": "filetype"
                        },
                        {
                            "value": "2500 x 2500",
                            "trait_type": "dimensions"
                        },
                        {
                            "value": "square",
                            "trait_type": "orientation"
                        },
                        {
                            "value": "common",
                            "trait_type": "rarity"
                        }
                    ],
                    "description": "An original portrait by Degen Poet, created with an Olympia Splendid 33 typewriter and watercolor pencils.",
                    "name": "Comet",
                    "symbol": "",
                    "token_standard": "NonFungible"
                },
                "links": {
                    "external_url": "https://drip.haus/degenpoet",
                    "animation_url": "",
                    "image": "https://arweave.net/DcZJWUkU54Q7cbO_D3x6nA-hsmq47EVlN7T5ZlFRRXg?ext=jpg"
                }
            },
            "authorities": [
                {
                    "address": "DZcDk4ahuNNfPdmoAbsSJB6QSWKYCU5CyFR9co97cr8E",
                    "scopes": [
                        "full"
                    ]
                }
            ],
            "compression": {
                "eligible": false,
                "compressed": true,
                "data_hash": "24Ahx1gwNNjuZUMd5qAUkPMLGcK6jYY3QsEq1CsNGQ62",
                "creator_hash": "EwBYYdBeEbhYXRZVVZrKz5aNFFwkohjhnfupP1ZLsixu",
                "asset_hash": "chrYJfWmBjgXd4DJ8yb4zM6avsyU2feTpVYpvZNTUBD",
                "tree": "HniGgAjEpEU2UTEkHoM87eDhsNKTWcWN2PBxvBycNG4S",
                "seq": 1176170,
                "leaf_id": 439935
            },
            "grouping": [
                {
                    "group_key": "collection",
                    "group_value": "DGPTxgKaBPJv3Ng7dc9AFDpX6E7kgUMZEgyTm3VGWPW6"
                }
            ],
            "royalty": {
                "royalty_model": "creators",
                "target": null,
                "percent": 0.030000000000000002,
                "basis_points": 300,
                "primary_sale_happened": false,
                "locked": false
            },
            "creators": [
                {
                    "address": "DGPTx9RcP5GkxRNscrSaSETcwuiK6xcbX93G1DSxrLoY",
                    "share": 0,
                    "verified": true
                },
                {
                    "address": "WoMbXFtdfH8crq2Zi7bQhfGx2Gv8EN4saP13gcdUGog",
                    "share": 50,
                    "verified": false
                },
                {
                    "address": "ART5dr4bDic2sQVZoFheEmUxwQq5VGSx9he7JxHcXNQD",
                    "share": 50,
                    "verified": false
                }
            ],
            "ownership": {
                "frozen": false,
                "delegated": false,
                "delegate": null,
                "ownership_model": "single",
                "owner": "HgLfJ34eukSchmwGATzFjHX6ecwipuzfhywG2CSHirSn"
            },
            "supply": {
                "print_max_supply": 0,
                "print_current_supply": 0,
                "edition_nonce": null
            },
            "mutable": true,
            "burnt": false
        },
        "id": 0
    });

    let extrnode = json!({
        "jsonrpc": "2.0",
        "result": {
            "interface": "V1_NFT",
            "id": "3Z6vaWvgwCb1835NoaFp6qMVb4jUhEMRqdQuuvxH5ubU",
            "content": {
                "$schema": "https://schema.metaplex.com/nft1.0.json",
                "json_uri": "https://arweave.net/1gaiazBrvTLSvOrjx0EkeTO6ATkqafdDlIq4j9TJM9c",
                "files": [
                    {
                        "uri": "https://arweave.net/DcZJWUkU54Q7cbO_D3x6nA-hsmq47EVlN7T5ZlFRRXg?ext=jpg",
                        "mime": "image/jpeg"
                    },
                    {
                        "uri": "",
                        "mime": "image/png"
                    }
                ],
                "metadata": {
                    "attributes": [
                        {
                            "trait_type": "drop",
                            "value": "25"
                        },
                        {
                            "trait_type": "created",
                            "value": "2023-10-01"
                        },
                        {
                            "trait_type": "filetype",
                            "value": "jpeg"
                        },
                        {
                            "trait_type": "dimensions",
                            "value": "2500 x 2500"
                        },
                        {
                            "trait_type": "orientation",
                            "value": "square"
                        },
                        {
                            "trait_type": "rarity",
                            "value": "common"
                        }
                    ],
                    "description": "An original portrait by Degen Poet, created with an Olympia Splendid 33 typewriter and watercolor pencils.",
                    "name": "Comet",
                    "symbol": ""
                },
                "links": {
                    "image": "https://arweave.net/DcZJWUkU54Q7cbO_D3x6nA-hsmq47EVlN7T5ZlFRRXg?ext=jpg",
                    "animation_url": "",
                    "external_url": "https://drip.haus/degenpoet"
                }
            },
            "authorities": [
                {
                    "address": "DZcDk4ahuNNfPdmoAbsSJB6QSWKYCU5CyFR9co97cr8E",
                    "scopes": [
                        "full"
                    ]
                }
            ],
            "compression": {
                "eligible": false,
                "compressed": true,
                "data_hash": "24Ahx1gwNNjuZUMd5qAUkPMLGcK6jYY3QsEq1CsNGQ62",
                "creator_hash": "EwBYYdBeEbhYXRZVVZrKz5aNFFwkohjhnfupP1ZLsixu",
                "asset_hash": "chrYJfWmBjgXd4DJ8yb4zM6avsyU2feTpVYpvZNTUBD",
                "tree": "HniGgAjEpEU2UTEkHoM87eDhsNKTWcWN2PBxvBycNG4S",
                "seq": 440036,
                "leaf_id": 439935
            },
            "grouping": [
                {
                    "group_key": "collection",
                    "group_value": "DGPTxgKaBPJv3Ng7dc9AFDpX6E7kgUMZEgyTm3VGWPW6"
                }
            ],
            "royalty": {
                "royalty_model": "creators",
                "target": null,
                "percent": 0.030000000000000002,
                "basis_points": 300,
                "primary_sale_happened": false,
                "locked": false
            },
            "creators": [
                {
                    "address": "DGPTx9RcP5GkxRNscrSaSETcwuiK6xcbX93G1DSxrLoY",
                    "share": 0,
                    "verified": true
                },
                {
                    "address": "WoMbXFtdfH8crq2Zi7bQhfGx2Gv8EN4saP13gcdUGog",
                    "share": 50,
                    "verified": false
                },
                {
                    "address": "ART5dr4bDic2sQVZoFheEmUxwQq5VGSx9he7JxHcXNQD",
                    "share": 50,
                    "verified": false
                }
            ],
            "ownership": {
                "frozen": false,
                "delegated": true,
                "delegate": "HgLfJ34eukSchmwGATzFjHX6ecwipuzfhywG2CSHirSn",
                "ownership_model": "single",
                "owner": "HgLfJ34eukSchmwGATzFjHX6ecwipuzfhywG2CSHirSn"
            },
            "supply": {
                "print_max_supply": 0,
                "print_current_supply": 0,
                "edition_nonce": null
            },
            "mutable": true,
            "burnt": false
        },
        "id": 0
    });

    let res = assert_json_matches_no_panic(&helius, &extrnode, Config::new(CompareMode::Strict))
        .err()
        .unwrap();

    let re1 =
        Regex::new(r#"json atom at path \".*?\.token_standard\" is missing from rhs\n*"#).unwrap();
    let re2 = Regex::new(r#"json atom at path \".*?\.cdn_uri\" is missing from rhs\n*"#).unwrap();
    let re3 = Regex::new(r#"json atoms at path \"(.*?\.compression\.seq)\" are not equal:\n\s*lhs:\n\s*\d+\n\s*rhs:\n\s*\d+\n*"#).unwrap();
    let re4 = Regex::new(r#"json atoms at path \"(.*?\.ownership\.delegate)\" are not equal:\n\s*lhs:\n\s*(null|\".*?\"|\d+)\n\s*rhs:\n\s*(null|\".*?\"|\d+)\n*"#).unwrap();
    let re5 = Regex::new(r#"json atoms at path \"(.*?\.ownership\.delegated)\" are not equal:\n\s*lhs:\n\s*(true|false|null|\".*?\"|\d+)\n\s*rhs:\n\s*(true|false|null|\".*?\"|\d+)\n*"#).unwrap();

    let res = re1.replace_all(&res, "").to_string();
    let res = re2.replace_all(&res, "").to_string();
    let res = re3.replace_all(&res, "").to_string();
    let res = re4.replace_all(&res, "").to_string();
    let res = re5.replace_all(&res, "").to_string();

    assert_eq!("", &res);
}
