use std::{str::FromStr, sync::Arc, time::Duration};

use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use interface::{error::IntegrityVerificationError, proofs::ProofChecker};
use metrics_utils::{BackfillerMetricsConfig, IntegrityVerificationMetricsConfig};
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
use regex::Regex;
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::CommitmentLevel;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::{
    bigtable::BigTableClient, proofs::MaybeProofChecker, slots_collector::SlotsCollector,
};

use crate::{
    api::IntegrityVerificationApi,
    params::{
        generate_get_asset_params, generate_get_asset_proof_params,
        generate_get_assets_by_authority_params, generate_get_assets_by_creator_params,
        generate_get_assets_by_group_params, generate_get_assets_by_owner_params,
    },
    requests::Body,
    slots_dumper::FileSlotsDumper,
};

pub const GET_ASSET_METHOD: &str = "getAsset";
pub const GET_ASSET_PROOF_METHOD: &str = "getAssetProof";
pub const GET_ASSET_BY_OWNER_METHOD: &str = "getAssetsByOwner";
pub const GET_ASSET_BY_AUTHORITY_METHOD: &str = "getAssetsByAuthority";
pub const GET_ASSET_BY_GROUP_METHOD: &str = "getAssetsByGroup";
pub const GET_ASSET_BY_CREATOR_METHOD: &str = "getAssetsByCreator";

const REQUESTS_INTERVAL_MILLIS: u64 = 1500;
const BIGTABLE_TIMEOUT: u32 = 1000;

#[derive(Default)]
struct DiffWithResponses {
    diff: Option<String>,
    reference_response: Value,
    testing_response: Value,
}

struct CollectSlotsTools {
    bigtable_client: Arc<BigTableClient>,
    slots_collect_path: String,
    metrics: Arc<BackfillerMetricsConfig>,
}

pub struct DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    reference_host: String,
    testing_host: String,
    api: IntegrityVerificationApi,
    keys_fetcher: T,
    metrics: Arc<IntegrityVerificationMetricsConfig>,
    collect_slots_tools: Option<CollectSlotsTools>,
    rpc_client: Arc<RpcClient>,
    proof_checker: MaybeProofChecker,
    regexes: Vec<Regex>,
    test_retries: u64,
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        reference_host: String,
        testing_host: String,
        keys_fetcher: T,
        test_metrics: Arc<IntegrityVerificationMetricsConfig>,
        slot_collect_metrics: Arc<BackfillerMetricsConfig>,
        bigtable_creds: Option<String>,
        slots_collect_path: Option<String>,
        collect_slots_for_proofs: bool,
        test_retries: u64,
        commitment_level: CommitmentLevel,
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

        if collect_slots_for_proofs && (bigtable_creds.is_none() || slots_collect_path.is_none()) {
            panic!("Invalid config: trying collect slots for proofs, but do not pass bigtable creds ({:?}) or slots collect path ({:?})", &bigtable_creds, &slots_collect_path);
        }
        let mut collect_slots_tools = None;
        if collect_slots_for_proofs {
            // Unwraps, is safe, because we check it above
            collect_slots_tools = Some(CollectSlotsTools {
                bigtable_client: Arc::new(
                    BigTableClient::connect_new_with(bigtable_creds.unwrap(), BIGTABLE_TIMEOUT)
                        .await
                        .unwrap(),
                ),
                slots_collect_path: slots_collect_path.unwrap(),
                metrics: slot_collect_metrics,
            })
        }
        let rpc_client = Arc::new(RpcClient::new(reference_host.clone()));
        let proof_checker = MaybeProofChecker::new(rpc_client.clone(), 1.0, commitment_level);
        Self {
            rpc_client,
            reference_host,
            testing_host,
            api: IntegrityVerificationApi::new(),
            keys_fetcher,
            metrics: test_metrics,
            collect_slots_tools,
            regexes,
            test_retries,
            proof_checker,
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
            let diff =
                self.regexes.iter().fold(diff, |acc, re| re.replace_all(&acc, "").to_string());
            if diff.is_empty() {
                return None;
            }

            return Some(diff);
        }

        None
    }

    async fn check_request(&self, req: &Body) -> DiffWithResponses {
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
                return DiffWithResponses::default();
            },
        };
        let testing_response = match testing_response {
            Ok(testing_response) => testing_response,
            Err(e) => {
                self.metrics.inc_network_errors_testing_host();
                error!("Testing host network error: {}", e);
                return DiffWithResponses::default();
            },
        };

        DiffWithResponses {
            diff: self.compare_responses(&reference_response, &testing_response),
            reference_response,
            testing_response,
        }
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
            let mut diff_with_responses = DiffWithResponses::default();
            for _ in 0..self.test_retries {
                diff_with_responses = self.check_request(req).await;
                if diff_with_responses.diff.is_none() {
                    break;
                }
                // Prevent rate-limit errors
                tokio::time::sleep(Duration::from_millis(REQUESTS_INTERVAL_MILLIS)).await;
            }

            let mut test_failed = false;
            if let Some(diff) = diff_with_responses.diff {
                test_failed = true;
                error!("{}: mismatch responses: req: {:#?}, diff: {}", req.method, req, diff);
                // this will never be cancelled
                self.try_collect_slots(
                    req,
                    &diff_with_responses.reference_response,
                    CancellationToken::new(),
                )
                .await;
            }

            if req.method == GET_ASSET_PROOF_METHOD {
                let asset_id = req.params["id"].as_str().unwrap_or_default();
                test_failed = match self
                    .check_proof_valid(asset_id, diff_with_responses.testing_response)
                    .await
                {
                    Ok(proof_valid) => {
                        if !proof_valid {
                            error!("Invalid proof for {} asset", asset_id)
                        };
                        !proof_valid
                    },
                    Err(e) => {
                        error!("Check proof valid: {}", e);
                        test_failed
                    },
                };
            }
            if test_failed {
                metrics_inc_failed_fn();
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(GET_ASSET_PROOF_METHOD, json!(generate_get_asset_proof_params(key)))
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e.to_string()))?;

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

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    async fn try_collect_slots(
        &self,
        req: &Body,
        reference_response: &Value,
        cancellation_token: CancellationToken,
    ) {
        let collect_tools = match &self.collect_slots_tools {
            None => return,
            Some(collect_tools) => collect_tools,
        };
        if req.method != GET_ASSET_PROOF_METHOD {
            return;
        }

        let asset_id = match req.params["id"].as_str() {
            None => {
                error!("cannot get asset id: {:?}", &req.params);
                return;
            },
            Some(asset_id) => asset_id,
        };
        let tree_id = match reference_response["result"]["tree_id"].as_str() {
            None => {
                error!("cannot get tree id: {:?}", &reference_response);
                return;
            },
            Some(tree_id) => tree_id,
        };
        let slot = match self.get_slot().await {
            Ok(slot) => slot,
            Err(e) => {
                error!("get_slot: {}", e);
                return;
            },
        };
        if let Ok(tree_id) = Pubkey::from_str(tree_id) {
            collect_tools.collect_slots(asset_id, tree_id, slot, cancellation_token).await
        }
    }

    async fn get_slot(&self) -> Result<u64, IntegrityVerificationError> {
        Ok(self.rpc_client.get_slot().await?)
    }

    async fn check_proof_valid(
        &self,
        asset_id: &str,
        response: Value,
    ) -> Result<bool, IntegrityVerificationError> {
        let tree_id = response["result"]["tree_id"]
            .as_str()
            .ok_or(IntegrityVerificationError::CannotGetResponseField("tree_id".to_string()))?;
        let leaf = Pubkey::from_str(
            response["result"]["leaf"]
                .as_str()
                .ok_or(IntegrityVerificationError::CannotGetResponseField("leaf".to_string()))?,
        )?
        .to_bytes();

        let initial_proofs = response["result"]["proof"]
            .as_array()
            .ok_or(IntegrityVerificationError::CannotGetResponseField("proof".to_string()))?
            .iter()
            .filter_map(|proof| proof.as_str().and_then(|v| Pubkey::from_str(v).ok()))
            .collect::<Vec<_>>();

        let get_asset_req = json!(&Body::new(
            GET_ASSET_METHOD,
            json!(generate_get_asset_params(asset_id.to_string()))
        ))
        .to_string();
        let get_asset = self.api.make_request(&self.reference_host, &get_asset_req).await?;
        let tree_id_pk: Pubkey = Pubkey::from_str(tree_id)?;
        let leaf_index = get_asset["result"]["compression"]["leaf_id"]
            .as_u64()
            .ok_or(IntegrityVerificationError::CannotGetResponseField("leaf_id".to_string()))?
            as u32;
        self.proof_checker.check_proof(tree_id_pk, initial_proofs, leaf_index, leaf).await
    }
}

impl CollectSlotsTools {
    async fn collect_slots(
        &self,
        asset: &str,
        tree_key: Pubkey,
        slot: u64,
        cancellation_token: CancellationToken,
    ) {
        let slots_collector = SlotsCollector::new(
            Arc::new(FileSlotsDumper::new(self.format_filename(&tree_key.to_string(), asset))),
            self.bigtable_client.big_table_inner_client.clone(),
            self.metrics.clone(),
        );

        info!("Start collecting slots for {}", tree_key);
        slots_collector.collect_slots(&tree_key, slot, 0, cancellation_token).await;
        info!("Collected slots for {}", tree_key);
    }

    fn format_filename(&self, tree_key: &str, asset: &str) -> String {
        format!("{}/{}-{}.txt", self.slots_collect_path, tree_key, asset)
    }
}

#[cfg(test)]
mod tests {
    use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use metrics_utils::{utils::start_metrics, IntegrityVerificationMetrics, MetricsTrait};
    use regex::Regex;
    use serde_json::json;
    use solana_sdk::commitment_config::CommitmentLevel;

    use crate::{diff_checker::DiffChecker, file_keys_fetcher::FileKeysFetcher};

    // this function used only inside tests under rpc_tests and bigtable_tests features, that do not running in our CI
    #[allow(dead_code)]
    async fn create_test_diff_checker() -> DiffChecker<FileKeysFetcher> {
        let mut metrics = IntegrityVerificationMetrics::new();
        metrics.register_metrics();
        start_metrics(metrics.registry, Some(6001)).await;

        DiffChecker::new(
            "https://test".to_string(),
            "".to_string(),
            FileKeysFetcher::new("./test_keys.txt").await.unwrap(),
            metrics.integrity_verification_metrics.clone(),
            metrics.slot_collector_metrics.clone(),
            Some(String::from("../../creds.json")),
            Some("./".to_string()),
            true,
            20,
            CommitmentLevel::Processed,
        )
        .await
    }

    #[cfg(feature = "rpc_tests")]
    #[tokio::test]
    async fn test_get_slot() {
        let slot = create_test_diff_checker().await.get_slot().await.unwrap();

        assert_ne!(slot, 0)
    }

    #[cfg(feature = "bigtable_tests")]
    #[tokio::test]
    async fn test_save_slots_to_file() {
        use std::str::FromStr;

        use solana_program::pubkey::Pubkey;
        use tokio_util::sync::CancellationToken;

        create_test_diff_checker()
            .await
            .collect_slots_tools
            .unwrap()
            .collect_slots(
                "BAtEs7TuGm2hP2owc9cTit2TNfVzpPFyQAAvkDWs6tDm",
                Pubkey::from_str("4FZcSBJkhPeNAkXecmKnnqHy93ABWzi3Q5u9eXkUfxVE").unwrap(),
                244259062,
                CancellationToken::new(),
            )
            .await;
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

        let re1 = Regex::new(r#"json atom at path \".*?\.token_standard\" is missing from rhs\n*"#)
            .unwrap();
        let re2 =
            Regex::new(r#"json atom at path \".*?\.cdn_uri\" is missing from rhs\n*"#).unwrap();
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

        assert_eq!("json atom at path \".result.mutable\" is missing from lhs", res.trim());
    }
}
