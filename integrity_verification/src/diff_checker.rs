use crate::api::IntegrityVerificationApi;
use crate::error::IntegrityVerificationError;
use crate::params::{
    generate_get_asset_params, generate_get_asset_proof_params,
    generate_get_assets_by_authority_params, generate_get_assets_by_creator_params,
    generate_get_assets_by_group_params, generate_get_assets_by_owner_params,
};
use crate::requests::Body;
use metrics_utils::IntegrityVerificationMetricsConfig;
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
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

const REQUESTS_INTERVAL_MILLIS: u64 = 500;

pub struct DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    pub reference_host: String,
    pub testing_host: String,
    pub api: IntegrityVerificationApi,
    pub keys_fetcher: T,
    pub metrics: Arc<IntegrityVerificationMetricsConfig>,
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher + Send + Sync,
{
    pub fn new(
        reference_host: String,
        tested_host: String,
        keys_fetcher: T,
        metrics: Arc<IntegrityVerificationMetricsConfig>,
    ) -> Self {
        Self {
            reference_host,
            testing_host: tested_host,
            api: IntegrityVerificationApi::new(),
            keys_fetcher,
            metrics,
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
    ) -> Result<Option<String>, IntegrityVerificationError> {
        // TODO
        Ok(None)
    }

    async fn test_requests<F, G>(
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

            if let Some(diff) = self
                .compare_responses(&reference_response, &testing_response)
                .unwrap()
            {
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| Body::new(GET_ASSET_METHOD, json!(generate_get_asset_params(key))))
            .collect::<Vec<_>>();

        self.test_requests(
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_PROOF_METHOD,
                    json!(generate_get_asset_proof_params(key)),
                )
            })
            .collect::<Vec<_>>();

        self.test_requests(
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_AUTHORITY_METHOD,
                    json!(generate_get_assets_by_authority_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.test_requests(
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_OWNER_METHOD,
                    json!(generate_get_assets_by_owner_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.test_requests(
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
            .get_verification_required_collections_keys()
            .await
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_BY_GROUP_METHOD,
                    json!(generate_get_assets_by_group_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.test_requests(
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
            .map_err(|e| IntegrityVerificationError::FetchKeys(e))?;

        let requests = verification_required_keys
            .into_iter()
            .map(|key| {
                Body::new(
                    GET_ASSET_PROOF_METHOD,
                    json!(generate_get_assets_by_creator_params(key, None, None)),
                )
            })
            .collect::<Vec<_>>();

        self.test_requests(
            requests,
            || self.metrics.inc_total_get_assets_by_creator_tested(),
            || self.metrics.inc_failed_get_assets_by_creator_tested(),
        )
        .await;

        Ok(())
    }
}
