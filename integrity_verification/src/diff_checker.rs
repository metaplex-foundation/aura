use crate::api::IntegrityVerificationApi;
use crate::error::IntegrityVerificationError;
use crate::params::generate_get_asset_params;
use crate::requests::Body;
use metrics_utils::IntegrityVerificationMetricsConfig;
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

const GET_ASSET_METHOD: &str = "getAsset";
const GET_ASSET_PROOF_METHOD: &str = "getAssetProof";
const GET_ASSET_BY_OWNER_METHOD: &str = "getAssetsByOwner";
const GET_ASSET_BY_AUTHORITY_METHOD: &str = "getAssetsByAuthority";
const GET_ASSET_BY_GROUP_METHOD: &str = "getAssetsByGroup";
const GET_ASSET_BY_CREATOR_METHOD: &str = "getAssetsByCreator";

pub struct DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher,
{
    pub reference_host: String,
    pub testing_host: String,
    pub api: IntegrityVerificationApi,
    pub keys_fetcher: T,
    pub metrics: Arc<IntegrityVerificationMetricsConfig>,
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher,
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
    T: IntegrityVerificationKeysFetcher,
{
    pub fn compare_responses(
        &self,
        reference_response: &Value,
        testing_response: &Value,
    ) -> Result<Option<String>, IntegrityVerificationError> {
        // TODO
        Ok(None)
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

        for req in requests.iter() {
            self.metrics.inc_total_get_asset_tested();

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

            if let Some(diff) = self.compare_responses(&reference_response, &testing_response)? {
                self.metrics.inc_failed_get_asset_tested();
                error!(
                    "{}: mismatch responses: req: {:#?}, diff: {}",
                    GET_ASSET_METHOD, req, diff
                );
            }
        }

        Ok(())
    }
}
