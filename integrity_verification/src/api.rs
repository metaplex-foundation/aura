use crate::error::IntegrityVerificationError;
use reqwest::Client;

#[derive(Debug)]
pub struct IntegrityVerificationApi {
    client: Client,
}

impl IntegrityVerificationApi {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    pub async fn make_request(
        &self,
        url: &str,
        body: &str,
    ) -> Result<serde_json::Value, IntegrityVerificationError> {
        let resp = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .body(body.to_owned())
            .send()
            .await?
            .text()
            .await?;

        Ok(serde_json::from_str(resp.as_str())?)
    }

    // async fn diff(&self, body: &str, title: &str) -> Result<(), TestApiError> {
    //     let actual_response = self.request(self.config.compare_url.as_str(), body).await?;
    //     let expected_response = self.request(self.config.url.as_str(), body).await?;
    //
    //     let res = assert_json_matches_no_panic(
    //         &actual_response,
    //         &expected_response,
    //         DiffConfig::new(CompareMode::Strict),
    //     );
    //     self.assert_output_eq(res, Ok(()), title);
    //
    //     Ok(())
    // }

    // fn assert_output_eq(
    //     &self,
    //     actual: Result<(), String>,
    //     expected: Result<(), &str>,
    //     title: &str,
    // ) {
    //     match (actual, expected) {
    //         (Ok(()), Ok(())) => {
    //             info!("No diff for: {}\n", title);
    //         }
    //         (Err(actual_error), Ok(())) => {
    //             warn!("Diff for: {}\n", title);
    //             warn!("{}", actual_error);
    //         }
    //         _ => {
    //             warn!("Unexpected diff");
    //         }
    //     }
    // }

    // pub async fn test_get_asset(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.assets_id {
    //         let body = Body::new("getAsset", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             body.as_str(),
    //             format!("Asset ID: {}", params.id.clone().unwrap_or_default()).as_str(),
    //         )
    //             .await?;
    //     }
    //
    //     Ok(())
    // }
    //
    // pub async fn test_get_asset_proof(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.assets_proof_id {
    //         let body = Body::new("getAssetProof", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             &body,
    //             format!("Asset proof ID: {}", params.id.clone().unwrap_or_default()).as_str(),
    //         )
    //             .await?;
    //     }
    //
    //     Ok(())
    // }
    //
    // pub async fn test_get_asset_by_group(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.asset_by_group {
    //         let body = Body::new("getAssetsByGroup", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             &body,
    //             format!(
    //                 "Asset by group - groupKey: {}, groupValue: {}",
    //                 params.group_key.clone().unwrap_or_default(),
    //                 params.group_value.clone().unwrap_or_default()
    //             )
    //                 .as_str(),
    //         )
    //             .await?
    //     }
    //
    //     Ok(())
    // }
    //
    // pub async fn test_get_asset_by_owner(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.asset_by_owner {
    //         let body = Body::new("getAssetsByOwner", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             &body,
    //             format!(
    //                 "Asset by owner - owner address: {}",
    //                 params.owner_address.clone().unwrap_or_default()
    //             )
    //                 .as_str(),
    //         )
    //             .await?;
    //     }
    //
    //     Ok(())
    // }
    //
    // pub async fn test_get_asset_by_creator(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.asset_by_creator {
    //         let body = Body::new("getAssetsByCreator", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             &body,
    //             format!(
    //                 "Asset by creator - creator address: {}",
    //                 params.creator_address.clone().unwrap_or_default()
    //             )
    //                 .as_str(),
    //         )
    //             .await?;
    //     }
    //
    //     Ok(())
    // }
    //
    // pub async fn test_get_assets_by_authority(&self) -> Result<(), TestApiError> {
    //     for params in &self.cases.asset_by_authority {
    //         let body = Body::new("getAssetsByAuthority", params);
    //         let body = serde_json::to_string(&body)?;
    //
    //         self.diff(
    //             &body,
    //             format!(
    //                 "Asset by authority - authority address: {}",
    //                 params.authority_address.clone().unwrap_or_default()
    //             )
    //                 .as_str(),
    //         )
    //             .await?;
    //     }
    //
    //     Ok(())
    // }
}
