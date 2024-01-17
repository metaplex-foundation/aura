use crate::error::IntegrityVerificationError;
use crate::params::generate_get_assets_by_owner_params;
use crate::requests::Body;
use reqwest::Client;
use serde_json::{json, Value};

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
}

#[tokio::test]
async fn test_api() {
    let api = IntegrityVerificationApi::new();
    let body = json!(Body::new(
        "getAssetsByOwner",
        json!(generate_get_assets_by_owner_params(
            "m7QZ3fVbBYSDfgjFhZDDSHK7VAPYa6xwUu34x3RU4mY".to_string(),
            None,
            None
        )),
    ));

    println!("{:#?}", &body);

    assert_eq!(
        Value::Null,
        api.make_request(
            "https://mainnet.helius-rpc.com/?api-key=0800bcca-7d4d-4a0a-800a-5aa9e1dc855f",
            &body.to_string()
        )
        .await
        .unwrap()
    );
}
