use interface::error::IntegrityVerificationError;
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
}

#[cfg(feature = "rpc_tests")]
#[tokio::test]
async fn test_api() {
    use crate::diff_checker::GET_ASSET_METHOD;
    use crate::params::generate_get_asset_params;
    use crate::requests::Body;
    use serde_json::{json, Value};

    let api = IntegrityVerificationApi::new();
    let body = json!(Body::new(
        GET_ASSET_METHOD,
        json!(generate_get_asset_params(
            "JCoRmqZf2Q9ftb4a81aD6XswpFgQHPGXvsHAmgvZ54M1".to_string()
        )),
    ));

    assert_ne!(
        Value::Null,
        api.make_request("http://test_url", &body.to_string())
            .await
            .unwrap()
    );
}
