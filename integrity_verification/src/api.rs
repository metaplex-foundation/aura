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
}

#[tokio::test]
async fn test_api() {
    use crate::params::generate_get_assets_by_owner_params;
    use crate::requests::Body;
    use serde_json::{json, Value};

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
