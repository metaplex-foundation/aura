use crate::api::IntegrityVerificationApi;
use crate::error::IntegrityVerificationError;

pub struct DiffChecker {
    pub reference_host: String,
    pub tested_host: String,
    pub api: IntegrityVerificationApi,
}

impl DiffChecker {
    pub fn new(reference_host: String, tested_host: String) -> Self {
        Self {
            reference_host,
            tested_host,
            api: IntegrityVerificationApi::new(),
        }
    }
}

impl DiffChecker {
    pub async fn f(&self, request: &str) -> Result<(), IntegrityVerificationError> {
        let reference_response = self.api.make_request(&self.reference_host, request).await?;
        let tested_response = self.api.make_request(&self.tested_host, request).await?;

        Ok(())
    }
}
