use crate::api::IntegrityVerificationApi;
use crate::error::IntegrityVerificationError;
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;

pub struct DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher,
{
    pub reference_host: String,
    pub tested_host: String,
    pub api: IntegrityVerificationApi,
    pub keys_fetcher: T,
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher,
{
    pub fn new(reference_host: String, tested_host: String, keys_fetcher: T) -> Self {
        Self {
            reference_host,
            tested_host,
            api: IntegrityVerificationApi::new(),
            keys_fetcher,
        }
    }
}

impl<T> DiffChecker<T>
where
    T: IntegrityVerificationKeysFetcher,
{
    pub async fn f(&self, request: &str) -> Result<(), IntegrityVerificationError> {
        let reference_response = self.api.make_request(&self.reference_host, request).await?;
        let tested_response = self.api.make_request(&self.tested_host, request).await?;

        Ok(())
    }
}
