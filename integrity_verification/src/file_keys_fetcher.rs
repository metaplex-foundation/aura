use crate::diff_checker::{
    GET_ASSET_BY_AUTHORITY_METHOD, GET_ASSET_BY_CREATOR_METHOD, GET_ASSET_BY_GROUP_METHOD,
    GET_ASSET_BY_OWNER_METHOD, GET_ASSET_METHOD, GET_ASSET_PROOF_METHOD,
};
use async_trait::async_trait;
use interface::error::IntegrityVerificationError;
use postgre_client::error::IndexDbError;
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

/// This is used in tests only.
pub struct FileKeysFetcher {
    keys_map: HashMap<String, Vec<String>>,
}

impl FileKeysFetcher {
    /// Reads test keys file in format:
    /// ```
    /// getAsset:
    /// k1,k2,k3
    /// getAssetProof:
    /// k4,k5,k6
    ///   ...
    /// ```
    pub async fn new(file_path: &str) -> Result<Self, IntegrityVerificationError> {
        let file = File::open(file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut keys_map = HashMap::new();
        let mut current_key = None;

        while let Some(line) = lines.next_line().await? {
            if line.ends_with(':') {
                current_key = Some(line.trim_end_matches(':').to_string());
            } else if let Some(key) = &current_key {
                if !line.is_empty() {
                    let keys = line.split(',').map(String::from).collect();
                    keys_map.insert(key.clone(), keys);
                }
            }
        }

        Ok(FileKeysFetcher { keys_map })
    }
    fn read_keys(&self, method_name: &str) -> Result<Vec<String>, IndexDbError> {
        Ok(self.keys_map.get(method_name).cloned().unwrap_or_default())
    }
}
#[async_trait]
impl IntegrityVerificationKeysFetcher for FileKeysFetcher {
    async fn get_verification_required_owners_keys(&self) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_BY_OWNER_METHOD)
    }

    async fn get_verification_required_creators_keys(&self) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_BY_CREATOR_METHOD)
    }

    async fn get_verification_required_authorities_keys(
        &self,
    ) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_BY_AUTHORITY_METHOD)
    }

    async fn get_verification_required_groups_keys(&self) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_BY_GROUP_METHOD)
    }

    async fn get_verification_required_assets_keys(&self) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_METHOD)
    }

    async fn get_verification_required_assets_proof_keys(
        &self,
    ) -> Result<Vec<String>, IndexDbError> {
        self.read_keys(GET_ASSET_PROOF_METHOD)
    }
}
