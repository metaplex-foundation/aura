use crate::error::UsecaseError;
use async_trait::async_trait;
use mockall::automock;
use std::collections::HashSet;

#[async_trait]
#[automock]
pub trait MigrationVersionManager {
    async fn get_all_applied_migrations(&self) -> Result<HashSet<u64>, UsecaseError>;
    async fn apply_migration(&self, version: u64) -> Result<(), UsecaseError>;
}
