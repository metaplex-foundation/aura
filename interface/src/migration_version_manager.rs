use mockall::automock;
use std::collections::HashSet;

#[automock]
pub trait PrimaryStorageMigrationVersionManager {
    fn get_all_applied_migrations(&self) -> Result<HashSet<u64>, String>;
}
