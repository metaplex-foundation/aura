pub mod downloader;
pub mod json_worker;
pub mod persister;
pub mod streamer;

pub enum TaskType {
    /// Used for new metadata that needs to be processed for the first time
    Pending,
    /// Used for existing metadata that needs to be updated
    Refresh,
}
