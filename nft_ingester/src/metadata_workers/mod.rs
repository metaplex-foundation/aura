pub mod downloader;
pub mod json_worker;
pub mod persister;
pub mod streamer;

pub enum TaskType {
    Pending,
    Refresh,
}
