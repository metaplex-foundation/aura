use async_trait::async_trait;
use interface::slots_dumper::SlotsDumper;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::error;

pub struct FileSlotsDumper {
    path: String,
}

impl FileSlotsDumper {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait]
impl SlotsDumper for FileSlotsDumper {
    async fn dump_slots(&self, slots: &[u64]) {
        let mut file =
            match OpenOptions::new().write(true).append(true).create(true).open(&self.path).await {
                Ok(file) => file,
                Err(e) => {
                    error!("Cannot open or create file: {}, error: {}", &self.path, e);
                    return;
                },
            };

        for &slot in slots.iter() {
            if let Err(e) = file.write_all(slot.to_string().as_bytes()).await {
                error!("Cannot write to file: {}, error: {}", &self.path, e);
            };
            if let Err(e) = file.write_all(b", ").await {
                error!("Cannot write to file: {}, error: {}", &self.path, e);
            };
        }
    }
}
