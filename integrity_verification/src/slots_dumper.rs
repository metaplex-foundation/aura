use async_trait::async_trait;
use interface::slots_dumper::SlotsDumper;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub struct FileSlotsDumper {
    path: String,
}

impl FileSlotsDumper {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

// TODO: rm unwraps
#[async_trait]
impl SlotsDumper for FileSlotsDumper {
    async fn dump_slots(&self, slots: &[u64]) {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .await
            .unwrap();

        for &slot in slots.iter() {
            file.write_all(slot.to_string().as_bytes()).await.unwrap();
            file.write_all(b", ").await.unwrap()
        }
    }
}
