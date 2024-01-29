use async_trait::async_trait;

#[async_trait]
pub trait SlotsDumper {
    async fn dump_slots(&self, slots: &[u64]);
}
