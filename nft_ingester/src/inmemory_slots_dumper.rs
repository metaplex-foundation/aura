use async_trait::async_trait;
use interface::slots_dumper::SlotsDumper;
use std::collections::BTreeSet;
use tokio::sync::Mutex;

pub struct InMemorySlotsDumper {
    slots: Mutex<BTreeSet<u64>>,
}
impl Default for InMemorySlotsDumper {
    fn default() -> Self {
        Self::new()
    }
}
impl InMemorySlotsDumper {
    /// Creates a new instance of `InMemorySlotsDumper`.
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(BTreeSet::new()),
        }
    }

    /// Retrieves the sorted keys in ascending order.
    pub async fn get_sorted_keys(&self) -> Vec<u64> {
        let slots = self.slots.lock().await;
        slots.iter().cloned().collect()
    }

    /// Clears the internal storage to reuse it.
    pub async fn clear(&self) {
        let mut slots = self.slots.lock().await;
        slots.clear();
    }
}

#[async_trait]
impl SlotsDumper for InMemorySlotsDumper {
    async fn dump_slots(&self, slots: &[u64]) {
        let mut storage = self.slots.lock().await;
        for &slot in slots {
            storage.insert(slot);
        }
    }
}
