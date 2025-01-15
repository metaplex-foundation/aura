use entities::schedule::ScheduledJob;
use interface::schedules::SchedulesStore;
use tracing::warn;

use crate::{
    column::TypedColumn,
    key_encoders::{decode_string, encode_string},
    Storage,
};

impl TypedColumn for ScheduledJob {
    type KeyType = String;

    type ValueType = Self;

    const NAME: &'static str = "Schedules";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        encode_string(index)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        decode_string(bytes)
    }
}

impl SchedulesStore for Storage {
    fn list_schedules(&self) -> Vec<entities::schedule::ScheduledJob> {
        let result = self
            .schedules
            .iter_start()
            .filter_map(|a| a.ok())
            .filter_map(|(_key_bytes, value_bytes)| {
                bincode::deserialize::<entities::schedule::ScheduledJob>(value_bytes.as_ref()).ok()
            })
            .collect::<Vec<_>>();

        result
    }

    fn get_schedule(&self, schedule_id: String) -> Option<entities::schedule::ScheduledJob> {
        match self.schedules.get(schedule_id) {
            Ok(r) => r,
            Err(e) => {
                warn!("{}", e);
                None
            },
        }
    }

    fn put_schedule(&self, entity: &entities::schedule::ScheduledJob) {
        if let Err(e) = self.schedules.put(entity.job_id.clone(), entity.clone()) {
            warn!("{}", e);
        }
    }
}
