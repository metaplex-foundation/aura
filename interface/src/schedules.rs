use mockall::automock;

/// Interface to implemented by a storage, that will be storing job schedules
/// (which is a job config) records.
#[automock]
pub trait SchedulesStore: Send + Sync {
    /// Fetch all schedule records. There should not be a lot of them.
    fn list_schedules(&self) -> Vec<entities::schedule::ScheduledJob>;

    /// Get job schedule config record for a job with given ID.
    fn get_schedule(&self, job_id: String) -> Option<entities::schedule::ScheduledJob>;

    /// Persist job schedule config
    fn put_schedule(&self, schedule: &entities::schedule::ScheduledJob);
}
