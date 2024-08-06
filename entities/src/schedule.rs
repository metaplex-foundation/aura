use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Represents information about backgound job that can be one time job,
/// or a scheduled job that is launched recurrently with a given interval.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScheduledJob {
    pub job_id: String,

    /// None - for one time jobs
    pub run_interval_sec: Option<u64>,

    /// Timestamp of last job run/change in epoc time (seconds).
    pub last_run_epoch_time: u64,

    /// The last state of the job.
    pub last_run_status: JobRunState,

    /// A state that is transfered between job runs and mann
    pub state: Option<Vec<u8>>,
}

impl ScheduledJob {
    /// Update last run timestamp with the current time.
    pub fn update_with_current_time(&mut self) {
        self.last_run_epoch_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Checks if it is already time to run the job.
    pub fn need_to_run_now(&self) -> bool {
        if let JobRunState::NotRun = self.last_run_status {
            return true;
        };

        self.run_interval_sec
            .map(|sec_interval| {
                let since_the_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                since_the_epoch > self.last_run_epoch_time + sec_interval
            })
            .unwrap_or(false)
    }

    /// Check if the given schedule belongs to a one-time job, that has been finished already.
    pub fn wont_run_again(&self) -> bool {
        matches!(self.last_run_status, JobRunState::Finished) && self.run_interval_sec.is_none()
    }
}

/// Status of a job, the schedule belongs to
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobRunState {
    NotRun,
    Started,
    Finished,
    Failed,
}
