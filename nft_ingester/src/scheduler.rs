use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use entities::models::OffChainData;
use rocks_db::asset_previews::UrlToDownload;
use tracing::log::error;

use rocks_db::Storage;

use async_trait::async_trait;
use entities::schedule::JobRunState;
use entities::schedule::ScheduledJob;
use interface::schedules::SchedulesStore;

use crate::api::dapi::rpc_asset_convertors::parse_files;

/// Represents a functionality for running background jobs according
/// to a confgurations stored in DB.
/// Jobs can be one-time jobs, or periodically running jobs.
///
/// The main feature of this scheduler is that if a jobs is a long
/// running task, it should do the work in iterative way.
/// And after each iteration, the intermediate state is persisted
/// to the DB (makes check point).
/// That's why if the application crushes before the job
/// has finished the work, it will continue execution from
/// the previous "checkpoint".
pub struct Scheduler {
    storage: Arc<Storage>,
    jobs: Vec<Box<dyn Job + Send>>,
}

impl Scheduler {
    pub fn new(storage: Arc<Storage>) -> Scheduler {
        // Here we defined all "scheduled" jobs
        let jobs: Vec<Box<dyn Job + Send>> = vec![Box::new(InitUrlsToDownloadJob {
            storage: storage.clone(),
            batch_size: 1000,
            last_key: None,
        })];

        Scheduler { storage, jobs }
    }

    pub async fn run_in_background(mut scheduler: Scheduler) {
        tokio::spawn(async move {
            scheduler.run().await;
        });
    }

    /// Execution loop of the scheduler.
    /// It executes jobs one by one sequentially.
    pub async fn run(&mut self) {
        loop {
            let mut sleep_to_next_run = u64::max_value();
            let mut to_remove = Vec::new();
            for job in self.jobs.iter_mut() {
                let mut sched = match self.storage.get_schedule(job.id()) {
                    Some(prev_sched) => prev_sched,
                    None => {
                        let initial_config = job.initial_config();
                        self.storage.put_schedule(&initial_config);
                        initial_config
                    }
                };
                if sched.wont_run_again() {
                    to_remove.push(job.id());
                } else if sched.need_to_run_now() {
                    self.storage.mark_started(&mut sched);
                    loop {
                        match job.run().await {
                            JobRunResult::Finished(state) => {
                                self.storage.mark_finished(&mut sched, state);
                                if sched.wont_run_again() {
                                    to_remove.push(job.id());
                                } else if let Some(run_interval_sec) = sched.run_interval_sec {
                                    sleep_to_next_run = std::cmp::min(
                                        sleep_to_next_run,
                                        sched.last_run_epoch_time + run_interval_sec,
                                    );
                                }
                                break;
                            }
                            JobRunResult::NotFinished(state) => {
                                self.storage.update_state(&mut sched, state);
                            }
                            JobRunResult::Error(state_op) => {
                                self.storage.mark_failed(&mut sched, state_op);
                                to_remove.push(job.id());
                                break;
                            }
                        };
                    }
                }
            }
            self.jobs.retain(|j| to_remove.contains(&j.id()));
            if self.jobs.is_empty() || sleep_to_next_run == u64::max_value() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(sleep_to_next_run)).await;
        }
    }
}

/// Helper functions for transitioning job from one state to another.
trait SchedulesStoreEx: SchedulesStore {
    fn mark_started(&self, sched: &mut ScheduledJob) {
        sched.last_run_status = JobRunState::Started;
        sched.update_with_current_time();
        self.put_schedule(sched);
    }
    fn mark_finished(&self, sched: &mut ScheduledJob, new_state: Option<Vec<u8>>) {
        sched.state = new_state;
        sched.update_with_current_time();
        sched.last_run_status = JobRunState::Finished;
        self.put_schedule(sched);
    }
    fn mark_failed(&self, sched: &mut ScheduledJob, new_state: Option<Option<Vec<u8>>>) {
        if let Some(state) = new_state {
            sched.state = state;
        }
        sched.update_with_current_time();
        sched.last_run_status = JobRunState::Failed;
        self.put_schedule(sched);
    }
    fn update_state(&self, sched: &mut ScheduledJob, new_state: Option<Vec<u8>>) {
        sched.state = new_state;
        sched.update_with_current_time();
        self.put_schedule(sched);
    }
}

impl<T> SchedulesStoreEx for T where T: SchedulesStore {}

/// Result of a job interation. Each iteration returns an updates state for the job.
pub enum JobRunResult {
    Finished(Option<Vec<u8>>),
    NotFinished(Option<Vec<u8>>),
    Error(Option<Option<Vec<u8>>>),
}

/// Interface of a job that can be runned by the scheduler.
#[async_trait]
pub trait Job {
    /// ID of the job that is used to bind the job itself with the persisted job state in DB.
    fn id(&self) -> String;

    /// If the job is newly added, i.e. doesn't have a state in DB yet,
    /// this method used to create an inital stated for the job.
    fn initial_config(&self) -> ScheduledJob;

    /// Used to init job state before running, with a state fetched from the DB.
    fn init_with_state(&mut self, prev_state: Option<Vec<u8>>);

    /// Executes next iteration of the job.
    async fn run(&mut self) -> JobRunResult;
}

/// One-shot job that is responsible for collecting all the NFT asset URLs from
/// the offchain data column family and pushing these URLs in
/// the URLs to download column family.
/// In the normal flow, a URL is pushed to "URLs to download" immediately
/// when the new NFT is ingested by the nft_injecter. But since
/// "URLs to download" column family has been created after the offchain data,
/// there is a need to backfill already existing records.
/// And this job does this backfilling.
pub struct InitUrlsToDownloadJob {
    storage: Arc<Storage>,
    /// how much offchain data records we process into URLs to donwload in a single run
    batch_size: usize,
    /// a key of the last offchain data record processed in the previous run
    last_key: Option<String>,
}

#[async_trait]
impl Job for InitUrlsToDownloadJob {
    fn id(&self) -> String {
        "init_urls_to_download".to_string()
    }

    fn initial_config(&self) -> ScheduledJob {
        ScheduledJob {
            job_id: self.id(),
            run_interval_sec: None, // one-time job
            last_run_epoch_time: 0,
            last_run_status: JobRunState::NotRun,
            state: None,
        }
    }

    fn init_with_state(&mut self, prev_state: Option<Vec<u8>>) {
        self.last_key = prev_state.and_then(|bytes| bincode::deserialize::<String>(&bytes).ok());
    }

    async fn run(&mut self) -> JobRunResult {
        let data = match &self.last_key {
            Some(v) => self
                .storage
                .asset_offchain_data
                .get_after(v.clone(), self.batch_size),
            None => self
                .storage
                .asset_offchain_data
                .get_from_start(self.batch_size),
        };

        if data.is_empty() {
            return JobRunResult::Finished(None);
        }

        self.last_key = Some(data.last().unwrap().0.clone()); // .unwrap() won't ever fail

        let urls: HashMap<String, UrlToDownload> = data
            .into_iter()
            .filter_map(|(_, OffChainData { url: _, metadata })| parse_files(&metadata))
            .flat_map(|files| files.into_iter().filter_map(|f| f.uri))
            .map(|uri| {
                (
                    uri,
                    UrlToDownload {
                        timestamp: 0,
                        download_attempts: 0,
                    },
                )
            })
            .collect();

        if let Err(e) = self.storage.urls_to_download.put_batch(urls).await {
            error!("Error writing urls_to_download: {e}");
            return JobRunResult::Error(None);
        };

        // If somehow the rocks key cannot be serialized, then it's probably better to crash?
        JobRunResult::NotFinished(
            self.last_key
                .clone()
                .map(|s| bincode::serialize(&s).unwrap()),
        )
    }
}
