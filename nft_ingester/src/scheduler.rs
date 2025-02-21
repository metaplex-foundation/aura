use crate::api::dapi::rpc_asset_convertors::parse_files;
use crate::consts::RAYDIUM_API_HOST;
use crate::error::IngesterError;
use crate::raydium_price_fetcher::{RaydiumTokenPriceFetcher, CACHE_TTL};
use async_trait::async_trait;
use entities::enums::SpecificationAssetClass;
use entities::schedule::{JobRunState, ScheduledJob};
use interface::schedules::SchedulesStore;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::columns::asset::{AssetCompleteDetails, AssetStaticDetails};
use rocks_db::{
    columns::{asset_previews::UrlToDownload, offchain_data::OffChainData},
    Storage,
};
use solana_program::pubkey::Pubkey;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::log::error;
use tracing::{info, warn};

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
    pub fn new(rocks_storage: Arc<Storage>, well_known_fungible_accounts: Option<Vec<String>>) -> Scheduler {

        // Here we defined all "scheduled" jobs
        let jobs: Vec<Box<dyn Job + Send>> = vec![
            Box::new(InitUrlsToDownloadJob {
            storage: rocks_storage.clone(),
            batch_size: 1000,
            last_key: None,
        }),
            Box::new(UpdateFungibleTokenTypeJob {
                rocks_storage: rocks_storage.clone(),
                well_known_fungible_accounts,
            })
        ];

        Scheduler { storage: rocks_storage, jobs }
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
            let mut sleep_to_next_run = u64::MAX;
            let mut to_remove = Vec::new();
            for job in self.jobs.iter_mut() {
                let mut sched = match self.storage.get_schedule(job.id()) {
                    Some(prev_sched) => prev_sched,
                    None => {
                        let initial_config = job.initial_config();
                        self.storage.put_schedule(&initial_config);
                        initial_config
                    },
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
                            },
                            JobRunResult::NotFinished(state) => {
                                self.storage.update_state(&mut sched, state);
                            },
                            JobRunResult::Error(state_op) => {
                                self.storage.mark_failed(&mut sched, state_op);
                                to_remove.push(job.id());
                                break;
                            },
                        };
                    }
                }
            }
            self.jobs.retain(|j| to_remove.contains(&j.id()));
            if self.jobs.is_empty() || sleep_to_next_run == u64::MAX {
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
        info!("Job InitUrlsToDownloadJob started.");

        let data = match &self.last_key {
            Some(v) => self.storage.asset_offchain_data.get_after(v.clone(), self.batch_size),
            None => self.storage.asset_offchain_data.get_from_start(self.batch_size),
        };

        if data.is_empty() {
            return JobRunResult::Finished(None);
        }

        self.last_key = Some(data.last().unwrap().0.clone()); // .unwrap() won't ever fail

        let urls: HashMap<String, UrlToDownload> = data
            .into_iter()
            .filter_map(|(_, OffChainData { metadata, .. })| {
                metadata.as_deref().and_then(parse_files)
            })
            .flat_map(|files| files.into_iter().filter_map(|f| f.uri))
            .map(|uri| (uri, UrlToDownload { timestamp: 0, download_attempts: 0 }))
            .collect();

        if let Err(e) = self.storage.urls_to_download.put_batch(urls).await {
            error!("Error writing urls_to_download: {e}");
            return JobRunResult::Error(None);
        };

        // If somehow the rocks key cannot be serialized, then it's probably better to crash?
        JobRunResult::NotFinished(self.last_key.clone().map(|s| bincode::serialize(&s).unwrap()))
    }
}


pub struct UpdateFungibleTokenTypeJob {
    rocks_storage: Arc<Storage>,
    well_known_fungible_accounts: Option<Vec<String>>,
}

#[async_trait]
impl Job for UpdateFungibleTokenTypeJob {
    fn id(&self) -> String {
        "update_well_known_fungible_token_types".to_string()
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

    fn init_with_state(&mut self, prev_state: Option<Vec<u8>>) {}

    async fn run(&mut self) -> JobRunResult {
        info!("Job UpdateFungibleTokenTypeJob started.");

        update_fungible_token_static_details(&self.rocks_storage, self.well_known_fungible_accounts.clone().unwrap())
            .expect("Error updating fungible assets.");

        JobRunResult::Finished(None)
    }
}


pub fn update_fungible_token_static_details(
    rocks_storage: &Arc<Storage>,
    well_known_fungible_accounts: Vec<String>,
) -> Result<(), IngesterError> {
    let mut batch = rocksdb::WriteBatch::default();

    for key in well_known_fungible_accounts {
        let pk = Pubkey::from_str(&key)?;
        let asset_complete_data = rocks_storage.get_complete_asset_details(pk);

        if let Ok(Some(asset_complete_data)) = asset_complete_data {
            if let Some(static_details) = &asset_complete_data.static_details {
                if static_details.specification_asset_class != SpecificationAssetClass::FungibleToken {
                    let new_static_details = Some(AssetStaticDetails {
                        specification_asset_class: SpecificationAssetClass::FungibleToken,
                        ..static_details.clone()
                    });
                    let new_asset_complete_data = AssetCompleteDetails {
                        static_details: new_static_details.clone(),
                        ..asset_complete_data.clone()
                    };

                    rocks_storage.merge_compete_details_with_batch(&mut batch, &new_asset_complete_data)?;
                }
            }
        }
    }

    rocks_storage.db.write(std::mem::take(&mut batch)).expect("Error writing to rocksdb.");
    rocks_storage.db.flush().expect("Error flushing rocksdb.");

    Ok(())
}
