pub mod pg;
pub mod rocks;

use metrics_utils::MetricsTrait;
use std::sync::{atomic::AtomicBool, Arc};
use testcontainers::clients::Cli;

pub struct TestEnvironment<'a> {
    pub rocks_env: rocks::RocksTestEnvironment,
    pub pg_env: pg::TestEnvironment<'a>,
}
const BATCH_SIZE: usize = 1000;
impl<'a> TestEnvironment<'a> {
    pub async fn create(
        cli: &'a Cli,
        cnt: usize,
        slot: u64,
    ) -> (TestEnvironment<'a>, rocks::GeneratedAssets) {
        let rocks_env = rocks::RocksTestEnvironment::new(&[]);
        let pg_env = pg::TestEnvironment::new(cli).await;
        let generated_data = rocks_env.generate_assets(cnt, slot);
        let env = Self { rocks_env, pg_env };

        let mut metrics_state = metrics_utils::MetricState::new();
        metrics_state.register_metrics();

        let syncronizer = nft_ingester::index_syncronizer::Synchronizer::new(
            env.rocks_env.storage.clone(),
            env.pg_env.client.clone(),
            BATCH_SIZE,
            200_000,
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
        );
        syncronizer
            .synchronize_asset_indexes(Arc::new(AtomicBool::new(true)))
            .await
            .unwrap();
        (env, generated_data)
    }

    pub async fn teardown(self) {
        self.pg_env.teardown().await;
    }
}
