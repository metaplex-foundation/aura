pub mod pg;
pub mod rocks;

use metrics_utils::MetricsTrait;
use testcontainers::clients::Cli;

pub struct TestEnvironment<'a> {
    pub rocks_env: rocks::RocksTestEnvironment,
    pub pg_env: pg::TestEnvironment<'a>,
}
const BATCH_SIZE: usize = 200_000;
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
            "".to_string(),
            metrics_state.synchronizer_metrics.clone(),
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        syncronizer.synchronize_asset_indexes(&rx).await.unwrap();
        (env, generated_data)
    }

    pub async fn teardown(&self) {
        self.pg_env.teardown().await;
    }
}
