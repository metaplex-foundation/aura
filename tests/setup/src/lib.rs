pub mod awaitility;
pub mod pg;
pub mod rocks;

use std::sync::Arc;

use entities::enums::{AssetType, SpecificationAssetClass, ASSET_TYPES};
use metrics_utils::MetricsTrait;
use rocks_db::columns::asset::{AssetAuthority, AssetCollection, AssetDynamicDetails, AssetOwner};
use solana_sdk::pubkey::Pubkey;
use testcontainers::clients::Cli;
use tokio::task::JoinSet;

use crate::rocks::RocksTestEnvironmentSetup;

pub struct TestEnvironment<'a> {
    pub rocks_env: rocks::RocksTestEnvironment,
    pub pg_env: pg::TestEnvironment<'a>,
}

impl<'a> TestEnvironment<'a> {
    pub async fn create(
        cli: &'a Cli,
        cnt: usize,
        slot: u64,
    ) -> (TestEnvironment<'a>, rocks::GeneratedAssets) {
        Self::create_and_setup_from_closures(
            cli,
            cnt,
            slot,
            &[SpecificationAssetClass::Nft],
            RocksTestEnvironmentSetup::with_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_without_authority,
        )
        .await
    }

    pub async fn create_noise(
        cli: &'a Cli,
        cnt: usize,
        slot: u64,
    ) -> (TestEnvironment<'a>, rocks::GeneratedAssets) {
        Self::create_and_setup_from_closures(
            cli,
            cnt,
            slot,
            &[
                SpecificationAssetClass::Unknown,
                SpecificationAssetClass::ProgrammableNft,
                SpecificationAssetClass::Nft,
                SpecificationAssetClass::FungibleAsset,
                SpecificationAssetClass::FungibleToken,
                SpecificationAssetClass::MplCoreCollection,
                SpecificationAssetClass::MplCoreAsset,
            ],
            RocksTestEnvironmentSetup::with_authority,
            RocksTestEnvironmentSetup::test_owner,
            RocksTestEnvironmentSetup::dynamic_data,
            RocksTestEnvironmentSetup::collection_without_authority,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_and_setup_from_closures(
        cli: &'a Cli,
        cnt: usize,
        slot: u64,
        spec_asset_class_list: &[SpecificationAssetClass],
        authorities: fn(&[Pubkey]) -> Vec<AssetAuthority>,
        owners: fn(&[Pubkey]) -> Vec<AssetOwner>,
        dynamic_details: fn(&[Pubkey], u64) -> Vec<AssetDynamicDetails>,
        collections: fn(&[Pubkey]) -> Vec<AssetCollection>,
    ) -> (TestEnvironment<'a>, rocks::GeneratedAssets) {
        let rocks_env = rocks::RocksTestEnvironment::new(&[]);
        let pg_env = pg::TestEnvironment::new(cli).await;

        let generated_data = rocks_env
            .generate_from_closure(
                cnt,
                slot,
                spec_asset_class_list,
                authorities,
                owners,
                dynamic_details,
                collections,
            )
            .await;

        let env = Self { rocks_env, pg_env };

        let mut metrics_state = metrics_utils::MetricState::new();
        metrics_state.register_metrics();

        let syncronizer = nft_ingester::index_syncronizer::Synchronizer::new(
            env.rocks_env.storage.clone(),
            env.pg_env.client.clone(),
            200000,
            "/tmp/sync_dump".to_string(),
            metrics_state.synchronizer_metrics.clone(),
            1,
        );
        let (_, rx) = tokio::sync::broadcast::channel::<()>(1);
        let synchronizer = Arc::new(syncronizer);

        let mut tasks = JoinSet::new();
        for asset_type in ASSET_TYPES {
            let synchronizer = synchronizer.clone();
            let rx = rx.resubscribe();
            tasks.spawn(async move {
                match asset_type {
                    AssetType::NonFungible => {
                        synchronizer.synchronize_nft_asset_indexes(&rx, 0).await.unwrap()
                    },
                    AssetType::Fungible => {
                        synchronizer.synchronize_fungible_asset_indexes(&rx, 0).await.unwrap()
                    },
                }
            });
        }

        while let Some(res) = tasks.join_next().await {
            if let Err(err) = res {
                panic!("{err}");
            }
        }

        (env, generated_data)
    }

    pub async fn teardown(&self) {
        self.pg_env.teardown().await;
    }
}
