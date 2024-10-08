use std::sync::Arc;

use entities::models::{OffChainData, Updated};
use rand::{random, Rng};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

use entities::enums::SpecificationAssetClass;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::errors::StorageError;
use rocks_db::migrator::MigrationState;
use rocks_db::{
    asset::AssetCollection, AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
    Storage,
};
use tokio::{sync::Mutex, task::JoinSet};

const DEFAULT_TEST_URL: &str = "http://example.com";

pub struct RocksTestEnvironment {
    pub storage: Arc<Storage>,
    _temp_dir: TempDir,
}

pub struct RocksTestEnvironmentSetup;

#[derive(Debug, Clone)]
pub struct GeneratedAssets {
    pub pubkeys: Vec<Pubkey>,
    pub static_details: Vec<AssetStaticDetails>,
    pub authorities: Vec<AssetAuthority>,
    pub owners: Vec<AssetOwner>,
    pub dynamic_details: Vec<AssetDynamicDetails>,
    pub collections: Vec<AssetCollection>,
}

impl RocksTestEnvironment {
    pub fn new(keys: &[(u64, Pubkey)]) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let join_set = Arc::new(Mutex::new(JoinSet::new()));
        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let storage = Storage::open(
            temp_dir.path().to_str().unwrap(),
            join_set,
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to create a database");

        for &(slot, ref pubkey) in keys {
            storage
                .asset_updated(slot, *pubkey)
                .expect("Cannot update assets.");
        }

        RocksTestEnvironment {
            storage: Arc::new(storage),
            _temp_dir: temp_dir,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn generate_from_closure(
        &self,
        cnt: usize,
        slot: u64,
        static_details: fn(&[Pubkey], u64) -> Vec<AssetStaticDetails>,
        authorities: fn(&[Pubkey]) -> Vec<AssetAuthority>,
        owners: fn(&[Pubkey]) -> Vec<AssetOwner>,
        dynamic_details: fn(&[Pubkey], u64) -> Vec<AssetDynamicDetails>,
        collections: fn(&[Pubkey]) -> Vec<AssetCollection>,
    ) -> GeneratedAssets {
        let pubkeys = (0..cnt)
            .map(|_| self.generate_and_store_pubkey(slot))
            .collect::<Vec<_>>();
        let static_details = static_details(&pubkeys, slot);
        let authorities = authorities(&pubkeys);
        let owners = owners(&pubkeys);
        let dynamic_details = dynamic_details(&pubkeys, slot);
        let collections = collections(&pubkeys);

        let assets = GeneratedAssets {
            pubkeys,
            static_details,
            authorities,
            owners,
            dynamic_details,
            collections,
        };

        self.put_everything_in_the_database(&assets)
            .await
            .expect("Cannot store 'GeneratedAssets' into storage.");

        assets
    }

    pub async fn generate_assets(&self, cnt: usize, slot: u64) -> GeneratedAssets {
        let pubkeys = (0..cnt)
            .map(|_| self.generate_and_store_pubkey(slot))
            .collect::<Vec<_>>();
        let static_details = RocksTestEnvironmentSetup::static_data_for_nft(&pubkeys, slot);
        let authorities = RocksTestEnvironmentSetup::with_authority(&pubkeys);
        let owners = RocksTestEnvironmentSetup::test_owner(&pubkeys);
        let dynamic_details = RocksTestEnvironmentSetup::dynamic_data(&pubkeys, slot);
        let collections = RocksTestEnvironmentSetup::collection_without_authority(&pubkeys);

        let assets = GeneratedAssets {
            pubkeys,
            static_details,
            authorities,
            owners,
            dynamic_details,
            collections,
        };

        self.put_everything_in_the_database(&assets)
            .await
            .expect("Cannot store 'GeneratedAssets' into storage.");

        assets
    }

    fn generate_and_store_pubkey(&self, slot: u64) -> Pubkey {
        let pubkey = Pubkey::new_unique();
        self.storage
            .asset_updated(slot, pubkey)
            .expect("Cannot update assets.");
        pubkey
    }

    async fn put_everything_in_the_database(
        &self,
        generated_assets: &GeneratedAssets,
    ) -> Result<(), StorageError> {
        self.storage.asset_offchain_data.put(
            ToOwned::to_owned(DEFAULT_TEST_URL),
            OffChainData {
                url: ToOwned::to_owned(DEFAULT_TEST_URL),
                metadata: ToOwned::to_owned("{}"),
            },
        )?;

        let static_data_batch = self.storage.asset_static_data.put_batch(
            generated_assets
                .static_details
                .iter()
                .map(|value| (value.pubkey, value.clone()))
                .collect(),
        );
        let authority_batch = self.storage.asset_authority_data.put_batch(
            generated_assets
                .authorities
                .iter()
                .map(|value| (value.pubkey, value.clone()))
                .collect(),
        );
        let owners_batch = self.storage.asset_owner_data.put_batch(
            generated_assets
                .owners
                .iter()
                .map(|value| (value.pubkey, value.clone()))
                .collect(),
        );
        let dynamic_details_batch = self.storage.asset_dynamic_data.put_batch(
            generated_assets
                .dynamic_details
                .iter()
                .map(|value| (value.pubkey, value.clone()))
                .collect(),
        );
        let collections_batch = self.storage.asset_collection_data.put_batch(
            generated_assets
                .collections
                .iter()
                .map(|value| (value.pubkey, value.clone()))
                .collect(),
        );

        tokio::try_join!(
            static_data_batch,
            authority_batch,
            owners_batch,
            dynamic_details_batch,
            collections_batch
        )?;

        Ok(())
    }
}

impl RocksTestEnvironmentSetup {
    pub fn static_data_for_mpl(pubkeys: &[Pubkey], slot: u64) -> Vec<AssetStaticDetails> {
        Self::generate_static_data(pubkeys, slot, SpecificationAssetClass::MplCoreAsset)
    }

    pub fn static_data_for_nft(pubkeys: &[Pubkey], slot: u64) -> Vec<AssetStaticDetails> {
        Self::generate_static_data(pubkeys, slot, SpecificationAssetClass::Nft)
    }

    fn generate_static_data(
        pubkeys: &[Pubkey],
        slot: u64,
        specification_asset_class: SpecificationAssetClass,
    ) -> Vec<AssetStaticDetails> {
        pubkeys
            .iter()
            .map(|pubkey| AssetStaticDetails {
                pubkey: *pubkey,
                created_at: slot as i64,
                specification_asset_class,
                royalty_target_type: entities::enums::RoyaltyTargetType::Creators,
                edition_address: Default::default(),
            })
            .collect()
    }

    pub fn without_authority(_: &[Pubkey]) -> Vec<AssetAuthority> {
        Vec::new()
    }

    pub fn with_authority(pubkeys: &[Pubkey]) -> Vec<AssetAuthority> {
        pubkeys
            .iter()
            .map(|pubkey| AssetAuthority {
                pubkey: *pubkey,
                authority: Pubkey::new_unique(),
                slot_updated: rand::thread_rng().gen_range(0..100),
                write_version: None,
            })
            .collect()
    }

    pub fn test_owner(pubkeys: &[Pubkey]) -> Vec<AssetOwner> {
        pubkeys
            .iter()
            .map(|pubkey| AssetOwner {
                pubkey: *pubkey,
                owner: generate_test_updated(Some(Pubkey::new_unique())),
                owner_type: generate_test_updated(entities::enums::OwnerType::Single),
                owner_delegate_seq: generate_test_updated(Some(
                    rand::thread_rng().gen_range(0..100),
                )),
                delegate: generate_test_updated(Some(Pubkey::new_unique())),
            })
            .collect()
    }

    pub fn dynamic_data(pubkeys: &[Pubkey], slot: u64) -> Vec<AssetDynamicDetails> {
        pubkeys
            .iter()
            .map(|pubkey| create_test_dynamic_data(*pubkey, slot, DEFAULT_TEST_URL.to_owned()))
            .collect()
    }

    pub fn collection_with_authority(pubkeys: &[Pubkey]) -> Vec<AssetCollection> {
        Self::generate_collection(pubkeys, Some(Pubkey::new_unique()))
    }

    pub fn collection_without_authority(pubkeys: &[Pubkey]) -> Vec<AssetCollection> {
        Self::generate_collection(pubkeys, None)
    }

    fn generate_collection(pubkeys: &[Pubkey], authority: Option<Pubkey>) -> Vec<AssetCollection> {
        pubkeys
            .iter()
            .map(|pubkey| AssetCollection {
                pubkey: *pubkey,
                collection: generate_test_updated(Pubkey::new_unique()),
                is_collection_verified: generate_test_updated(false),
                authority: generate_test_updated(authority),
            })
            .collect()
    }
}

pub const DEFAULT_PUBKEY_OF_ONES: Pubkey = Pubkey::new_from_array([1u8; 32]);
pub const PUBKEY_OF_TWOS: Pubkey = Pubkey::new_from_array([2u8; 32]);

pub fn create_test_dynamic_data(pubkey: Pubkey, slot: u64, url: String) -> AssetDynamicDetails {
    AssetDynamicDetails {
        pubkey,
        is_compressible: Updated::new(slot, None, false),
        is_compressed: Updated::new(slot, None, false),
        is_frozen: Updated::new(slot, None, false),
        supply: Some(Updated::new(slot, None, 1)),
        seq: None,
        is_burnt: Updated::new(slot, None, false),
        was_decompressed: Updated::new(slot, None, false),
        onchain_data: None,
        creators: Updated::new(slot, None, vec![generate_test_creator()]),
        royalty_amount: Updated::new(slot, None, 0),
        url: Updated::new(slot, None, url),
        chain_mutability: Default::default(),
        lamports: None,
        executable: None,
        metadata_owner: None,
        ..Default::default()
    }
}

fn generate_test_creator() -> entities::models::Creator {
    entities::models::Creator {
        creator: Pubkey::new_unique(),
        creator_verified: random(),
        creator_share: rand::thread_rng().gen_range(0..100),
    }
}

fn generate_test_updated<T>(v: T) -> Updated<T> {
    Updated::new(rand::thread_rng().gen_range(0..100), None, v)
}
