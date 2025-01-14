use std::sync::Arc;

use entities::{enums::SpecificationAssetClass, models::Updated};
use metrics_utils::red::RequestErrorDurationMetrics;
use rand::{random, Rng};
use rocks_db::{
    column::TypedColumn,
    columns::{
        asset::{
            AssetAuthority, AssetCollection, AssetCompleteDetails, AssetDynamicDetails, AssetOwner,
            AssetStaticDetails,
        },
        offchain_data::OffChainData,
    },
    errors::StorageError,
    migrator::MigrationState,
    SlotStorage, Storage, ToFlatbuffersConverter,
};
use solana_sdk::pubkey::Pubkey;
use sqlx::types::chrono::Utc;
use tempfile::TempDir;
use tokio::{sync::Mutex, task::JoinSet};

const DEFAULT_TEST_URL: &str = "http://example.com";

pub struct RocksTestEnvironment {
    pub storage: Arc<Storage>,
    pub slot_storage: Arc<SlotStorage>,
    // holds references to storage & slot storage temp dirs
    _temp_dirs: (TempDir, TempDir),
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
        let storage_temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let slot_storage_temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let join_set = Arc::new(Mutex::new(JoinSet::new()));
        let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
        let storage = Storage::open(
            storage_temp_dir.path().to_str().unwrap(),
            join_set.clone(),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to create storage database");
        let slot_storage = SlotStorage::open(
            slot_storage_temp_dir.path().to_str().unwrap(),
            join_set,
            red_metrics.clone(),
        )
        .expect("Failed to create slot storage database");

        for &(slot, ref pubkey) in keys {
            storage.asset_updated(slot, *pubkey).expect("Cannot update assets.");
        }

        RocksTestEnvironment {
            storage: Arc::new(storage),
            slot_storage: Arc::new(slot_storage),
            _temp_dirs: (storage_temp_dir, slot_storage_temp_dir),
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
        let pubkeys = (0..cnt).map(|_| self.generate_and_store_pubkey(slot)).collect::<Vec<_>>();
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
        let pubkeys = (0..cnt).map(|_| self.generate_and_store_pubkey(slot)).collect::<Vec<_>>();
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
        self.storage.asset_updated(slot, pubkey).expect("Cannot update assets.");
        pubkey
    }

    async fn put_everything_in_the_database(
        &self,
        generated_assets: &GeneratedAssets,
    ) -> Result<(), StorageError> {
        self.storage.asset_offchain_data.put(
            ToOwned::to_owned(DEFAULT_TEST_URL),
            OffChainData {
                url: Some(ToOwned::to_owned(DEFAULT_TEST_URL)),
                metadata: Some(ToOwned::to_owned("{}")),
                last_read_at: Utc::now().timestamp(),
                storage_mutability: DEFAULT_TEST_URL.into(),
            },
        )?;

        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(2500);
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        generated_assets
            .pubkeys
            .iter()
            .zip(generated_assets.static_details.iter())
            .zip(generated_assets.dynamic_details.iter())
            .zip(generated_assets.collections.iter())
            .zip(generated_assets.owners.iter())
            .enumerate()
            .for_each(
                |(index, ((((pubkey, static_details), dynamic_details), collection), owner))| {
                    let authority = generated_assets.authorities.get(index);
                    let complete_asset = AssetCompleteDetails {
                        pubkey: *pubkey,
                        static_details: Some(static_details.clone()),
                        dynamic_details: Some(dynamic_details.clone()),
                        authority: authority.cloned(),
                        collection: Some(collection.clone()),
                        owner: Some(owner.clone()),
                    };
                    let asset_data = complete_asset.convert_to_fb(&mut builder);
                    builder.finish_minimal(asset_data);
                    batch.put_cf(
                        &self.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                        *pubkey,
                        builder.finished_data(),
                    );
                    builder.reset();
                },
            );
        self.storage.db.write(batch)?;
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
                is_current_owner: generate_test_updated(true),
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
        was_decompressed: Some(Updated::new(slot, None, false)),
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
