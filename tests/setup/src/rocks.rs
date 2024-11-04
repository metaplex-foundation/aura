use std::sync::Arc;

use entities::models::{OffChainData, Updated};
use rand::{random, Rng};
use rocks_db::asset::AssetCompleteDetails;
use rocks_db::column::TypedColumn;
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
        let pubkey = Pubkey::from(rand::random::<[u8; 32]>());
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
                        &self
                            .storage
                            .db
                            .cf_handle(AssetCompleteDetails::NAME)
                            .unwrap(),
                        *pubkey,
                        builder.finished_data(),
                    );
                    builder.reset();
                },
            );
        self.storage.db.write(batch)?;

        // let static_data_batch = self.storage.asset_static_data.put_batch(
        //     generated_assets
        //         .static_details
        //         .iter()
        //         .map(|value| (value.pubkey, value.clone()))
        //         .collect(),
        // );
        // let authority_batch = self.storage.asset_authority_data.put_batch(
        //     generated_assets
        //         .authorities
        //         .iter()
        //         .map(|value| (value.pubkey, value.clone()))
        //         .collect(),
        // );
        // let owners_batch = self.storage.asset_owner_data.put_batch(
        //     generated_assets
        //         .owners
        //         .iter()
        //         .map(|value| (value.pubkey, value.clone()))
        //         .collect(),
        // );
        // let dynamic_details_batch = self.storage.asset_dynamic_data.put_batch(
        //     generated_assets
        //         .dynamic_details
        //         .iter()
        //         .map(|value| (value.pubkey, value.clone()))
        //         .collect(),
        // );
        // let collections_batch = self.storage.asset_collection_data.put_batch(
        //     generated_assets
        //         .collections
        //         .iter()
        //         .map(|value| (value.pubkey, value.clone()))
        //         .collect(),
        // );

        // tokio::try_join!(
        //     static_data_batch,
        //     authority_batch,
        //     owners_batch,
        //     dynamic_details_batch,
        //     collections_batch
        // )
        // .map_err(|e| {
        //     error!("join failed {}", e);
        //     e
        // })?;

        // self.storage.apply_migration1().await.map_err(|e| {
        //     error!("appliying migration failed {}", e);
        //     e
        // })?;

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
                authority: Pubkey::from(rand::random::<[u8; 32]>()),
                slot_updated: rand::thread_rng().gen_range(0..100),
                write_version: Some(rand::thread_rng().gen_range(0..100)),
            })
            .collect()
    }

    pub fn test_owner(pubkeys: &[Pubkey]) -> Vec<AssetOwner> {
        pubkeys
            .iter()
            .map(|pubkey| AssetOwner {
                pubkey: *pubkey,
                owner: generate_test_updated(None, Some(Pubkey::from(rand::random::<[u8; 32]>()))),
                owner_type: generate_test_updated(None, entities::enums::OwnerType::Single),
                owner_delegate_seq: generate_test_updated(
                    None,
                    Some(rand::thread_rng().gen_range(0..100)),
                ),
                delegate: generate_test_updated(
                    None,
                    Some(Pubkey::from(rand::random::<[u8; 32]>())),
                ),
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
                collection: generate_test_updated(None, Pubkey::new_unique()),
                is_collection_verified: generate_test_updated(None, false),
                authority: generate_test_updated(None, authority),
            })
            .collect()
    }
}

pub const DEFAULT_PUBKEY_OF_ONES: Pubkey = Pubkey::new_from_array([1u8; 32]);
pub const PUBKEY_OF_TWOS: Pubkey = Pubkey::new_from_array([2u8; 32]);

pub fn create_test_dynamic_data(pubkey: Pubkey, slot: u64, url: String) -> AssetDynamicDetails {
    AssetDynamicDetails {
        pubkey,
        is_compressible: generate_test_updated(Some(slot), rand::thread_rng().gen_bool(0.5)),
        is_compressed: generate_test_updated(Some(slot), rand::thread_rng().gen_bool(0.5)),
        is_frozen: generate_test_updated(Some(slot), rand::thread_rng().gen_bool(0.5)),
        supply: maybe_generate_test_updated(Some(slot), 1),
        seq: maybe_generate_test_updated(Some(slot), rand::thread_rng().gen_range(0..50000)),
        is_burnt: generate_test_updated(Some(slot), rand::thread_rng().gen_bool(0.5)),
        was_decompressed: generate_test_updated(Some(slot), rand::thread_rng().gen_bool(0.5)),
        onchain_data: maybe_generate_test_updated(
            Some(slot),
            "{\"name\":\"onchain data\",\"symbol\":\"some\",\"primary_sale_happened\":false}"
                .to_string(),
        ),
        creators: generate_test_updated(Some(slot), vec![generate_test_creator()]),
        royalty_amount: generate_test_updated(Some(slot), rand::thread_rng().gen_range(0..50)),
        url: generate_test_updated(Some(slot), url),
        chain_mutability: maybe_generate_test_updated(
            Some(slot),
            entities::enums::ChainMutability::Mutable,
        ),
        lamports: maybe_generate_test_updated(Some(slot), rand::thread_rng().gen_range(0..50000)),
        executable: maybe_generate_test_updated(Some(slot), false),
        metadata_owner: None,
        raw_name: None,
        mpl_core_plugins: maybe_generate_test_updated(Some(slot), "some core plugins".to_string()),
        mpl_core_unknown_plugins: maybe_generate_test_updated(
            Some(slot),
            "some unknown plugins".to_string(),
        ),
        rent_epoch: maybe_generate_test_updated(
            Some(slot),
            rand::thread_rng().gen_range(1110..1000000),
        ),
        num_minted: maybe_generate_test_updated(Some(slot), rand::thread_rng().gen_range(10..1000)),
        current_size: maybe_generate_test_updated(
            Some(slot),
            rand::thread_rng().gen_range(20..1000),
        ),
        plugins_json_version: maybe_generate_test_updated(
            Some(slot),
            rand::thread_rng().gen_range(0..10),
        ),
        mpl_core_external_plugins: maybe_generate_test_updated(
            Some(slot),
            "some external plugins".to_string(),
        ),
        mpl_core_unknown_external_plugins: maybe_generate_test_updated(
            Some(slot),
            "some unknown".to_string(),
        ),
        mint_extensions: maybe_generate_test_updated(Some(slot), "value".to_string()),
    }
}

fn generate_test_creator() -> entities::models::Creator {
    entities::models::Creator {
        creator: Pubkey::from(rand::random::<[u8; 32]>()),
        creator_verified: random(),
        creator_share: rand::thread_rng().gen_range(0..100),
    }
}

fn generate_test_updated<T>(slot: Option<u64>, v: T) -> Updated<T> {
    Updated::new(
        slot.unwrap_or(rand::thread_rng().gen_range(0..1000)),
        Some(entities::models::UpdateVersion::Sequence(
            rand::thread_rng().gen_range(100..200),
        )),
        v,
    )
}

fn maybe_generate_test_updated<T>(slot: Option<u64>, value: T) -> Option<Updated<T>> {
    if rand::thread_rng().gen_bool(0.5) {
        Some(generate_test_updated(slot, value))
    } else {
        None
    }
}
