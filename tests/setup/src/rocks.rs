use std::sync::Arc;

use entities::models::{OffChainData, Updated};
use rand::{random, Rng};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::migrator::MigrationState;
use rocks_db::{
    asset::AssetCollection, AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails,
    Storage,
};
use tokio::{sync::Mutex, task::JoinSet};

pub struct RocksTestEnvironment {
    pub storage: Arc<Storage>,
    _temp_dir: TempDir,
}

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
            storage.asset_updated(slot, *pubkey).unwrap();
        }

        RocksTestEnvironment {
            storage: Arc::new(storage),
            _temp_dir: temp_dir,
        }
    }

    pub fn generate_assets(&self, cnt: usize, slot: u64) -> GeneratedAssets {
        let pubkeys = (0..cnt).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        for pk in pubkeys.iter() {
            self.storage.asset_updated(slot, *pk).unwrap();
        }
        let url = "http://example.com";
        // generate 1000 units of data using generate_test_static_data,generate_test_authority,generate_test_owner and create_test_dynamic_data for a 1000 unique pubkeys
        let static_details = pubkeys
            .iter()
            .map(|pk| generate_test_static_data(*pk, slot))
            .collect::<Vec<_>>();
        let authorities = pubkeys
            .iter()
            .map(|pk| generate_test_authority(*pk))
            .collect::<Vec<_>>();
        let owners = pubkeys
            .iter()
            .map(|pk| generate_test_owner(*pk))
            .collect::<Vec<_>>();
        let dynamic_details = pubkeys
            .iter()
            .map(|pk| create_test_dynamic_data(*pk, slot, url.to_string()))
            .collect::<Vec<_>>();
        let collections = pubkeys
            .iter()
            .map(|pk| generate_test_collection(*pk))
            .collect::<Vec<_>>();
        // put everything in the database

        self.storage
            .asset_offchain_data
            .put(
                url.to_string(),
                OffChainData {
                    url: url.to_string(),
                    metadata: "{}".to_string(),
                },
            )
            .unwrap();
        for (((((pk, static_data), authority_data), owner_data), dynamic_data), collection_data) in
            pubkeys
                .iter()
                .zip(static_details.iter())
                .zip(authorities.iter())
                .zip(owners.iter())
                .zip(dynamic_details.iter())
                .zip(collections.iter())
        {
            self.storage
                .asset_authority_data
                .put(*pk, authority_data.clone())
                .unwrap();
            self.storage
                .asset_static_data
                .put(*pk, static_data.clone())
                .unwrap();
            self.storage
                .asset_owner_data
                .put(*pk, owner_data.clone())
                .unwrap();
            self.storage
                .asset_dynamic_data
                .put(*pk, dynamic_data.clone())
                .unwrap();
            self.storage
                .asset_collection_data
                .put(*pk, collection_data.clone())
                .unwrap();
        }
        GeneratedAssets {
            pubkeys,
            static_details,
            authorities,
            owners,
            dynamic_details,
            collections,
        }
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

pub fn generate_test_creator() -> entities::models::Creator {
    entities::models::Creator {
        creator: Pubkey::new_unique(),
        creator_verified: random(),
        creator_share: rand::thread_rng().gen_range(0..100),
    }
}

pub fn generate_test_static_data(pubkey: Pubkey, slot: u64) -> AssetStaticDetails {
    AssetStaticDetails {
        pubkey,
        created_at: slot as i64,
        specification_asset_class: entities::enums::SpecificationAssetClass::Nft,
        royalty_target_type: entities::enums::RoyaltyTargetType::Creators,
        edition_address: Default::default(),
    }
}

pub fn generate_test_authority(pubkey: Pubkey) -> AssetAuthority {
    AssetAuthority {
        pubkey,
        authority: Pubkey::new_unique(),
        slot_updated: rand::thread_rng().gen_range(0..100),
        write_version: None,
    }
}

pub fn generate_test_owner(pubkey: Pubkey) -> AssetOwner {
    AssetOwner {
        pubkey,
        owner: generate_test_updated(Some(Pubkey::new_unique())),
        owner_type: generate_test_updated(entities::enums::OwnerType::Single),
        owner_delegate_seq: generate_test_updated(Some(rand::thread_rng().gen_range(0..100))),
        delegate: generate_test_updated(Some(Pubkey::new_unique())),
    }
}

fn generate_test_updated<T>(v: T) -> Updated<T> {
    Updated::new(rand::thread_rng().gen_range(0..100), None, v)
}

fn generate_test_collection(pubkey: Pubkey) -> AssetCollection {
    AssetCollection {
        pubkey,
        collection: generate_test_updated(Pubkey::new_unique()),
        is_collection_verified: generate_test_updated(false),
        authority: generate_test_updated(Some(Pubkey::new_unique())),
    }
}
