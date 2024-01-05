use std::sync::Arc;

use entities::models::Updated;
use rand::{random, Rng};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

use rocks_db::{
    asset::AssetCollection, offchain_data::OffChainData, AssetAuthority, AssetDynamicDetails,
    AssetOwner, AssetStaticDetails, Storage,
};
use tokio::{sync::Mutex, task::JoinSet};

pub struct RocksTestEnvironment {
    pub storage: Arc<Storage>,
    _temp_dir: TempDir,
}

impl RocksTestEnvironment {
    pub fn new(keys: &[(u64, Pubkey)]) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
        let join_set = Arc::new(Mutex::new(JoinSet::new()));
        let storage = Storage::open(temp_dir.path().to_str().unwrap(), join_set)
            .expect("Failed to create a database");
        for &(slot, ref pubkey) in keys {
            storage.asset_updated(slot, pubkey.clone()).unwrap();
        }

        RocksTestEnvironment {
            storage: Arc::new(storage),
            _temp_dir: temp_dir,
        }
    }

    pub fn generate_assets(
        &self,
        cnt: usize,
        slot: u64,
    ) -> (
        Vec<Pubkey>,
        Vec<AssetStaticDetails>,
        Vec<AssetAuthority>,
        Vec<AssetOwner>,
        Vec<AssetDynamicDetails>,
        Vec<AssetCollection>,
    ) {
        let pks = (0..cnt).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        for pk in pks.iter() {
            self.storage.asset_updated(slot, pk.clone()).unwrap();
        }
        let url = "http://example.com";
        // generate 1000 units of data using generate_test_static_data,generate_test_authority,generate_test_owner and create_test_dynamic_data for a 1000 unique pubkeys
        let static_data = pks
            .iter()
            .map(|pk| generate_test_static_data(pk.clone(), slot))
            .collect::<Vec<_>>();
        let authority_data = pks
            .iter()
            .map(|pk| generate_test_authority(pk.clone()))
            .collect::<Vec<_>>();
        let owner_data = pks
            .iter()
            .map(|pk| generate_test_owner(pk.clone()))
            .collect::<Vec<_>>();
        let dynamic_data = pks
            .iter()
            .map(|pk| create_test_dynamic_data(pk.clone(), slot, url.to_string()))
            .collect::<Vec<_>>();
        let collection_data = pks
            .iter()
            .map(|pk| generate_test_collection(pk.clone()))
            .collect::<Vec<_>>();
        // put everything in the database

        self.storage
            .asset_offchain_data
            .put(
                url.to_string(),
                &OffChainData {
                    url: url.to_string(),
                    metadata: "{}".to_string(),
                },
            )
            .unwrap();
        for (((((pk, static_data), authority_data), owner_data), dynamic_data), collection_data) in
            pks.iter()
                .zip(static_data.iter())
                .zip(authority_data.iter())
                .zip(owner_data.iter())
                .zip(dynamic_data.iter())
                .zip(collection_data.iter())
        {
            self.storage
                .asset_authority_data
                .put(*pk, authority_data)
                .unwrap();
            self.storage.asset_owner_data.put(*pk, owner_data).unwrap();
            self.storage
                .asset_static_data
                .put(*pk, static_data)
                .unwrap();
            self.storage.asset_owner_data.put(*pk, owner_data).unwrap();
            self.storage
                .asset_dynamic_data
                .put(*pk, dynamic_data)
                .unwrap();
            self.storage
                .asset_collection_data
                .put(*pk, collection_data)
                .unwrap();
        }
        (
            pks,
            static_data,
            authority_data,
            owner_data,
            dynamic_data,
            collection_data,
        )
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
    }
}

pub fn generate_test_authority(pubkey: Pubkey) -> AssetAuthority {
    AssetAuthority {
        pubkey,
        authority: Pubkey::new_unique(),
        slot_updated: rand::thread_rng().gen_range(0..100),
    }
}

pub fn generate_test_owner(pubkey: Pubkey) -> AssetOwner {
    AssetOwner {
        pubkey,
        owner: generate_test_updated(Pubkey::new_unique()),
        owner_type: generate_test_updated(entities::enums::OwnerType::Single),
        owner_delegate_seq: Some(generate_test_updated(rand::thread_rng().gen_range(0..100))),
        delegate: Some(generate_test_updated(Pubkey::new_unique())),
    }
}

fn generate_test_updated<T>(v: T) -> Updated<T> {
    Updated::new(rand::thread_rng().gen_range(0..100), None, v)
}

fn generate_test_collection(pubkey: Pubkey) -> AssetCollection {
    AssetCollection {
        pubkey,
        collection: Pubkey::new_unique(),
        slot_updated: random(),
        is_collection_verified: random(),
        collection_seq: random(),
    }
}
