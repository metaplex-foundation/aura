#[cfg(test)]
pub mod setup {
    use std::sync::Arc;

    use entities::models::Updated;
    use rand::Rng;
    use solana_sdk::pubkey::Pubkey;
    use tempfile::TempDir;

    use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
    use tokio::{sync::Mutex, task::JoinSet};

    pub struct TestEnvironment {
        pub storage: Storage,
        _temp_dir: TempDir,
    }

    impl TestEnvironment {
        pub fn new(temp_dir: TempDir, keys: &[(u64, Pubkey)]) -> Self {
            let join_set = Arc::new(Mutex::new(JoinSet::new()));
            let storage = Storage::open(temp_dir.path().to_str().unwrap(), join_set)
                .expect("Failed to create a database");
            for &(slot, ref pubkey) in keys {
                storage.asset_updated(slot, pubkey.clone()).unwrap();
            }

            TestEnvironment {
                storage,
                _temp_dir: temp_dir,
            }
        }
    }

    pub const DEFAULT_PUBKEY_OF_ONES: Pubkey = Pubkey::new_from_array([1u8; 32]);
    pub const PUBKEY_OF_TWOS: Pubkey = Pubkey::new_from_array([2u8; 32]);

    pub fn create_test_dynamic_data(pubkey: Pubkey, slot: u64) -> AssetDynamicDetails {
        AssetDynamicDetails {
            pubkey,
            is_compressible: Updated::new(slot, None, false),
            is_compressed: Updated::new(slot, None, false),
            is_frozen: Updated::new(slot, None, false),
            is_burnt: Updated::new(slot, None, false),
            was_decompressed: Updated::new(slot, None, false),
            creators: Updated::new(slot, None, Vec::new()),
            royalty_amount: Updated::new(slot, None, 0),
            ..Default::default()
        }
    }

    pub fn generate_test_static_data(pubkey: Pubkey, slot: u64) -> AssetStaticDetails {
        AssetStaticDetails {
            pubkey,
            created_at: slot as i64,
            specification_asset_class: entities::enums::SpecificationAssetClass::IdentityNft,
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
}
