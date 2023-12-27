use std::sync::atomic::AtomicU64;
use std::{marker::PhantomData, sync::Arc};

use asset::SlotAssetIdx;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

pub use asset::{
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, AssetsUpdateIdx,
};
pub use column::columns;
use column::{Column, TypedColumn};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::errors::StorageError;

pub mod asset;
mod asset_client;
pub mod asset_streaming_client;
pub mod backup_service;
mod batch_client;
pub mod bubblegum_slots;
pub mod cl_items;
pub mod column;
pub mod errors;
pub mod key_encoders;
pub mod offchain_data;
pub mod storage_traits;

pub type Result<T> = std::result::Result<T, StorageError>;

pub struct Storage {
    pub asset_static_data: Column<AssetStaticDetails>,
    pub asset_dynamic_data: Column<AssetDynamicDetails>,
    pub asset_authority_data: Column<AssetAuthority>,
    pub asset_owner_data: Column<AssetOwner>,
    pub asset_leaf_data: Column<asset::AssetLeaf>,
    pub asset_collection_data: Column<asset::AssetCollection>,
    pub asset_offchain_data: Column<offchain_data::OffChainData>,
    pub cl_items: Column<cl_items::ClItem>,
    pub cl_leafs: Column<cl_items::ClLeaf>,
    pub bubblegum_slots: Column<bubblegum_slots::BubblegumSlots>,
    pub db: Arc<DB>,
    pub assets_update_idx: Column<AssetsUpdateIdx>,
    pub slot_asset_idx: Column<SlotAssetIdx>,
    assets_update_last_seq: AtomicU64,
    join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
}

impl Storage {
    pub fn open(
        db_path: &str,
        join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
    ) -> Result<Self> {
        let db = Arc::new(DB::open_cf_descriptors(
            &Self::get_db_options(),
            db_path,
            vec![
                Self::new_cf_descriptor::<offchain_data::OffChainData>(),
                Self::new_cf_descriptor::<AssetStaticDetails>(),
                Self::new_cf_descriptor::<AssetDynamicDetails>(),
                Self::new_cf_descriptor::<AssetAuthority>(),
                Self::new_cf_descriptor::<AssetOwner>(),
                Self::new_cf_descriptor::<asset::AssetLeaf>(),
                Self::new_cf_descriptor::<asset::AssetCollection>(),
                Self::new_cf_descriptor::<cl_items::ClItem>(),
                Self::new_cf_descriptor::<cl_items::ClLeaf>(),
                Self::new_cf_descriptor::<bubblegum_slots::BubblegumSlots>(),
                Self::new_cf_descriptor::<asset::AssetsUpdateIdx>(),
                Self::new_cf_descriptor::<asset::SlotAssetIdx>(),
            ],
        )?);
        let asset_offchain_data = Self::column(db.clone());

        let asset_static_data = Self::column(db.clone());
        let asset_dynamic_data = Self::column(db.clone());
        let asset_authority_data = Self::column(db.clone());
        let asset_owner_data = Self::column(db.clone());
        let asset_leaf_data = Self::column(db.clone());
        let asset_collection_data = Self::column(db.clone());

        let cl_items = Self::column(db.clone());
        let cl_leafs = Self::column(db.clone());

        let bubblegum_slots = Self::column(db.clone());

        let assets_update_idx = Self::column(db.clone());
        let slot_asset_idx = Self::column(db.clone());

        Ok(Self {
            asset_static_data,
            asset_dynamic_data,
            asset_authority_data,
            asset_owner_data,
            asset_leaf_data,
            asset_collection_data,
            asset_offchain_data,
            cl_items,
            cl_leafs,
            bubblegum_slots,
            db,
            assets_update_idx,
            slot_asset_idx,
            assets_update_last_seq: AtomicU64::new(0),
            join_set,
        })
    }

    fn new_cf_descriptor<C: TypedColumn>() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options::<C>())
    }

    pub fn column<C>(backend: Arc<DB>) -> Column<C>
    where
        C: TypedColumn,
    {
        Column {
            backend,
            column: PhantomData,
        }
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create missing items to support a clean start
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Per the docs, a good value for this is the number of cores on the machine
        options.increase_parallelism(num_cpus::get() as i32);

        let mut env = rocksdb::Env::new().unwrap();
        // While a compaction is ongoing, all the background threads
        // could be used by the compaction. This can stall writes which
        // need to flush the memtable. Add some high-priority background threads
        // which can service these writes.
        env.set_high_priority_background_threads(4);
        options.set_env(&env);

        // Set max total wal size to 4G.
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);

        // Allow Rocks to open/keep open as many files as it needs for performance;
        // however, this is also explicitly required for a secondary instance.
        // See https://github.com/facebook/rocksdb/wiki/Secondary-instance
        options.set_max_open_files(-1);

        options
    }

    fn get_cf_options<C: TypedColumn>() -> Options {
        const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024; // 256MB

        let mut cf_options = Options::default();
        // 256 * 8 = 2GB. 6 of these columns should take at most 12GB of RAM
        cf_options.set_max_write_buffer_number(8);
        cf_options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);
        let file_num_compaction_trigger = 4;
        // Recommend that this be around the size of level 0. Level 0 estimated size in stable state is
        // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger
        // Source: https://docs.rs/rocksdb/0.6.0/rocksdb/struct.Options.html#method.set_level_zero_file_num_compaction_trigger
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        cf_options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        cf_options.set_max_bytes_for_level_base(total_size_base);
        cf_options.set_target_file_size_base(file_size_base);

        // Optional merges
        if C::NAME == asset::AssetStaticDetails::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_merge_static_details",
                asset::AssetStaticDetails::merge_static_details,
            );
        }

        if C::NAME == asset::AssetDynamicDetails::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_merge_dynamic_details",
                asset::AssetDynamicDetails::merge_dynamic_details,
            );
        }
        if C::NAME == asset::AssetAuthority::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_merge_asset_authorities",
                asset::AssetAuthority::merge_asset_authorities,
            );
        }
        if C::NAME == asset::AssetOwner::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_merge_asset_owner",
                asset::AssetOwner::merge_asset_owner,
            );
        }
        if C::NAME == asset::AssetLeaf::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_merge_asset_leaf",
                asset::AssetLeaf::merge_asset_leaf,
            );
        }
        if C::NAME == asset::AssetCollection::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_asset_collection",
                asset::AssetCollection::merge_asset_collection,
            );
        }
        if C::NAME == cl_items::ClItem::NAME {
            cf_options.set_merge_operator_associative(
                "merge_fn_cl_item",
                cl_items::ClItem::merge_cl_items,
            );
        }

        cf_options
    }
}
