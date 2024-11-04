use asset_previews::{AssetPreviews, UrlToDownload};
use entities::schedule::ScheduledJob;
use inflector::Inflector;
use leaf_signatures::LeafSignature;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::{marker::PhantomData, sync::Arc};

use asset::{
    AssetAuthorityDeprecated, AssetCollectionDeprecated, AssetCompleteDetails,
    AssetDynamicDetailsDeprecated, AssetOwnerDeprecated, AssetStaticDetailsDeprecated,
    MetadataMintMap, SlotAssetIdx,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::migrator::{MigrationState, MigrationVersions, RocksMigration};
pub use asset::{
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, AssetsUpdateIdx,
};
use column::{Column, TypedColumn};
use entities::enums::TokenMetadataEdition;
use entities::models::{
    AssetSignature, BatchMintToVerify, FailedBatchMint, OffChainData, RawBlock, SplMint,
    TokenAccount,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::batch_mint::BatchMintWithStaker;
use crate::errors::StorageError;
use crate::inscriptions::{Inscription, InscriptionData};
use crate::migrations::clean_update_authorities::CleanCollectionAuthoritiesMigration;
use crate::migrations::collection_authority::{
    AssetCollectionVersion0, CollectionAuthorityMigration,
};
use crate::migrations::external_plugins::{AssetDynamicDetailsV0, ExternalPluginsMigration};
use crate::migrations::spl2022::{
    AssetDynamicDetailsWithoutExtentions, DynamicDataToken2022MintExtentionsMigration,
    TokenAccounts2022ExtentionsMigration,
};
use crate::parameters::ParameterColumn;
use crate::token_accounts::{TokenAccountMintOwnerIdx, TokenAccountOwnerIdx};
use crate::token_prices::TokenPrice;
use crate::tree_seq::{TreeSeqIdx, TreesGaps};

pub mod asset;
mod asset_client;
pub mod asset_previews;
pub mod asset_signatures;
pub mod asset_streaming_client;
pub mod backup_service;
mod batch_client;
pub mod batch_mint;
pub mod batch_savers;
pub mod bubblegum_slots;
pub mod cl_items;
pub mod column;
pub mod dump_client;
pub mod editions;
pub mod errors;
pub mod fork_cleaner;
pub mod inscriptions;
pub mod key_encoders;
pub mod leaf_signatures;
pub mod migrations;
pub mod migrator;
pub mod offchain_data;
pub mod parameters;
pub mod processing_possibility;
pub mod raw_block;
pub mod raw_blocks_streaming_client;
pub mod schedule;
pub mod sequence_consistent;
pub mod signature_client;
pub mod slots_dumper;
pub mod storage_traits;
pub mod token_accounts;
pub mod token_prices;
pub mod transaction;
pub mod transaction_client;
pub mod tree_seq;
// import the flatbuffers runtime library
extern crate flatbuffers;
#[allow(
    clippy::missing_safety_doc,
    unused_imports,
    clippy::extra_unused_lifetimes
)]
pub mod asset_generated;
pub mod mappers;

pub type Result<T> = std::result::Result<T, StorageError>;

const ROCKS_COMPONENT: &str = "rocks_db";
const DROP_ACTION: &str = "drop";
const RAW_BLOCKS_CBOR_ENDPOINT: &str = "raw_blocks_cbor";
const FULL_ITERATION_ACTION: &str = "full_iteration";
const BATCH_ITERATION_ACTION: &str = "batch_iteration";
const BATCH_GET_ACTION: &str = "batch_get";
const ITERATOR_TOP_ACTION: &str = "iterator_top";
const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024; // 256MB
pub struct Storage {
    pub asset_data: Column<AssetCompleteDetails>,
    pub asset_static_data: Column<AssetStaticDetails>,
    pub asset_static_data_deprecated: Column<AssetStaticDetailsDeprecated>,
    pub asset_dynamic_data: Column<AssetDynamicDetails>,
    pub asset_dynamic_data_deprecated: Column<AssetDynamicDetailsDeprecated>,
    pub metadata_mint_map: Column<MetadataMintMap>,
    pub asset_authority_data: Column<AssetAuthority>,
    pub asset_authority_deprecated: Column<AssetAuthorityDeprecated>,
    pub asset_owner_data_deprecated: Column<AssetOwnerDeprecated>,
    pub asset_owner_data: Column<AssetOwner>,
    pub asset_leaf_data: Column<asset::AssetLeaf>,
    pub asset_collection_data: Column<asset::AssetCollection>,
    pub asset_collection_data_deprecated: Column<AssetCollectionDeprecated>,
    pub asset_offchain_data: Column<OffChainData>,
    pub cl_items: Column<cl_items::ClItem>,
    pub cl_leafs: Column<cl_items::ClLeaf>,
    pub bubblegum_slots: Column<bubblegum_slots::BubblegumSlots>,
    pub ingestable_slots: Column<bubblegum_slots::IngestableSlots>,
    pub force_reingestable_slots: Column<bubblegum_slots::ForceReingestableSlots>,
    pub raw_blocks_cbor: Column<RawBlock>,
    pub db: Arc<DB>,
    pub assets_update_idx: Column<AssetsUpdateIdx>,
    pub slot_asset_idx: Column<SlotAssetIdx>,
    pub tree_seq_idx: Column<TreeSeqIdx>,
    pub trees_gaps: Column<TreesGaps>,
    pub token_metadata_edition_cbor: Column<TokenMetadataEdition>,
    pub token_accounts: Column<TokenAccount>,
    pub token_account_owner_idx: Column<TokenAccountOwnerIdx>,
    pub token_account_mint_owner_idx: Column<TokenAccountMintOwnerIdx>,
    pub asset_signature: Column<AssetSignature>,
    pub batch_mint_to_verify: Column<BatchMintToVerify>,
    pub failed_batch_mints: Column<FailedBatchMint>,
    pub batch_mints: Column<BatchMintWithStaker>,
    pub migration_version: Column<MigrationVersions>,
    pub token_prices: Column<TokenPrice>,
    pub asset_previews: Column<AssetPreviews>,
    pub urls_to_download: Column<UrlToDownload>,
    pub schedules: Column<ScheduledJob>,
    pub inscriptions: Column<Inscription>,
    pub inscription_data: Column<InscriptionData>,
    pub leaf_signature: Column<LeafSignature>,
    pub spl_mints: Column<SplMint>,
    assets_update_last_seq: AtomicU64,
    join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
    red_metrics: Arc<RequestErrorDurationMetrics>,
}

impl Storage {
    fn new(
        db: Arc<DB>,
        join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
    ) -> Self {
        let asset_static_data = Self::column(db.clone(), red_metrics.clone());
        let asset_dynamic_data = Self::column(db.clone(), red_metrics.clone());
        let asset_dynamic_data_deprecated = Self::column(db.clone(), red_metrics.clone());
        let asset_data = Self::column(db.clone(), red_metrics.clone());
        let metadata_mint_map = Self::column(db.clone(), red_metrics.clone());
        let asset_authority_data = Self::column(db.clone(), red_metrics.clone());
        let asset_authority_deprecated = Self::column(db.clone(), red_metrics.clone());
        let asset_owner_data = Self::column(db.clone(), red_metrics.clone());
        let asset_owner_data_deprecated = Self::column(db.clone(), red_metrics.clone());
        let asset_leaf_data = Self::column(db.clone(), red_metrics.clone());
        let asset_collection_data = Self::column(db.clone(), red_metrics.clone());
        let asset_collection_data_deprecated = Self::column(db.clone(), red_metrics.clone());
        let asset_offchain_data = Self::column(db.clone(), red_metrics.clone());

        let cl_items = Self::column(db.clone(), red_metrics.clone());
        let cl_leafs = Self::column(db.clone(), red_metrics.clone());

        let bubblegum_slots = Self::column(db.clone(), red_metrics.clone());
        let ingestable_slots = Self::column(db.clone(), red_metrics.clone());
        let force_reingestable_slots = Self::column(db.clone(), red_metrics.clone());
        let raw_blocks = Self::column(db.clone(), red_metrics.clone());
        let assets_update_idx = Self::column(db.clone(), red_metrics.clone());
        let slot_asset_idx = Self::column(db.clone(), red_metrics.clone());
        let tree_seq_idx = Self::column(db.clone(), red_metrics.clone());
        let trees_gaps = Self::column(db.clone(), red_metrics.clone());
        let token_metadata_edition_cbor = Self::column(db.clone(), red_metrics.clone());
        let asset_static_data_deprecated = Self::column(db.clone(), red_metrics.clone());
        let asset_signature = Self::column(db.clone(), red_metrics.clone());
        let token_accounts = Self::column(db.clone(), red_metrics.clone());
        let token_account_owner_idx = Self::column(db.clone(), red_metrics.clone());
        let token_account_mint_owner_idx = Self::column(db.clone(), red_metrics.clone());
        let batch_mint_to_verify = Self::column(db.clone(), red_metrics.clone());
        let failed_batch_mints = Self::column(db.clone(), red_metrics.clone());
        let batch_mints = Self::column(db.clone(), red_metrics.clone());
        let migration_version = Self::column(db.clone(), red_metrics.clone());
        let token_prices = Self::column(db.clone(), red_metrics.clone());
        let asset_previews = Self::column(db.clone(), red_metrics.clone());
        let urls_to_download = Self::column(db.clone(), red_metrics.clone());
        let schedules = Self::column(db.clone(), red_metrics.clone());
        let inscriptions = Self::column(db.clone(), red_metrics.clone());
        let inscription_data = Self::column(db.clone(), red_metrics.clone());
        let leaf_signature = Self::column(db.clone(), red_metrics.clone());
        let spl_mints = Self::column(db.clone(), red_metrics.clone());

        Self {
            asset_static_data,
            asset_dynamic_data,
            asset_dynamic_data_deprecated,
            asset_data,
            metadata_mint_map,
            asset_authority_data,
            asset_authority_deprecated,
            asset_owner_data,
            asset_owner_data_deprecated,
            asset_leaf_data,
            asset_collection_data,
            asset_collection_data_deprecated,
            asset_offchain_data,
            cl_items,
            cl_leafs,
            bubblegum_slots,
            ingestable_slots,
            force_reingestable_slots,
            raw_blocks_cbor: raw_blocks,
            db,
            assets_update_idx,
            slot_asset_idx,
            assets_update_last_seq: AtomicU64::new(0),
            join_set,
            tree_seq_idx,
            trees_gaps,
            token_metadata_edition_cbor,
            token_accounts,
            token_account_owner_idx,
            asset_static_data_deprecated,
            red_metrics,
            asset_signature,
            token_account_mint_owner_idx,
            batch_mint_to_verify,
            failed_batch_mints,
            batch_mints,
            migration_version,
            token_prices,
            asset_previews,
            urls_to_download,
            schedules,
            inscriptions,
            inscription_data,
            leaf_signature,
            spl_mints,
        }
    }

    pub fn open<P>(
        db_path: P,
        join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        migration_state: MigrationState,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let cf_descriptors = Self::create_cf_descriptors(&migration_state);
        let db = Arc::new(DB::open_cf_descriptors(
            &Self::get_db_options(),
            db_path,
            cf_descriptors,
        )?);
        Ok(Self::new(db, join_set, red_metrics))
    }

    pub fn open_secondary(
        primary_path: &str,
        secondary_path: &str,
        join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        migration_state: MigrationState,
    ) -> Result<Self> {
        let cf_descriptors = Self::create_cf_descriptors(&migration_state);
        let db = Arc::new(DB::open_cf_descriptors_as_secondary(
            &Self::get_db_options(),
            primary_path,
            secondary_path,
            cf_descriptors,
        )?);
        Ok(Self::new(db, join_set, red_metrics))
    }

    pub fn open_readonly_with_cfs(
        db_path: &str,
        c_names: Vec<&str>,
        join_set: Arc<Mutex<JoinSet<core::result::Result<(), tokio::task::JoinError>>>>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
    ) -> Result<Self> {
        let db = Arc::new(Self::open_readonly_with_cfs_only_db(db_path, c_names)?);
        Ok(Self::new(db, join_set, red_metrics))
    }

    pub fn open_readonly_with_cfs_only_db<P>(db_path: P, c_names: Vec<&str>) -> Result<DB>
    where
        P: AsRef<Path>,
    {
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = c_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Self::get_default_cf_options()))
            .collect();
        DB::open_cf_descriptors_read_only(&Self::get_db_options(), db_path, cf_descriptors, false)
            .map_err(StorageError::RocksDb)
    }

    fn create_cf_descriptors(migration_state: &MigrationState) -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::new_cf_descriptor::<OffChainData>(migration_state),
            Self::new_cf_descriptor::<AssetCompleteDetails>(migration_state),
            Self::new_cf_descriptor::<MetadataMintMap>(migration_state),
            Self::new_cf_descriptor::<asset::AssetLeaf>(migration_state),
            Self::new_cf_descriptor::<cl_items::ClItem>(migration_state),
            Self::new_cf_descriptor::<cl_items::ClLeaf>(migration_state),
            Self::new_cf_descriptor::<bubblegum_slots::BubblegumSlots>(migration_state),
            Self::new_cf_descriptor::<asset::AssetsUpdateIdx>(migration_state),
            Self::new_cf_descriptor::<asset::SlotAssetIdx>(migration_state),
            Self::new_cf_descriptor::<signature_client::SignatureIdx>(migration_state),
            Self::new_cf_descriptor::<RawBlock>(migration_state),
            Self::new_cf_descriptor::<parameters::ParameterColumn<u64>>(migration_state),
            Self::new_cf_descriptor::<bubblegum_slots::IngestableSlots>(migration_state),
            Self::new_cf_descriptor::<bubblegum_slots::ForceReingestableSlots>(migration_state),
            Self::new_cf_descriptor::<TreeSeqIdx>(migration_state),
            Self::new_cf_descriptor::<TreesGaps>(migration_state),
            Self::new_cf_descriptor::<TokenMetadataEdition>(migration_state),
            Self::new_cf_descriptor::<AssetSignature>(migration_state),
            Self::new_cf_descriptor::<TokenAccount>(migration_state),
            Self::new_cf_descriptor::<TokenAccountOwnerIdx>(migration_state),
            Self::new_cf_descriptor::<TokenAccountMintOwnerIdx>(migration_state),
            Self::new_cf_descriptor::<MigrationVersions>(migration_state),
            Self::new_cf_descriptor::<BatchMintToVerify>(migration_state),
            Self::new_cf_descriptor::<FailedBatchMint>(migration_state),
            Self::new_cf_descriptor::<BatchMintWithStaker>(migration_state),
            Self::new_cf_descriptor::<TokenPrice>(migration_state),
            Self::new_cf_descriptor::<AssetPreviews>(migration_state),
            Self::new_cf_descriptor::<UrlToDownload>(migration_state),
            Self::new_cf_descriptor::<ScheduledJob>(migration_state),
            Self::new_cf_descriptor::<Inscription>(migration_state),
            Self::new_cf_descriptor::<InscriptionData>(migration_state),
            Self::new_cf_descriptor::<LeafSignature>(migration_state),
            Self::new_cf_descriptor::<SplMint>(migration_state),
        ]
    }

    fn new_cf_descriptor<C: TypedColumn>(
        migration_state: &MigrationState,
    ) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options::<C>(migration_state))
    }

    pub fn column<C>(backend: Arc<DB>, red_metrics: Arc<RequestErrorDurationMetrics>) -> Column<C>
    where
        C: TypedColumn,
        <C as TypedColumn>::ValueType: 'static,
        <C as TypedColumn>::ValueType: Clone,
        <C as TypedColumn>::KeyType: 'static,
    {
        Column {
            backend,
            column: PhantomData,
            red_metrics,
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
        options.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);

        options
    }

    fn get_default_cf_options() -> Options {
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
        cf_options
    }

    fn get_cf_options<C: TypedColumn>(migration_state: &MigrationState) -> Options {
        let mut cf_options = Self::get_default_cf_options();

        if matches!(migration_state, &MigrationState::CreateColumnFamilies) {
            cf_options.set_merge_operator_associative(
                &format!("merge_fn_merge_{}", C::NAME.to_snake_case()),
                asset::AssetStaticDetails::merge_keep_existing,
            );
            return cf_options;
        }
        // Optional merges
        match C::NAME {
            // todo: add migration version
            asset::AssetCompleteDetails::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_complete_details",
                    asset::merge_complete_details_fb_simplified,
                );
            }
            AssetStaticDetails::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_static_details",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            asset::AssetDynamicDetails::NAME => {
                let mf = match migration_state {
                    MigrationState::Version(version) => match *version {
                        CollectionAuthorityMigration::VERSION
                            ..=ExternalPluginsMigration::VERSION => {
                            AssetDynamicDetailsV0::merge_dynamic_details
                        }
                        CleanCollectionAuthoritiesMigration::VERSION
                            ..=DynamicDataToken2022MintExtentionsMigration::VERSION => {
                            AssetDynamicDetailsWithoutExtentions::merge_dynamic_details
                        }
                        _ => asset::AssetDynamicDetails::merge_dynamic_details,
                    },
                    MigrationState::Last => asset::AssetDynamicDetails::merge_dynamic_details,
                    MigrationState::CreateColumnFamilies => {
                        asset::AssetStaticDetails::merge_keep_existing
                    }
                };
                cf_options.set_merge_operator_associative("merge_fn_merge_dynamic_details", mf);
            }
            asset::AssetDynamicDetailsDeprecated::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_dynamic_details_deprecated",
                    AssetStaticDetails::merge_keep_existing,
                );
            }
            asset::AssetAuthorityDeprecated::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_asset_authority_deprecated",
                    AssetStaticDetails::merge_keep_existing,
                );
            }
            asset::AssetCollectionDeprecated::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_asset_collection_deprecated",
                    AssetStaticDetails::merge_keep_existing,
                );
            }
            asset::AssetAuthority::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_asset_authorities",
                    asset::AssetAuthority::merge_asset_authorities,
                );
            }
            asset::AssetOwner::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_asset_owner",
                    asset::AssetOwner::merge_asset_owner,
                );
            }
            asset::AssetLeaf::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_merge_asset_leaf",
                    asset::AssetLeaf::merge_asset_leaf,
                );
            }
            asset::AssetCollection::NAME => {
                let mf = if matches!(
                    migration_state,
                    &MigrationState::Version(CollectionAuthorityMigration::VERSION)
                ) {
                    AssetCollectionVersion0::merge_asset_collection
                } else {
                    asset::AssetCollection::merge_asset_collection
                };
                cf_options.set_merge_operator_associative("merge_fn_asset_collection", mf);
            }
            cl_items::ClItem::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_cl_item",
                    cl_items::ClItem::merge_cl_items,
                );
            }
            ParameterColumn::<u64>::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_top_parameter_column",
                    parameters::merge_top_parameter,
                );
            }
            AssetOwnerDeprecated::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_asset_owner_deprecated_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            MetadataMintMap::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_metadata_mint_map_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            OffChainData::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_off_chain_data_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            cl_items::ClLeaf::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_cl_leaf_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            bubblegum_slots::BubblegumSlots::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_bubblegum_slots_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            bubblegum_slots::IngestableSlots::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_ingestable_slots_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            bubblegum_slots::ForceReingestableSlots::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_force_reingestable_slots_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            RawBlock::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_raw_block_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            AssetsUpdateIdx::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_assets_update_idx_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            SlotAssetIdx::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_slot_asset_idx_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            TreeSeqIdx::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_tree_seq_idx_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            TreesGaps::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_trees_gaps_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            TokenMetadataEdition::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_metadata_edition_keep_existing",
                    crate::editions::merge_token_metadata_edition,
                );
            }
            AssetStaticDetailsDeprecated::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_asset_static_deprecated_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            AssetSignature::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_asset_signature_keep_existing",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            TokenAccount::NAME => {
                let mf = match migration_state {
                    MigrationState::Version(version) => match *version {
                        CollectionAuthorityMigration::VERSION
                            ..=TokenAccounts2022ExtentionsMigration::VERSION => {
                            AssetDynamicDetailsV0::merge_dynamic_details
                        }
                        _ => token_accounts::merge_token_accounts,
                    },
                    MigrationState::Last => crate::token_accounts::merge_token_accounts,
                    MigrationState::CreateColumnFamilies => {
                        asset::AssetStaticDetails::merge_keep_existing
                    }
                };
                cf_options.set_merge_operator_associative("merge_fn_token_accounts", mf);
            }
            TokenAccountOwnerIdx::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_accounts_owner_idx",
                    TokenAccountOwnerIdx::merge_values,
                );
            }
            TokenAccountMintOwnerIdx::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_accounts_mint_owner_idx",
                    TokenAccountMintOwnerIdx::merge_values,
                );
            }
            BatchMintToVerify::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_batch_mint_to_verify",
                    batch_mint::merge_batch_mint_to_verify,
                );
            }
            FailedBatchMint::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_failed_batch_mint",
                    batch_mint::merge_failed_batch_mint,
                );
            }
            BatchMintWithStaker::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_downloaded_batch_mint",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            MigrationVersions::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_migration_versions",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            TokenPrice::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_prices",
                    asset::AssetStaticDetails::merge_keep_existing,
                );
            }
            Inscription::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_inscriptions",
                    Inscription::merge_values,
                );
            }
            InscriptionData::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_token_inscription_data",
                    InscriptionData::merge_values,
                );
            }
            LeafSignature::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_leaf_signature",
                    LeafSignature::merge_leaf_signatures,
                );
            }
            SplMint::NAME => {
                cf_options.set_merge_operator_associative(
                    "merge_fn_spl_mint",
                    token_accounts::merge_mints,
                );
            }
            _ => {}
        }
        cf_options
    }
}
