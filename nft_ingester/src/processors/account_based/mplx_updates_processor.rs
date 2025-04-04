use std::{collections::HashMap, sync::Arc};

use blockbuster::token_metadata::types::TokenStandard;
use entities::{
    enums::{ChainMutability, RoyaltyTargetType, SpecificationAssetClass, TokenMetadataEdition},
    models::{BurntMetadataSlot, ChainDataV1, Creator, MetadataInfo, UpdateVersion, Updated, Uses},
};
use metrics_utils::IngesterMetricsConfig;
use mpl_token_metadata::accounts::MasterEdition;
use rocks_db::{
    batch_savers::{BatchSaveStorage, MetadataModels},
    columns::asset::{
        AssetAuthority, AssetCollection, AssetCompleteDetails, AssetDynamicDetails,
        AssetStaticDetails, MetadataMintMap,
    },
    errors::StorageError,
};
use serde_json::json;
use solana_program::pubkey::Pubkey;
use tokio::time::Instant;
use usecase::save_metrics::result_to_metrics;

pub struct MplxAccountsProcessor {
    metrics: Arc<IngesterMetricsConfig>,
}

impl MplxAccountsProcessor {
    pub fn new(metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { metrics }
    }

    pub fn transform_and_store_burnt_metadata(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        burnt_metadata_slot: &BurntMetadataSlot,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self.mark_metadata_as_burnt(storage, key, burnt_metadata_slot);

        result_to_metrics(self.metrics.clone(), &res, "burn_metadata_merge_with_batch");
        self.metrics.set_latency(
            "burn_metadata_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn transform_and_store_metadata_account(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        metadata_info: &MetadataInfo,
        well_known_fungible_accounts: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let metadata_models =
            self.create_rocks_metadata_models(key, metadata_info, well_known_fungible_accounts);

        let begin_processing = Instant::now();
        let asset = AssetCompleteDetails::from(&metadata_models);
        let res = storage.store_metadata_models(&asset, metadata_models.metadata_mint);
        result_to_metrics(self.metrics.clone(), &res, "metadata_accounts_merge_with_batch");
        self.metrics.set_latency(
            "metadata_accounts_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn transform_and_store_edition_account(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        edition: &TokenMetadataEdition,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = storage.store_edition(key, edition);

        result_to_metrics(self.metrics.clone(), &res, "editions_merge_with_batch");
        self.metrics.set_latency(
            "editions_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn create_rocks_metadata_models(
        &self,
        key: Pubkey,
        metadata_info: &MetadataInfo,
        wellknown_fungible_accounts: &HashMap<String, String>,
    ) -> MetadataModels {
        let mut models = MetadataModels::default();

        let metadata = metadata_info.metadata.clone();
        let mint = metadata.mint;
        models.metadata_mint = Some(MetadataMintMap { pubkey: key, mint_key: mint });

        let data = metadata.clone();
        let authority = metadata.update_authority;
        let uri = data.uri.trim().replace('\0', "");

        let class = match metadata.token_standard {
            Some(TokenStandard::NonFungible) => SpecificationAssetClass::Nft,
            Some(TokenStandard::FungibleAsset) => SpecificationAssetClass::FungibleAsset,
            Some(TokenStandard::Fungible) => SpecificationAssetClass::FungibleToken,
            Some(TokenStandard::NonFungibleEdition) => SpecificationAssetClass::Nft,
            Some(TokenStandard::ProgrammableNonFungible) => {
                SpecificationAssetClass::ProgrammableNft
            },
            Some(TokenStandard::ProgrammableNonFungibleEdition) => {
                SpecificationAssetClass::ProgrammableNft
            },
            _ => {
                if wellknown_fungible_accounts.contains_key(&mint.to_string()) {
                    SpecificationAssetClass::FungibleToken
                } else {
                    SpecificationAssetClass::Unknown
                }
            },
        };

        models.asset_static = Some(AssetStaticDetails {
            pubkey: mint,
            specification_asset_class: class,
            royalty_target_type: RoyaltyTargetType::Creators,
            created_at: metadata_info.slot_updated as i64,
            edition_address: Some(MasterEdition::find_pda(&mint).0),
        });

        let mut chain_data = ChainDataV1 {
            name: data.name.clone(),
            symbol: data.symbol.clone(),
            edition_nonce: metadata.edition_nonce,
            primary_sale_happened: metadata.primary_sale_happened,
            token_standard: metadata.token_standard.map(|s| s.into()),
            uses: metadata.uses.map(|u| Uses {
                use_method: u.use_method.into(),
                remaining: u.remaining,
                total: u.total,
            }),
        };
        chain_data.sanitize();

        let chain_data = json!(chain_data);

        let chain_mutability = if metadata_info.metadata.is_mutable {
            ChainMutability::Mutable
        } else {
            ChainMutability::Immutable
        };

        // supply field saving inside process_mint_accs fn
        models.asset_dynamic = Some(AssetDynamicDetails {
            pubkey: mint,
            is_compressible: Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                false,
            ),
            is_compressed: Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                false,
            ),
            seq: None,
            // should not set this value for regular NFT updates
            was_decompressed: None,
            onchain_data: Some(Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                chain_data.to_string(),
            )),
            creators: Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                data.clone()
                    .creators
                    .unwrap_or_default()
                    .iter()
                    .map(|creator| Creator {
                        creator: creator.address,
                        creator_verified: creator.verified,
                        creator_share: creator.share,
                    })
                    .collect(),
            ),
            royalty_amount: Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                data.seller_fee_basis_points,
            ),
            url: Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                uri.clone(),
            ),
            lamports: Some(Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                metadata_info.lamports,
            )),
            executable: Some(Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                metadata_info.executable,
            )),
            metadata_owner: metadata_info.metadata_owner.clone().map(|m| {
                Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    m,
                )
            }),
            chain_mutability: Some(Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                chain_mutability,
            )),
            rent_epoch: Some(Updated::new(
                metadata_info.slot_updated,
                Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                metadata_info.rent_epoch,
            )),
            ..Default::default()
        });

        if let Some(c) = &metadata.collection {
            models.asset_collection = Some(AssetCollection {
                pubkey: mint,
                collection: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    c.key,
                ),
                is_collection_verified: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    c.verified,
                ),
                authority: Default::default(),
            });
        }

        models.asset_authority = Some(AssetAuthority {
            pubkey: mint,
            authority,
            slot_updated: metadata_info.slot_updated,
            write_version: Some(metadata_info.write_version),
        });

        models
    }

    fn mark_metadata_as_burnt(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        burnt_metadata_slot: &BurntMetadataSlot,
    ) -> Result<(), StorageError> {
        let Some(asset_dynamic_details) =
            storage.get_mint_map(key)?.map(|map| AssetDynamicDetails {
                pubkey: map.mint_key,
                is_burnt: Updated::new(
                    burnt_metadata_slot.slot_updated,
                    None, // once we got burn we may not even check write version
                    true,
                ),
                ..Default::default()
            })
        else {
            return Ok(());
        };

        storage.store_dynamic(&asset_dynamic_details)
    }
}
