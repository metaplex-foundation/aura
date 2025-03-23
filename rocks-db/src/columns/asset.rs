use std::{
    cmp::{max, Ordering},
    collections::HashMap,
};

use bincode::{deserialize, serialize};
use entities::{
    enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass},
    models::{
        AssetIndex, EditionData, EditionV1, SplMint, TokenAccount, UpdateVersion, Updated,
        UrlWithStatus,
    },
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use tracing::error;

use crate::{
    columns::{
        inscriptions::{Inscription, InscriptionData},
        offchain_data::OffChainData,
    },
    generated::asset_generated::asset as fb,
    key_encoders::{decode_pubkey, decode_u64_pubkey, encode_pubkey, encode_u64_pubkey},
    Result, ToFlatbuffersConverter, TypedColumn,
};

const MAX_OTHER_OWNERS: usize = 10;

#[derive(Debug)]
pub struct AssetSelectedMaps {
    pub asset_complete_details: HashMap<Pubkey, AssetCompleteDetails>,
    pub mpl_core_collections: HashMap<Pubkey, AssetCollection>,
    pub assets_leaf: HashMap<Pubkey, AssetLeaf>,
    pub offchain_data: HashMap<String, OffChainData>,
    pub urls: HashMap<Pubkey, String>,
    pub editions: HashMap<Pubkey, EditionData>,
    pub inscriptions: HashMap<Pubkey, Inscription>,
    pub inscriptions_data: HashMap<Pubkey, InscriptionData>,
    pub token_accounts: HashMap<Pubkey, TokenAccount>,
    pub spl_mints: HashMap<Pubkey, SplMint>,
}

#[derive(Debug)]
pub struct MasterAssetEditionsInfo {
    pub master_edition_address: Pubkey,
    pub supply: u64,
    pub max_supply: Option<u64>,
    pub editions: Vec<AssetEditionInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AssetEditionInfo {
    pub mint: Pubkey,
    pub edition_address: Pubkey,
    pub edition: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenMetadataEditionParentIndex {
    pub parent: Pubkey,
    pub edition: u64,
    pub asset_key: Pubkey,
    pub write_version: u64,
}

impl From<&EditionV1> for TokenMetadataEditionParentIndex {
    fn from(edition_v1: &EditionV1) -> Self {
        Self {
            parent: edition_v1.parent,
            edition: edition_v1.edition,
            asset_key: edition_v1.key,
            write_version: edition_v1.write_version,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct AssetCompleteDetails {
    pub pubkey: Pubkey,
    pub static_details: Option<AssetStaticDetails>,
    pub dynamic_details: Option<AssetDynamicDetails>,
    pub authority: Option<AssetAuthority>,
    pub owner: Option<AssetOwner>,
    pub collection: Option<AssetCollection>,
}

impl From<AssetStaticDetails> for AssetCompleteDetails {
    fn from(value: AssetStaticDetails) -> Self {
        Self {
            pubkey: value.pubkey,
            static_details: Some(value.clone()),
            dynamic_details: None,
            authority: None,
            owner: None,
            collection: None,
        }
    }
}

impl<'a> ToFlatbuffersConverter<'a> for AssetCompleteDetails {
    type Target = fb::AssetCompleteDetails<'a>;

    fn convert_to_fb_bytes(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let asset_complete_details = self.convert_to_fb(&mut builder);
        builder.finish_minimal(asset_complete_details);
        builder.finished_data().to_vec()
    }

    fn convert_to_fb(&self, builder: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::Target> {
        let pk = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let static_details =
            self.static_details.as_ref().map(|sd| asset_static_details_to_fb(builder, sd));
        let dynamic_details =
            self.dynamic_details.as_ref().map(|dd| asset_dynamic_details_to_fb(builder, dd));
        let authority = self.authority.as_ref().map(|a| asset_authority_to_fb(builder, a));
        let owner = self.owner.as_ref().map(|o| asset_owner_to_fb(builder, o));
        let collection = self.collection.as_ref().map(|c| asset_collection_to_fb(builder, c));
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey: pk,
                static_details,
                dynamic_details,
                authority,
                owner,
                collection,
                other_known_owners: None,
            },
        )
    }
}

impl<'a> From<fb::AssetCompleteDetails<'a>> for AssetCompleteDetails {
    fn from(value: fb::AssetCompleteDetails<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        AssetCompleteDetails {
            pubkey,
            static_details: value.static_details().map(From::from),
            dynamic_details: value.dynamic_details().map(From::from),
            authority: value.authority().map(From::from),
            owner: value.owner().map(From::from),
            collection: value.collection().map(From::from),
        }
    }
}

impl<'a> From<fb::AssetStaticDetails<'a>> for AssetStaticDetails {
    fn from(value: fb::AssetStaticDetails<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        let edition_address =
            value.edition_address().map(|ea| Pubkey::try_from(ea.bytes()).unwrap());
        AssetStaticDetails {
            pubkey,
            specification_asset_class: value.specification_asset_class().into(),
            royalty_target_type: value.royalty_target_type().into(),
            created_at: value.created_at(),
            edition_address,
        }
    }
}

impl<'a> From<fb::AssetDynamicDetails<'a>> for AssetDynamicDetails {
    fn from(value: fb::AssetDynamicDetails<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        let is_compressible = value.is_compressible().map(updated_bool_from_fb).unwrap();
        let is_compressed = value.is_compressed().map(updated_bool_from_fb).unwrap();
        let is_frozen = value.is_frozen().map(updated_bool_from_fb).unwrap();
        let supply = value.supply().map(updated_u64_from_fb);
        let seq = value.seq().map(updated_u64_from_fb);
        let is_burnt = value.is_burnt().map(updated_bool_from_fb).unwrap();
        let was_decompressed = value.was_decompressed().map(updated_bool_from_fb);
        let onchain_data = value.onchain_data().and_then(updated_string_from_fb);
        let creators = value.creators().map(updated_creators_from_fb).unwrap();
        let royalty_amount = value.royalty_amount().map(updated_u16_from_fb).unwrap();
        let url = value.url().and_then(updated_string_from_fb).unwrap();
        let chain_mutability = value.chain_mutability().map(updated_chain_mutability_from_fb);
        let lamports = value.lamports().map(updated_u64_from_fb);
        let executable = value.executable().map(updated_bool_from_fb);
        let metadata_owner = value.metadata_owner().and_then(updated_string_from_fb);
        let raw_name = value.raw_name().and_then(updated_string_from_fb);
        let mpl_core_plugins = value.mpl_core_plugins().and_then(updated_string_from_fb);
        let mpl_core_unknown_plugins =
            value.mpl_core_unknown_plugins().and_then(updated_string_from_fb);
        let rent_epoch = value.rent_epoch().map(updated_u64_from_fb);
        let num_minted = value.num_minted().map(updated_u32_from_fb);
        let current_size = value.current_size().map(updated_u32_from_fb);
        let plugins_json_version = value.plugins_json_version().map(updated_u32_from_fb);
        let mpl_core_external_plugins =
            value.mpl_core_external_plugins().and_then(updated_string_from_fb);
        let mpl_core_unknown_external_plugins =
            value.mpl_core_unknown_external_plugins().and_then(updated_string_from_fb);
        let mint_extensions = value.mint_extensions().and_then(updated_string_from_fb);
        AssetDynamicDetails {
            pubkey,
            is_compressible,
            is_compressed,
            is_frozen,
            supply,
            seq,
            is_burnt,
            was_decompressed,
            onchain_data,
            creators,
            royalty_amount,
            url,
            chain_mutability,
            lamports,
            executable,
            metadata_owner,
            raw_name,
            mpl_core_plugins,
            mpl_core_unknown_plugins,
            rent_epoch,
            num_minted,
            current_size,
            plugins_json_version,
            mpl_core_external_plugins,
            mpl_core_unknown_external_plugins,
            mint_extensions,
        }
    }
}

impl<'a> From<fb::AssetAuthority<'a>> for AssetAuthority {
    fn from(value: fb::AssetAuthority<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        let v;
        // using unsafe because the generated code does not have a safe way to get the optional value without default
        unsafe {
            v = value._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None);
        }
        let authority = Pubkey::try_from(value.authority().unwrap().bytes()).unwrap();
        AssetAuthority { pubkey, authority, slot_updated: value.slot_updated(), write_version: v }
    }
}

impl<'a> From<fb::AssetOwner<'a>> for AssetOwner {
    fn from(value: fb::AssetOwner<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        let owner = value.owner().map(updated_optional_pubkey_from_fb).unwrap();
        let delegate = value.delegate().map(updated_optional_pubkey_from_fb).unwrap();
        let owner_type = value.owner_type().map(updated_owner_type_from_fb).unwrap();
        let owner_delegate_seq =
            value.owner_delegate_seq().map(updated_optional_u64_from_fb).unwrap();
        let is_current_owner =
            value.is_current_owner().map(updated_bool_from_fb).unwrap_or_default();
        AssetOwner { pubkey, owner, delegate, owner_type, owner_delegate_seq, is_current_owner }
    }
}

impl<'a> From<fb::AssetCollection<'a>> for AssetCollection {
    fn from(value: fb::AssetCollection<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        let collection = value.collection().map(updated_pubkey_from_fb).unwrap();
        let is_collection_verified =
            value.is_collection_verified().map(updated_bool_from_fb).unwrap();
        let authority = value.authority().map(updated_optional_pubkey_from_fb).unwrap();
        AssetCollection { pubkey, collection, is_collection_verified, authority }
    }
}

impl AssetStaticDetails {
    pub fn convert_to_fb<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb::AssetCompleteDetails<'a>> {
        let pk = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let static_details = asset_static_details_to_fb(builder, self);
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey: pk,
                static_details: Some(static_details),
                dynamic_details: None,
                authority: None,
                owner: None,
                collection: None,
                other_known_owners: None,
            },
        )
    }
}

impl AssetDynamicDetails {
    pub fn convert_to_fb<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb::AssetCompleteDetails<'a>> {
        let pk = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let dynamic_details = Some(asset_dynamic_details_to_fb(builder, self));
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey: pk,
                static_details: None,
                dynamic_details,
                authority: None,
                owner: None,
                collection: None,
                other_known_owners: None,
            },
        )
    }
}

impl AssetAuthority {
    pub fn convert_to_fb<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb::AssetCompleteDetails<'a>> {
        let pubkey = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let authority = Some(asset_authority_to_fb(builder, self));
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey,
                static_details: None,
                dynamic_details: None,
                authority,
                owner: None,
                collection: None,
                other_known_owners: None,
            },
        )
    }
}

impl AssetOwner {
    pub fn convert_to_fb<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb::AssetCompleteDetails<'a>> {
        let pubkey = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let owner = Some(asset_owner_to_fb(builder, self));
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey,
                static_details: None,
                dynamic_details: None,
                authority: None,
                owner,
                collection: None,
                other_known_owners: None,
            },
        )
    }
}

impl AssetCollection {
    pub fn convert_to_fb<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb::AssetCompleteDetails<'a>> {
        let pubkey = Some(builder.create_vector(&self.pubkey.to_bytes()));
        let collection = Some(asset_collection_to_fb(builder, self));
        fb::AssetCompleteDetails::create(
            builder,
            &fb::AssetCompleteDetailsArgs {
                pubkey,
                static_details: None,
                dynamic_details: None,
                authority: None,
                owner: None,
                collection,
                other_known_owners: None,
            },
        )
    }
}

fn asset_static_details_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    static_details: &AssetStaticDetails,
) -> WIPOffset<fb::AssetStaticDetails<'a>> {
    let pubkey_bytes = pubkey_to_bytes(&static_details.pubkey);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    let edition_address_fb = static_details.edition_address.as_ref().map(|ea| {
        let ea_bytes = pubkey_to_bytes(ea);
        builder.create_vector(&ea_bytes)
    });

    fb::AssetStaticDetails::create(
        builder,
        &fb::AssetStaticDetailsArgs {
            pubkey: Some(pubkey_fb),
            specification_asset_class: static_details.specification_asset_class.into(),
            royalty_target_type: static_details.royalty_target_type.into(),
            created_at: static_details.created_at,
            edition_address: edition_address_fb,
        },
    )
}

fn asset_dynamic_details_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    dynamic_details: &AssetDynamicDetails,
) -> WIPOffset<fb::AssetDynamicDetails<'a>> {
    let pubkey_bytes = pubkey_to_bytes(&dynamic_details.pubkey);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    let is_compressible_fb = updated_bool_to_fb(builder, &dynamic_details.is_compressible);
    let is_compressed_fb = updated_bool_to_fb(builder, &dynamic_details.is_compressed);
    let is_frozen_fb = updated_bool_to_fb(builder, &dynamic_details.is_frozen);

    // Optional fields
    let supply_fb =
        dynamic_details.supply.as_ref().map(|supply| updated_u64_to_fb(builder, supply));
    let seq_fb = dynamic_details.seq.as_ref().map(|seq| updated_u64_to_fb(builder, seq));
    let is_burnt_fb = updated_bool_to_fb(builder, &dynamic_details.is_burnt);
    let was_decompressed_fb = dynamic_details
        .was_decompressed
        .as_ref()
        .map(|was_dec| updated_bool_to_fb(builder, was_dec));
    let onchain_data_fb = dynamic_details
        .onchain_data
        .as_ref()
        .map(|onchain_data| updated_string_to_fb(builder, onchain_data));
    let creators_fb = updated_creators_to_fb(builder, &dynamic_details.creators);
    let royalty_amount_fb = updated_u16_to_u32_fb(builder, &dynamic_details.royalty_amount);
    let url_fb = updated_string_to_fb(builder, &dynamic_details.url);
    let chain_mutability_fb = dynamic_details
        .chain_mutability
        .as_ref()
        .map(|chain_mutability| updated_chain_mutability_to_fb(builder, chain_mutability));
    let lamports_fb =
        dynamic_details.lamports.as_ref().map(|lamports| updated_u64_to_fb(builder, lamports));
    let executable_fb = dynamic_details
        .executable
        .as_ref()
        .map(|executable| updated_bool_to_fb(builder, executable));
    let metadata_owner_fb = dynamic_details
        .metadata_owner
        .as_ref()
        .map(|metadata_owner| updated_string_to_fb(builder, metadata_owner));
    let raw_name_fb =
        dynamic_details.raw_name.as_ref().map(|raw_name| updated_string_to_fb(builder, raw_name));
    let mpl_core_plugins_fb = dynamic_details
        .mpl_core_plugins
        .as_ref()
        .map(|mpl_core_plugins| updated_string_to_fb(builder, mpl_core_plugins));
    let mpl_core_unknown_plugins_fb = dynamic_details
        .mpl_core_unknown_plugins
        .as_ref()
        .map(|mpl_core_unknown_plugins| updated_string_to_fb(builder, mpl_core_unknown_plugins));
    let rent_epoch_fb = dynamic_details
        .rent_epoch
        .as_ref()
        .map(|rent_epoch| updated_u64_to_fb(builder, rent_epoch));
    let num_minted_fb = dynamic_details
        .num_minted
        .as_ref()
        .map(|num_minted| updated_u32_to_fb(builder, num_minted));
    let current_size_fb = dynamic_details
        .current_size
        .as_ref()
        .map(|current_size| updated_u32_to_fb(builder, current_size));
    let plugins_json_version_fb = dynamic_details
        .plugins_json_version
        .as_ref()
        .map(|plugins_json_version| updated_u32_to_fb(builder, plugins_json_version));
    let mpl_core_external_plugins_fb = dynamic_details
        .mpl_core_external_plugins
        .as_ref()
        .map(|mpl_core_external_plugins| updated_string_to_fb(builder, mpl_core_external_plugins));
    let mpl_core_unknown_external_plugins_fb = dynamic_details
        .mpl_core_unknown_external_plugins
        .as_ref()
        .map(|mpl_core_unknown_external_plugins| {
            updated_string_to_fb(builder, mpl_core_unknown_external_plugins)
        });
    let mint_extensions_fb = dynamic_details
        .mint_extensions
        .as_ref()
        .map(|mint_extensions| updated_string_to_fb(builder, mint_extensions));
    // Continue converting other fields similarly

    fb::AssetDynamicDetails::create(
        builder,
        &fb::AssetDynamicDetailsArgs {
            pubkey: Some(pubkey_fb),
            is_compressible: Some(is_compressible_fb),
            is_compressed: Some(is_compressed_fb),
            is_frozen: Some(is_frozen_fb),
            supply: supply_fb,
            seq: seq_fb,
            is_burnt: Some(is_burnt_fb),
            was_decompressed: was_decompressed_fb,
            onchain_data: onchain_data_fb,
            creators: Some(creators_fb),
            royalty_amount: Some(royalty_amount_fb),
            url: Some(url_fb),
            chain_mutability: chain_mutability_fb,
            lamports: lamports_fb,
            executable: executable_fb,
            metadata_owner: metadata_owner_fb,
            raw_name: raw_name_fb,
            mpl_core_plugins: mpl_core_plugins_fb,
            mpl_core_unknown_plugins: mpl_core_unknown_plugins_fb,
            rent_epoch: rent_epoch_fb,
            num_minted: num_minted_fb,
            current_size: current_size_fb,
            plugins_json_version: plugins_json_version_fb,
            mpl_core_external_plugins: mpl_core_external_plugins_fb,
            mpl_core_unknown_external_plugins: mpl_core_unknown_external_plugins_fb,
            mint_extensions: mint_extensions_fb,
        },
    )
}

fn asset_authority_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    authority: &AssetAuthority,
) -> WIPOffset<fb::AssetAuthority<'a>> {
    let pubkey_bytes = pubkey_to_bytes(&authority.pubkey);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    let authority_bytes = pubkey_to_bytes(&authority.authority);
    let authority_fb = builder.create_vector(&authority_bytes);

    let mut b = fb::AssetAuthorityBuilder::new(builder);
    if let Some(wv) = authority.write_version {
        b.add_write_version(wv);
    }
    b.add_slot_updated(authority.slot_updated);
    b.add_authority(authority_fb);
    b.add_pubkey(pubkey_fb);
    b.finish()
}
fn asset_owner_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    owner: &AssetOwner,
) -> WIPOffset<fb::AssetOwner<'a>> {
    let pubkey_bytes = pubkey_to_bytes(&owner.pubkey);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    let owner_fb = updated_optional_pubkey_to_fb(builder, &owner.owner);
    let delegate_fb = updated_optional_pubkey_to_fb(builder, &owner.delegate);
    let owner_type_fb = updated_owner_type_to_fb(builder, &owner.owner_type);
    let owner_delegate_seq_fb = updated_optional_u64_to_fb(builder, &owner.owner_delegate_seq);
    let is_current_owner = updated_bool_to_fb(builder, &owner.is_current_owner);

    fb::AssetOwner::create(
        builder,
        &fb::AssetOwnerArgs {
            pubkey: Some(pubkey_fb),
            owner: Some(owner_fb),
            delegate: Some(delegate_fb),
            owner_type: Some(owner_type_fb),
            owner_delegate_seq: Some(owner_delegate_seq_fb),
            is_current_owner: Some(is_current_owner),
        },
    )
}
fn asset_collection_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    collection: &AssetCollection,
) -> WIPOffset<fb::AssetCollection<'a>> {
    let pubkey_bytes = pubkey_to_bytes(&collection.pubkey);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    let collection_fb = updated_pubkey_to_fb(builder, &collection.collection);
    let is_collection_verified_fb = updated_bool_to_fb(builder, &collection.is_collection_verified);
    let authority_fb = updated_optional_pubkey_to_fb(builder, &collection.authority);

    fb::AssetCollection::create(
        builder,
        &fb::AssetCollectionArgs {
            pubkey: Some(pubkey_fb),
            collection: Some(collection_fb),
            is_collection_verified: Some(is_collection_verified_fb),
            authority: Some(authority_fb),
        },
    )
}

fn updated_bool_from_fb(updated: fb::UpdatedBool) -> Updated<bool> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value(),
    }
}

fn updated_bool_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<bool>,
) -> WIPOffset<fb::UpdatedBool<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedBool::create(
        builder,
        &fb::UpdatedBoolArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value,
        },
    )
}

fn updated_u64_from_fb(updated: fb::UpdatedU64) -> Updated<u64> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value(),
    }
}

fn updated_u64_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<u64>,
) -> WIPOffset<fb::UpdatedU64<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedU64::create(
        builder,
        &fb::UpdatedU64Args {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value,
        },
    )
}

fn updated_u32_from_fb(updated: fb::UpdatedU32) -> Updated<u32> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value(),
    }
}

fn updated_u32_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<u32>,
) -> WIPOffset<fb::UpdatedU32<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedU32::create(
        builder,
        &fb::UpdatedU32Args {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value,
        },
    )
}

fn updated_u16_from_fb(updated: fb::UpdatedU32) -> Updated<u16> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value() as u16,
    }
}

fn updated_u16_to_u32_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<u16>,
) -> WIPOffset<fb::UpdatedU32<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedU32::create(
        builder,
        &fb::UpdatedU32Args {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value as u32,
        },
    )
}

fn updated_string_from_fb(updated: fb::UpdatedString) -> Option<Updated<String>> {
    updated.value().map(|value| Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: value.to_string(),
    })
}

fn updated_string_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<String>,
) -> WIPOffset<fb::UpdatedString<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    let value = builder.create_string(&updated.value);

    fb::UpdatedString::create(
        builder,
        &fb::UpdatedStringArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: Some(value),
        },
    )
}

fn updated_pubkey_from_fb(updated: fb::UpdatedPubkey) -> Updated<Pubkey> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: Pubkey::try_from(updated.value().unwrap().bytes()).unwrap(),
    }
}

fn updated_pubkey_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<Pubkey>,
) -> WIPOffset<fb::UpdatedPubkey<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    let pubkey_bytes = pubkey_to_bytes(&updated.value);
    let pubkey_fb = builder.create_vector(&pubkey_bytes);

    fb::UpdatedPubkey::create(
        builder,
        &fb::UpdatedPubkeyArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: Some(pubkey_fb),
        },
    )
}

fn updated_optional_pubkey_from_fb(updated: fb::UpdatedOptionalPubkey) -> Updated<Option<Pubkey>> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value().map(|pubkey| Pubkey::try_from(pubkey.bytes()).unwrap()),
    }
}

fn updated_optional_pubkey_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<Option<Pubkey>>,
) -> WIPOffset<fb::UpdatedOptionalPubkey<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    let value = updated.value.as_ref().map(|pubkey| {
        let pubkey_bytes = pubkey_to_bytes(pubkey);
        builder.create_vector(&pubkey_bytes)
    });

    fb::UpdatedOptionalPubkey::create(
        builder,
        &fb::UpdatedOptionalPubkeyArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value,
        },
    )
}
fn updated_optional_u64_from_fb(updated: fb::UpdatedU64) -> Updated<Option<u64>> {
    let v;
    unsafe {
        v = updated._tab.get::<u64>(fb::UpdatedU64::VT_VALUE, None);
    }
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: v,
    }
}

fn updated_optional_u64_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<Option<u64>>,
) -> WIPOffset<fb::UpdatedU64<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);
    let mut ub = fb::UpdatedU64Builder::new(builder);
    ub.add_slot_updated(updated.slot_updated);
    ub.add_update_version(update_version);
    if let Some(value) = updated.value {
        ub.add_value(value);
    }
    ub.finish()
}

fn creator_from_fb(creator: fb::Creator) -> entities::models::Creator {
    entities::models::Creator {
        creator: Pubkey::try_from(creator.creator().unwrap().bytes()).unwrap(),
        creator_verified: creator.creator_verified(),
        creator_share: creator.creator_share() as u8,
    }
}

fn updated_creators_from_fb(
    updated: fb::UpdatedCreators,
) -> Updated<Vec<entities::models::Creator>> {
    let mut ve = Vec::new();
    if let Some(cc) = updated.value() {
        for creator in cc {
            ve.push(creator_from_fb(creator));
        }
    }
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: ve,
    }
}

fn updated_creators_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<Vec<entities::models::Creator>>,
) -> WIPOffset<fb::UpdatedCreators<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);
    let mut creators = Vec::with_capacity(updated.value.len());
    for creator in &updated.value {
        let pubkey_bytes = pubkey_to_bytes(&creator.creator);
        let pubkey_fb = builder.create_vector(&pubkey_bytes);

        let creator_fb = fb::Creator::create(
            builder,
            &fb::CreatorArgs {
                creator: Some(pubkey_fb),
                creator_verified: creator.creator_verified,
                creator_share: creator.creator_share as u32,
            },
        );
        creators.push(creator_fb);
    }

    let creators_fb = builder.create_vector(creators.as_slice());

    fb::UpdatedCreators::create(
        builder,
        &fb::UpdatedCreatorsArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: Some(creators_fb),
        },
    )
}

fn updated_owner_type_from_fb(updated: fb::UpdatedOwnerType) -> Updated<OwnerType> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value().into(),
    }
}

fn updated_owner_type_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<OwnerType>,
) -> WIPOffset<fb::UpdatedOwnerType<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedOwnerType::create(
        builder,
        &fb::UpdatedOwnerTypeArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value.into(),
        },
    )
}

fn updated_chain_mutability_from_fb(
    updated: fb::UpdatedChainMutability,
) -> Updated<ChainMutability> {
    Updated {
        slot_updated: updated.slot_updated(),
        update_version: updated.update_version().and_then(convert_update_version_from_fb),
        value: updated.value().into(),
    }
}

fn updated_chain_mutability_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    updated: &Updated<ChainMutability>,
) -> WIPOffset<fb::UpdatedChainMutability<'a>> {
    let update_version = convert_update_version_to_fb(builder, &updated.update_version);

    fb::UpdatedChainMutability::create(
        builder,
        &fb::UpdatedChainMutabilityArgs {
            slot_updated: updated.slot_updated,
            update_version: Some(update_version),
            value: updated.value.into(),
        },
    )
}

fn convert_update_version_to_fb<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    update_version: &Option<UpdateVersion>,
) -> WIPOffset<fb::UpdateVersion<'a>> {
    let (version_type, version_value) = match update_version {
        Some(UpdateVersion::Sequence(seq)) => (fb::UpdateVersionType::Sequence, *seq),
        Some(UpdateVersion::WriteVersion(wv)) => (fb::UpdateVersionType::WriteVersion, *wv),
        None => (fb::UpdateVersionType::None, 0),
    };

    fb::UpdateVersion::create(builder, &fb::UpdateVersionArgs { version_type, version_value })
}

fn convert_update_version_from_fb(update_version: fb::UpdateVersion) -> Option<UpdateVersion> {
    match update_version.version_type() {
        fb::UpdateVersionType::Sequence => {
            Some(UpdateVersion::Sequence(update_version.version_value()))
        },
        fb::UpdateVersionType::WriteVersion => {
            Some(UpdateVersion::WriteVersion(update_version.version_value()))
        },
        fb::UpdateVersionType::None => None,
        _ => None,
    }
}

fn pubkey_to_bytes(pubkey: &Pubkey) -> [u8; 32] {
    pubkey.to_bytes()
}

impl From<&AssetDynamicDetails> for AssetCompleteDetails {
    fn from(value: &AssetDynamicDetails) -> Self {
        Self {
            pubkey: value.pubkey,
            static_details: None,
            dynamic_details: Some(value.clone()),
            authority: None,
            owner: None,
            collection: None,
        }
    }
}

impl From<AssetAuthority> for AssetCompleteDetails {
    fn from(value: AssetAuthority) -> Self {
        Self {
            pubkey: value.pubkey,
            static_details: None,
            dynamic_details: None,
            authority: Some(value.clone()),
            owner: None,
            collection: None,
        }
    }
}

// impl From<AssetOwner> for AssetCompleteDetails {
//     fn from(value: AssetOwner) -> Self {
//         Self {
//             pubkey: value.pubkey, // todo: what do I do with this? For token accounts it's wrong
//             static_details: None,
//             dynamic_details: None,
//             authority: None,
//             owner: Some(value.clone()),
//             collection: None,
//         }
//     }
// }

impl From<AssetCollection> for AssetCompleteDetails {
    fn from(value: AssetCollection) -> Self {
        Self {
            pubkey: value.pubkey,
            static_details: None,
            dynamic_details: None,
            authority: None,
            owner: None,
            collection: Some(value.clone()),
        }
    }
}

impl AssetCompleteDetails {
    pub fn get_slot_updated(&self) -> u64 {
        // Collect the slot_updated values from all available fields
        let slots = [
            self.dynamic_details.as_ref().map(|d| d.get_slot_updated()),
            self.authority.as_ref().map(|a| a.slot_updated),
            self.owner.as_ref().map(|o| o.get_slot_updated()),
            self.collection.as_ref().map(|c| c.get_slot_updated()),
        ];
        // Filter out None values and find the maximum slot_updated
        slots.iter().filter_map(|&slot| slot).max().unwrap_or(0)
    }

    pub fn any_field_is_set(&self) -> bool {
        self.static_details.is_some()
            || self.dynamic_details.is_some()
            || self.authority.is_some()
            || self.owner.is_some()
            || self.collection.is_some()
    }

    pub fn to_index_without_url_checks(
        &self,
        mpl_core_collections: &HashMap<Pubkey, Pubkey>,
    ) -> AssetIndex {
        AssetIndex {
            pubkey: self.pubkey,
            specification_version: entities::enums::SpecificationVersions::V1,
            specification_asset_class: self
                .static_details
                .as_ref()
                .map(|a| a.specification_asset_class)
                .unwrap_or_default(),
            royalty_target_type: self
                .static_details
                .as_ref()
                .map(|a| a.royalty_target_type)
                .unwrap_or_default(),
            slot_created: self.static_details.as_ref().map(|a| a.created_at).unwrap_or_default(),
            is_compressible: self
                .dynamic_details
                .as_ref()
                .map(|d| d.is_compressible.value)
                .unwrap_or_default(),
            is_compressed: self
                .dynamic_details
                .as_ref()
                .map(|d| d.is_compressed.value)
                .unwrap_or_default(),
            is_frozen: self.dynamic_details.as_ref().map(|d| d.is_frozen.value).unwrap_or_default(),
            supply: self
                .dynamic_details
                .as_ref()
                .map(|d| d.supply.clone().map(|s| s.value as i64))
                .unwrap_or_default(),
            is_burnt: self.dynamic_details.as_ref().map(|d| d.is_burnt.value).unwrap_or_default(),
            creators: self
                .dynamic_details
                .as_ref()
                .map(|d| d.creators.clone().value)
                .unwrap_or_default(),
            royalty_amount: self
                .dynamic_details
                .as_ref()
                .map(|d| d.royalty_amount.value as i64)
                .unwrap_or_default(),
            slot_updated: self.get_slot_updated() as i64,
            metadata_url: self
                .dynamic_details
                .as_ref()
                .map(|d| UrlWithStatus::new(&d.url.value, false)),
            authority: self.authority.as_ref().map(|a| a.authority),
            owner: self.owner.as_ref().and_then(|o| o.owner.value),
            delegate: self.owner.as_ref().and_then(|o| o.delegate.value),
            owner_type: self.owner.as_ref().map(|o| o.owner_type.value),
            collection: self.collection.as_ref().map(|c| c.collection.value),
            is_collection_verified: self
                .collection
                .as_ref()
                .map(|c| c.is_collection_verified.value),
            update_authority: self
                .collection
                .as_ref()
                .and_then(|c| mpl_core_collections.get(&c.collection.value))
                .copied(),
            fungible_asset_mint: None,
            fungible_asset_balance: None,
        }
    }
}
// The following structures are used to store the asset data in the rocksdb database. The data is spread across multiple columns based on the update pattern.
// The final representation of the asset should be reconstructed by querying the database for the asset and its associated columns.
// Some values, like slot_updated should be calculated based on the latest update to the asset.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AssetStaticDetails {
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub created_at: i64,
    pub edition_address: Option<Pubkey>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetStaticDetailsDeprecated {
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct AssetDynamicDetails {
    pub pubkey: Pubkey,
    pub is_compressible: Updated<bool>,
    pub is_compressed: Updated<bool>,
    pub is_frozen: Updated<bool>,
    pub supply: Option<Updated<u64>>,
    pub seq: Option<Updated<u64>>,
    pub is_burnt: Updated<bool>,
    pub was_decompressed: Option<Updated<bool>>,
    pub onchain_data: Option<Updated<String>>,
    pub creators: Updated<Vec<entities::models::Creator>>,
    pub royalty_amount: Updated<u16>,
    pub url: Updated<String>,
    pub chain_mutability: Option<Updated<ChainMutability>>,
    pub lamports: Option<Updated<u64>>,
    pub executable: Option<Updated<bool>>,
    pub metadata_owner: Option<Updated<String>>,
    pub raw_name: Option<Updated<String>>,
    pub mpl_core_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
    pub mpl_core_external_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_external_plugins: Option<Updated<String>>,
    pub mint_extensions: Option<Updated<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetDynamicDetailsDeprecated {
    pub pubkey: Pubkey,
    pub is_compressible: Updated<bool>,
    pub is_compressed: Updated<bool>,
    pub is_frozen: Updated<bool>,
    pub supply: Option<Updated<u64>>,
    pub seq: Option<Updated<u64>>,
    pub is_burnt: Updated<bool>,
    pub was_decompressed: Updated<bool>,
    pub onchain_data: Option<Updated<String>>,
    pub creators: Updated<Vec<entities::models::Creator>>,
    pub royalty_amount: Updated<u16>,
    pub url: Updated<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetadataMintMap {
    // this is Metadata acc pubkey
    // it's PDA with next seeds ["metadata", metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s, mint_key]
    pub pubkey: Pubkey,
    pub mint_key: Pubkey,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct AssetAuthority {
    pub pubkey: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: u64,
    pub write_version: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetAuthorityDeprecated {
    pub pubkey: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct AssetOwner {
    pub pubkey: Pubkey,
    pub owner: Updated<Option<Pubkey>>,
    pub delegate: Updated<Option<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Updated<Option<u64>>,
    pub is_current_owner: Updated<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetOwnerDeprecated {
    pub pubkey: Pubkey,
    pub owner: Updated<Pubkey>,
    pub delegate: Option<Updated<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Option<Updated<u64>>,
}

/// Leaf information about compressed asset
/// Nonce - is basically the leaf index. It takes from tree supply.
/// NOTE: leaf index is not the same as node index. Leaf index is specifically the index of the leaf in the tree.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeaf {
    pub pubkey: Pubkey,
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
    pub collection_hash: Option<Hash>,
    pub asset_data_hash: Option<Hash>,
    pub flags: Option<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeafDeprecated {
    pub pubkey: Pubkey,
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SourcedAssetLeaf {
    pub leaf: AssetLeaf,
    pub is_from_finalized_source: bool,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AssetCollection {
    pub pubkey: Pubkey,
    pub collection: Updated<Pubkey>,
    pub is_collection_verified: Updated<bool>,
    pub authority: Updated<Option<Pubkey>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollectionDeprecated {
    pub pubkey: Pubkey,
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
    pub slot_updated: u64,
}

pub(crate) fn update_field<T: Clone>(current: &mut Updated<T>, new: &Updated<T>) {
    if current.update_version.is_some() && new.update_version.is_some() {
        match current
            .update_version
            .clone()
            .unwrap()
            .partial_cmp(&new.update_version.clone().unwrap())
        {
            Some(Ordering::Less) => {
                *current = new.clone();
                return;
            },
            Some(Ordering::Greater) => return,
            _ => {}, // types are different need to check slot
        }
    }

    if new.slot_updated > current.slot_updated {
        *current = new.clone();
    }
}

pub(crate) fn update_optional_field<T: Clone + Default>(
    current: &mut Option<Updated<T>>,
    new: &Option<Updated<T>>,
) {
    if current.clone().unwrap_or_default().update_version.is_some()
        && new.clone().unwrap_or_default().update_version.is_some()
    {
        match current
            .clone()
            .unwrap_or_default()
            .update_version
            .unwrap()
            .partial_cmp(&new.clone().unwrap_or_default().update_version.unwrap())
        {
            Some(Ordering::Less) => {
                *current = new.clone();
                return;
            },
            Some(Ordering::Greater) => return,
            _ => {}, // types are different need to check slot
        }
    }

    if new.clone().unwrap_or_default().slot_updated
        > current.clone().unwrap_or_default().slot_updated
    {
        *current = new.clone();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MplCoreCollectionAuthority {
    pub authority: Updated<Option<Pubkey>>,
}

impl TypedColumn for MplCoreCollectionAuthority {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "MPL_CORE_COLLECTION_AUTHORITY";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl MplCoreCollectionAuthority {
    pub fn merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                },
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
                },
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.authority, &new_val.authority);
                        current_val
                    } else {
                        new_val
                    });
                },
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize new_val: {}", e)
                },
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

impl TypedColumn for AssetStaticDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "ASSET_STATIC_V2"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetStaticDetailsDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "ASSET_STATIC"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetDynamicDetailsDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_DYNAMIC";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetCompleteDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_COMPLETE_DETAILS";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetDynamicDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_DYNAMIC_V2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for MetadataMintMap {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "METADATA_MINT_MAP";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetAuthorityDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_AUTHORITY";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetAuthority {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_AUTHORITY_V2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

/// Not a real merge operation. We just check if static info
/// already exist and if so we don't overwrite it.
impl AssetStaticDetails {
    pub fn merge_keep_existing(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        if let Some(existing_val) = existing_val {
            result = existing_val.to_vec();

            return Some(result);
        }

        if let Some(op) = operands.into_iter().next() {
            result = op.to_vec();

            return Some(result);
        }

        Some(result)
    }
}

impl AssetCompleteDetails {
    pub fn merge_complete_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        Self::merge_complete_details_raw(_new_key, existing_val, operands.iter())
    }

    pub fn merge_raw(existing: &mut Option<Self>, operands: &[Self]) {
        for op in operands {
            if let Some(ref mut current_val) = existing {
                // Merge dynamic_details
                if let Some(new_dynamic_details) = &op.dynamic_details {
                    if let Some(ref mut current_dynamic_details) = current_val.dynamic_details {
                        current_dynamic_details.merge(new_dynamic_details);
                    } else {
                        current_val.dynamic_details = Some(new_dynamic_details.to_owned());
                    }
                }

                // Keep existing static_details if present
                if current_val.static_details.is_none() {
                    current_val.static_details = op.static_details.clone();
                }

                // Merge authority
                if let Some(new_authority) = &op.authority {
                    if let Some(ref mut current_authority) = current_val.authority {
                        current_authority.merge(new_authority);
                    } else {
                        current_val.authority = Some(new_authority.clone());
                    }
                }

                // Merge owner
                if let Some(new_owner) = &op.owner {
                    if let Some(ref mut current_owner) = current_val.owner {
                        current_owner.merge(new_owner);
                    } else {
                        current_val.owner = Some(new_owner.clone());
                    }
                }

                // Merge collection
                if let Some(new_collection) = &op.collection {
                    if let Some(ref mut current_collection) = current_val.collection {
                        current_collection.merge(new_collection);
                    } else {
                        current_val.collection = Some(new_collection.clone());
                    }
                }
            } else {
                *existing = Some(op.clone());
            }
        }
    }

    pub fn merge_complete_details_raw<'a>(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: impl Iterator<Item = &'a [u8]>,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                },
                Err(e) => {
                    error!("RocksDB: AssetCompleteDetails deserialize existing_val: {}", e)
                },
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    if let Some(ref mut current_val) = result {
                        // Merge dynamic_details
                        if let Some(new_dynamic_details) = new_val.dynamic_details {
                            if let Some(ref mut current_dynamic_details) =
                                current_val.dynamic_details
                            {
                                current_dynamic_details.merge(&new_dynamic_details);
                            } else {
                                current_val.dynamic_details = Some(new_dynamic_details);
                            }
                        }

                        // Keep existing static_details if present
                        if current_val.static_details.is_none() {
                            current_val.static_details = new_val.static_details;
                        }

                        // Merge authority
                        if let Some(new_authority) = new_val.authority {
                            if let Some(ref mut current_authority) = current_val.authority {
                                current_authority.merge(&new_authority);
                            } else {
                                current_val.authority = Some(new_authority);
                            }
                        }

                        // Merge owner
                        if let Some(new_owner) = new_val.owner {
                            if let Some(ref mut current_owner) = current_val.owner {
                                current_owner.merge(&new_owner);
                            } else {
                                current_val.owner = Some(new_owner);
                            }
                        }

                        // Merge collection
                        if let Some(new_collection) = new_val.collection {
                            if let Some(ref mut current_collection) = current_val.collection {
                                current_collection.merge(&new_collection);
                            } else {
                                current_val.collection = Some(new_collection);
                            }
                        }
                    } else {
                        result = Some(new_val);
                    }
                },
                Err(e) => {
                    error!("RocksDB: AssetCompleteDetails deserialize new_val: {}", e)
                },
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

pub fn merge_complete_details_fb(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    merge_complete_details_fb_raw(_new_key, existing_val, operands.iter())
}
pub fn merge_complete_details_fb_through_proxy<'a>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: impl Iterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let mut existing_val = existing_val
        .and_then(|bytes| {
            fb::root_as_asset_complete_details(bytes)
                .map_err(|e| {
                    error!("RocksDB: AssetCompleteDetails deserialize existing_val: {}", e)
                })
                .ok()
        })
        .map(AssetCompleteDetails::from);
    AssetCompleteDetails::merge_raw(
        &mut existing_val,
        operands
            .filter_map(|op| fb::root_as_asset_complete_details(op).ok())
            .map(AssetCompleteDetails::from)
            .collect::<Vec<_>>()
            .as_slice(),
    );
    existing_val.map(|r| {
        let mut builder = FlatBufferBuilder::with_capacity(2500);
        let tt = r.convert_to_fb(&mut builder);
        builder.finish_minimal(tt);
        builder.finished_data().to_vec()
    })
}

macro_rules! create_updated_primitive_offset {
    ($func_name:ident, $updated_type:ident, $updated_args:ident) => {
        fn $func_name<'a>(
            builder: &mut flatbuffers::FlatBufferBuilder<'a>,
            updated: &fb::$updated_type<'a>,
        ) -> flatbuffers::WIPOffset<fb::$updated_type<'a>> {
            let update_version_offset = updated.update_version().map(|uv| {
                fb::UpdateVersion::create(
                    builder,
                    &fb::UpdateVersionArgs {
                        version_type: uv.version_type(),
                        version_value: uv.version_value(),
                    },
                )
            });

            fb::$updated_type::create(
                builder,
                &fb::$updated_args {
                    slot_updated: updated.slot_updated(),
                    update_version: update_version_offset,
                    value: updated.value(),
                },
            )
        }
    };
}
macro_rules! create_updated_offset {
    ($func_name:ident, $updated_type:ident, $updated_args:ident, $create_value_fn:expr) => {
        fn $func_name<'a>(
            builder: &mut flatbuffers::FlatBufferBuilder<'a>,
            updated: &fb::$updated_type<'a>,
        ) -> flatbuffers::WIPOffset<fb::$updated_type<'a>> {
            let update_version_offset = updated.update_version().map(|uv| {
                fb::UpdateVersion::create(
                    builder,
                    &fb::UpdateVersionArgs {
                        version_type: uv.version_type(),
                        version_value: uv.version_value(),
                    },
                )
            });

            let value_offset = updated.value().map(|value| $create_value_fn(builder, value));

            fb::$updated_type::create(
                builder,
                &fb::$updated_args {
                    slot_updated: updated.slot_updated(),
                    update_version: update_version_offset,
                    value: value_offset,
                },
            )
        }
    };
}

create_updated_primitive_offset!(create_updated_bool_offset, UpdatedBool, UpdatedBoolArgs);
create_updated_primitive_offset!(create_updated_u64_offset, UpdatedU64, UpdatedU64Args);
create_updated_primitive_offset!(create_updated_u32_offset, UpdatedU32, UpdatedU32Args);
create_updated_primitive_offset!(
    create_updated_chain_mutability_offset,
    UpdatedChainMutability,
    UpdatedChainMutabilityArgs
);
create_updated_primitive_offset!(
    create_updated_owner_type_offset,
    UpdatedOwnerType,
    UpdatedOwnerTypeArgs
);

create_updated_offset!(
    create_updated_string_offset,
    UpdatedString,
    UpdatedStringArgs,
    create_string_offset
);
create_updated_offset!(
    create_updated_pubkey_offset,
    UpdatedPubkey,
    UpdatedPubkeyArgs,
    create_vector_offset
);
create_updated_offset!(
    create_updated_optional_pubkey_offset,
    UpdatedOptionalPubkey,
    UpdatedOptionalPubkeyArgs,
    create_vector_offset
);

fn create_updated_creators_offset<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    updated: &fb::UpdatedCreators<'a>,
) -> flatbuffers::WIPOffset<fb::UpdatedCreators<'a>> {
    let update_version_offset = updated.update_version().map(|uv| {
        fb::UpdateVersion::create(
            builder,
            &fb::UpdateVersionArgs {
                version_type: uv.version_type(),
                version_value: uv.version_value(),
            },
        )
    });

    let value_offset = if let Some(creator_original) = updated.value() {
        let mut creators = Vec::with_capacity(creator_original.len());
        for creator in &creator_original {
            let pubkey_fb = creator.creator().map(|c| builder.create_vector(c.bytes()));

            let creator_fb = fb::Creator::create(
                builder,
                &fb::CreatorArgs {
                    creator: pubkey_fb,
                    creator_verified: creator.creator_verified(),
                    creator_share: creator.creator_share(),
                },
            );
            creators.push(creator_fb);
        }
        Some(builder.create_vector(creators.as_slice()))
    } else {
        None
    };

    fb::UpdatedCreators::create(
        builder,
        &fb::UpdatedCreatorsArgs {
            slot_updated: updated.slot_updated(),
            update_version: update_version_offset,
            value: value_offset,
        },
    )
}

pub fn merge_complete_details_fb_simplified(
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    merge_complete_details_fb_simple_raw(new_key, existing_val, operands.iter())
}

#[derive(Clone)]
struct FbOwnerContainer<'a> {
    pubkey: Option<flatbuffers::Vector<'a, u8>>,
    owner: Option<fb::UpdatedOptionalPubkey<'a>>,
    delegate: Option<fb::UpdatedOptionalPubkey<'a>>,
    owner_type: Option<fb::UpdatedOwnerType<'a>>,
    owner_delegate_seq: Option<fb::UpdatedU64<'a>>,
    is_current_owner: Option<fb::UpdatedBool<'a>>,
}

impl<'a> From<fb::AssetOwner<'a>> for FbOwnerContainer<'a> {
    fn from(value: fb::AssetOwner<'a>) -> Self {
        Self {
            pubkey: value.pubkey(),
            owner: value.owner(),
            delegate: value.delegate(),
            owner_type: value.owner_type(),
            owner_delegate_seq: value.owner_delegate_seq(),
            is_current_owner: value.is_current_owner(),
        }
    }
}

pub fn merge_complete_details_fb_simple_raw<'a>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: impl Iterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let existing_val = existing_val.and_then(|bytes| {
        fb::root_as_asset_complete_details(bytes)
            .map_err(|e| error!("RocksDB: AssetCompleteDetails deserialize existing_val: {}", e))
            .ok()
    });
    let mut pk = existing_val.and_then(|a| a.pubkey());
    let mut static_details = existing_val.and_then(|a| a.static_details());
    // creating a copy of every single field of the rest of the asset fields including the pubkeys to properly select the latest ones and reconstruct the asset
    let mut dynamic_details_pubkey =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.pubkey());
    let mut dynamic_details_is_compressible =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.is_compressible());
    let mut dynamic_details_is_compressed =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.is_compressed());
    let mut dynamic_details_is_frozen =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.is_frozen());
    let mut dynamic_details_supply =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.supply());
    let mut dynamic_details_seq =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.seq());
    let mut dynamic_details_is_burnt =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.is_burnt());
    let mut dynamic_details_was_decompressed =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.was_decompressed());
    let mut dynamic_details_onchain_data =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.onchain_data());
    let mut dynamic_details_creators =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.creators());
    let mut dynamic_details_royalty_amount =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.royalty_amount());
    let mut dynamic_details_url =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.url());
    let mut dynamic_details_chain_mutability =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.chain_mutability());
    let mut dynamic_details_lamports =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.lamports());
    let mut dynamic_details_executable =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.executable());
    let mut dynamic_details_metadata_owner =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.metadata_owner());
    let mut dynamic_details_raw_name =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.raw_name());
    let mut dynamic_details_mpl_core_plugins =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.mpl_core_plugins());
    let mut dynamic_details_mpl_core_unknown_plugins =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.mpl_core_unknown_plugins());
    let mut dynamic_details_rent_epoch =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.rent_epoch());
    let mut dynamic_details_num_minted =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.num_minted());
    let mut dynamic_details_current_size =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.current_size());
    let mut dynamic_details_plugins_json_version =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.plugins_json_version());
    let mut dynamic_details_mpl_core_external_plugins =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.mpl_core_external_plugins());
    let mut dynamic_details_mpl_core_unknown_external_plugins = existing_val
        .and_then(|a| a.dynamic_details())
        .and_then(|d| d.mpl_core_unknown_external_plugins());
    let mut dynamic_details_mint_extensions =
        existing_val.and_then(|a| a.dynamic_details()).and_then(|d| d.mint_extensions());
    let mut authority = existing_val.and_then(|a| a.authority());
    let mut owner_pubkey = existing_val.and_then(|a| a.owner()).and_then(|d| d.pubkey());
    let mut owner_owner = existing_val.and_then(|a| a.owner()).and_then(|d| d.owner());
    let mut owner_delegate = existing_val.and_then(|a| a.owner()).and_then(|d| d.delegate());
    let mut owner_owner_type = existing_val.and_then(|a| a.owner()).and_then(|d| d.owner_type());
    let mut owner_owner_delegate_seq =
        existing_val.and_then(|a| a.owner()).and_then(|d| d.owner_delegate_seq());
    let mut owner_is_current_owner =
        existing_val.and_then(|a| a.owner()).and_then(|d| d.is_current_owner());
    let mut collection_pubkey = existing_val.and_then(|a| a.collection()).and_then(|d| d.pubkey());
    let mut collection_collection =
        existing_val.and_then(|a| a.collection()).and_then(|d| d.collection());
    let mut collection_is_collection_verified =
        existing_val.and_then(|a| a.collection()).and_then(|d| d.is_collection_verified());
    let mut collection_authority =
        existing_val.and_then(|a| a.collection()).and_then(|d| d.authority());
    let mut specification_asset_class =
        existing_val.and_then(|a| a.static_details()).map(|d| d.specification_asset_class());

    // With the owner the merge is a bit more complex because we need to check if the owner is a new owner or an existing one
    // The following cases are possible:
    // 1. The owner in operand is None, in this case we don't need to update the owner
    // 2. The owner in operand is Some, but the existing owner is None, in this case we set the new owner
    // 3. Both the owner in operand and the existing owner are Some, in this case we have several subcases:
    //    3.1. The owner in operand has the same pubkey as the existing owner, in this case we need to merge the owner fields and no updates to the other known owners is needed (TODO: alternative is to update the owner with the latest fields and merge the new/old owner with the other known owners based on the owner value)
    //    3.2. The owner in operand has a different pubkey than the existing owner, in this case we need to check if the new owner is newer than the existing owner and update the owner fields accordingly. The newer owner should be set as the owner and the other known owners should be merged with the older owner fields
    // The same owner pubkey in both the main owner field, or in the other known owners should be merged based on those: first we do compare the update_version and if the update_version is the same we compare the slot_updated
    // For different pubkeys we first merge the owners with matching record in the other known owners field. If there is no match we add the new owner to the other known owners field and compare the slot updated to determine the latest owner with the is_current_owner set to true. If the slot_updated is the same we declare the owner with the is_current_owner = true. If both have the same slot_updated and is_current_owner we keep the existing owner, putting the other on top of the other known owners.
    // If after appending to the other known owners the vector is bigger than 10, we remove the oldest elements from the vector to keep it at 10 elements, unless the oldest element has the same slot_updated as the current owner, in this case we keep everything as is.

    // Collect existing owner and other known owners
    // let mut owner = existing_val.and_then(|a| a.owner());
    let mut other_known_owners = existing_val
        .and_then(|a| a.other_known_owners())
        .map(|vec| {
            vec.iter()
                .map(|v| (v.pubkey().map(|k| k.bytes()).unwrap_or_default(), v.into()))
                .collect::<HashMap<_, FbOwnerContainer>>()
        })
        .unwrap_or_default();
    for op in operands {
        if let Ok(new_val) = fb::root_as_asset_complete_details(op) {
            if pk.is_none() {
                pk = new_val.pubkey();
            }
            // Keep existing static_details if present, but if the exising asset class was fungible and the new asset class is one of the NFT types, update the asset class as it's a known case for TokenMetadata
            match static_details {
                Some(existing_static_details) => {
                    if let Some(new_static_details) = new_val.static_details() {
                        let existing_class = existing_static_details.specification_asset_class();
                        let new_class = new_static_details.specification_asset_class();
                        if matches!(
                            existing_class,
                            fb::SpecificationAssetClass::FungibleToken
                                | fb::SpecificationAssetClass::FungibleAsset
                        ) && matches!(
                            new_class,
                            fb::SpecificationAssetClass::Nft
                                | fb::SpecificationAssetClass::ProgrammableNft
                        ) {
                            specification_asset_class = Some(new_class);
                        }

                        if existing_class == fb::SpecificationAssetClass::Unknown {
                            specification_asset_class = Some(new_class);
                        }
                    }
                },
                None => {
                    static_details = new_val.static_details();
                },
            }
            // Merge dynamic_details
            if let Some(new_dynamic_details) = new_val.dynamic_details() {
                if dynamic_details_pubkey.is_none() {
                    dynamic_details_pubkey = new_dynamic_details.pubkey()
                }
                merge_field(
                    &mut dynamic_details_is_compressible,
                    new_dynamic_details.is_compressible(),
                );
                merge_field(
                    &mut dynamic_details_is_compressed,
                    new_dynamic_details.is_compressed(),
                );
                merge_field(&mut dynamic_details_is_frozen, new_dynamic_details.is_frozen());
                merge_field(&mut dynamic_details_supply, new_dynamic_details.supply());
                merge_field(&mut dynamic_details_seq, new_dynamic_details.seq());
                merge_field(&mut dynamic_details_is_burnt, new_dynamic_details.is_burnt());
                merge_field(
                    &mut dynamic_details_was_decompressed,
                    new_dynamic_details.was_decompressed(),
                );
                merge_field(&mut dynamic_details_onchain_data, new_dynamic_details.onchain_data());
                merge_field(&mut dynamic_details_creators, new_dynamic_details.creators());
                merge_field(
                    &mut dynamic_details_royalty_amount,
                    new_dynamic_details.royalty_amount(),
                );
                merge_field(&mut dynamic_details_url, new_dynamic_details.url());
                merge_field(
                    &mut dynamic_details_chain_mutability,
                    new_dynamic_details.chain_mutability(),
                );
                merge_field(&mut dynamic_details_lamports, new_dynamic_details.lamports());
                merge_field(&mut dynamic_details_executable, new_dynamic_details.executable());
                merge_field(
                    &mut dynamic_details_metadata_owner,
                    new_dynamic_details.metadata_owner(),
                );
                merge_field(&mut dynamic_details_raw_name, new_dynamic_details.raw_name());
                merge_field(
                    &mut dynamic_details_mpl_core_plugins,
                    new_dynamic_details.mpl_core_plugins(),
                );
                merge_field(
                    &mut dynamic_details_mpl_core_unknown_plugins,
                    new_dynamic_details.mpl_core_unknown_plugins(),
                );
                merge_field(&mut dynamic_details_rent_epoch, new_dynamic_details.rent_epoch());
                merge_field(&mut dynamic_details_num_minted, new_dynamic_details.num_minted());
                merge_field(&mut dynamic_details_current_size, new_dynamic_details.current_size());
                merge_field(
                    &mut dynamic_details_plugins_json_version,
                    new_dynamic_details.plugins_json_version(),
                );
                merge_field(
                    &mut dynamic_details_mpl_core_external_plugins,
                    new_dynamic_details.mpl_core_external_plugins(),
                );
                merge_field(
                    &mut dynamic_details_mpl_core_unknown_external_plugins,
                    new_dynamic_details.mpl_core_unknown_external_plugins(),
                );
                merge_field(
                    &mut dynamic_details_mint_extensions,
                    new_dynamic_details.mint_extensions(),
                );
            }
            // Merge authority
            if let Some(new_authority) = new_val.authority() {
                if authority.is_none_or(|current_authority| {
                    new_authority.compare(&current_authority) == Ordering::Greater
                }) {
                    authority = Some(new_authority);
                }
            }
            // Merge owner
            if let Some(new_owner) = new_val.owner() {
                if let Some(new_owner_pubkey) = new_owner.pubkey() {
                    // handle the case when the existing owner is missing
                    if owner_pubkey.is_none() {
                        owner_pubkey = new_owner.pubkey();
                        owner_owner = new_owner.owner();
                        owner_delegate = new_owner.delegate();
                        owner_owner_type = new_owner.owner_type();
                        owner_owner_delegate_seq = new_owner.owner_delegate_seq();
                        owner_is_current_owner = new_owner.is_current_owner();
                    } else {
                        // if the owner pubkey is the same(meaning same token account addresses) we merge the owner fields
                        if owner_pubkey.map(|k| k.bytes()) == new_owner.pubkey().map(|k| k.bytes())
                        {
                            merge_field(&mut owner_owner, new_owner.owner());
                            merge_field(&mut owner_delegate, new_owner.delegate());
                            merge_field(
                                &mut owner_owner_delegate_seq,
                                new_owner.owner_delegate_seq(),
                            );
                            merge_field(&mut owner_is_current_owner, new_owner.is_current_owner());
                            // after merging the owner fields we may end up with an account that has the is_current_owner set to false, in this case we need to check for an account with is_current_owner set to true inside the other known owners and set it as the owner. We select the account with the highest slot_updated. The previous owner should be moved to the other known owners
                            if !owner_is_current_owner.map(|u| u.value()).unwrap_or(false) {
                                let best_current_owner_option = {
                                    other_known_owners
                                        .iter()
                                        .filter_map(|(k, v)| {
                                            v.is_current_owner.as_ref().filter(|u| u.value()).map(
                                                |is_owner| (is_owner.slot_updated(), *k, v.clone()),
                                            )
                                        })
                                        .max_by_key(|(slot, _, _)| *slot)
                                };
                                if let Some((_, new_owner_key, new_current_owner)) =
                                    best_current_owner_option
                                {
                                    let previous_owner = FbOwnerContainer {
                                        pubkey: owner_pubkey,
                                        owner: owner_owner,
                                        delegate: owner_delegate,
                                        owner_type: owner_owner_type,
                                        owner_delegate_seq: owner_owner_delegate_seq,
                                        is_current_owner: owner_is_current_owner,
                                    };
                                    other_known_owners
                                        .insert(owner_pubkey.unwrap().bytes(), previous_owner);
                                    owner_pubkey = new_current_owner.pubkey;
                                    owner_owner = new_current_owner.owner;
                                    owner_delegate = new_current_owner.delegate;
                                    owner_owner_delegate_seq = new_current_owner.owner_delegate_seq;
                                    owner_is_current_owner = new_current_owner.is_current_owner;
                                    other_known_owners.remove(new_owner_key);
                                }
                            }
                        } else {
                            // if the owner pubkey is different it might already be in the other known owners, first we merge it with the one from the other known owners
                            // then we check which one is newer and put the other one in the other known owners

                            let mut merged_owner = FbOwnerContainer::from(new_owner);
                            if let Some(oldish_owner) =
                                other_known_owners.get(new_owner_pubkey.bytes())
                            {
                                let mut oldish_owner = oldish_owner.clone();
                                merge_field(&mut oldish_owner.owner, new_owner.owner());
                                merge_field(&mut oldish_owner.delegate, new_owner.delegate());
                                merge_field(
                                    &mut oldish_owner.owner_delegate_seq,
                                    new_owner.owner_delegate_seq(),
                                );
                                merge_field(
                                    &mut oldish_owner.is_current_owner,
                                    new_owner.is_current_owner(),
                                );

                                // owner type should not be taken from any other owners because it's even merged separately.
                                // and with such an approach we will have global(same) owner_type for all the records.
                                //
                                // otherwise it may(and will) affect rows in other_known_owners field by saving there owners with different owner types.
                                // meaning some of the rows will have correct owner type and some of them will not have it at all(Unknown).
                                oldish_owner.owner_type = owner_owner_type;

                                merged_owner = oldish_owner;
                            }
                            other_known_owners
                                .insert(new_owner_pubkey.bytes(), merged_owner.clone());

                            // now the merged owner holds the merged data. We need to check if it's marked as the current owner and if it's newer than the current owner
                            // if it doesn't have the is current owner set to true we don't need to do anything
                            if merged_owner.is_current_owner.map(|u| u.value()).unwrap_or(false)
                                && (!owner_is_current_owner.map(|u| u.value()).unwrap_or(false)
                                    || merged_owner
                                        .is_current_owner
                                        .map(|u| u.slot_updated())
                                        .unwrap_or_default()
                                        > owner_is_current_owner
                                            .map(|u| u.slot_updated())
                                            .unwrap_or_default())
                            {
                                // if the merged owner is newer we set it as the owner and move the old owner into the other known owners
                                let previous_owner = FbOwnerContainer {
                                    pubkey: owner_pubkey,
                                    owner: owner_owner,
                                    delegate: owner_delegate,
                                    owner_type: owner_owner_type,
                                    owner_delegate_seq: owner_owner_delegate_seq,
                                    is_current_owner: owner_is_current_owner,
                                };
                                other_known_owners
                                    .insert(owner_pubkey.unwrap().bytes(), previous_owner);
                                owner_pubkey = merged_owner.pubkey;
                                owner_owner = merged_owner.owner;
                                owner_delegate = merged_owner.delegate;
                                owner_owner_delegate_seq = merged_owner.owner_delegate_seq;
                                owner_is_current_owner = merged_owner.is_current_owner;
                                other_known_owners.remove(new_owner_pubkey.bytes());
                            }
                        }
                        // Since the owner type can only be changed through a mint account state update,
                        // we should avoid modifying it every time we receive token account updates for the corresponding mint.
                        // Even in cases where multiple mint account state updates occur within a single slot, the merge function
                        // ensures that the latest update is preserved, as the WriteVersion is saved during the process.

                        // Additionally, during the processing of token account updates, the owner type is set to its default value.
                        merge_field(&mut owner_owner_type, new_owner.owner_type());
                    }
                }
            }

            // Merge collection
            if let Some(new_collection) = new_val.collection() {
                if collection_pubkey.is_none() {
                    collection_pubkey = new_collection.pubkey();
                }
                merge_field(&mut collection_collection, new_collection.collection());
                merge_field(
                    &mut collection_is_collection_verified,
                    new_collection.is_collection_verified(),
                );
                merge_field(&mut collection_authority, new_collection.authority());
            }
        }
    }
    pk?;
    let mut builder = FlatBufferBuilder::with_capacity(2500);

    let pk: Option<WIPOffset<flatbuffers::Vector<'_, u8>>> =
        pk.map(|k| builder.create_vector(k.bytes()));
    let static_details = static_details.map(|s| {
        let args = fb::AssetStaticDetailsArgs {
            pubkey: s.pubkey().map(|k| builder.create_vector(k.bytes())),
            specification_asset_class: specification_asset_class
                .unwrap_or(s.specification_asset_class()),
            royalty_target_type: s.royalty_target_type(),
            created_at: s.created_at(),
            edition_address: s.edition_address().map(|k| builder.create_vector(k.bytes())),
        };
        fb::AssetStaticDetails::create(&mut builder, &args)
    });

    let dynamic_details = dynamic_details_pubkey.map(|d| {
        let args = fb::AssetDynamicDetailsArgs {
            pubkey: Some(builder.create_vector(d.bytes())),
            is_compressible: dynamic_details_is_compressible
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            is_compressed: dynamic_details_is_compressed
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            is_frozen: dynamic_details_is_frozen
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            supply: dynamic_details_supply.map(|u| create_updated_u64_offset(&mut builder, &u)),
            seq: dynamic_details_seq.map(|u| create_updated_u64_offset(&mut builder, &u)),
            is_burnt: dynamic_details_is_burnt
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            was_decompressed: dynamic_details_was_decompressed
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            onchain_data: dynamic_details_onchain_data
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            creators: dynamic_details_creators
                .map(|u| create_updated_creators_offset(&mut builder, &u)),
            royalty_amount: dynamic_details_royalty_amount
                .map(|u| create_updated_u32_offset(&mut builder, &u)),
            url: dynamic_details_url.map(|u| create_updated_string_offset(&mut builder, &u)),
            chain_mutability: dynamic_details_chain_mutability
                .map(|u| create_updated_chain_mutability_offset(&mut builder, &u)),
            lamports: dynamic_details_lamports.map(|u| create_updated_u64_offset(&mut builder, &u)),
            executable: dynamic_details_executable
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            metadata_owner: dynamic_details_metadata_owner
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            raw_name: dynamic_details_raw_name
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            mpl_core_plugins: dynamic_details_mpl_core_plugins
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            mpl_core_unknown_plugins: dynamic_details_mpl_core_unknown_plugins
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            rent_epoch: dynamic_details_rent_epoch
                .map(|u| create_updated_u64_offset(&mut builder, &u)),
            num_minted: dynamic_details_num_minted
                .map(|u| create_updated_u32_offset(&mut builder, &u)),
            current_size: dynamic_details_current_size
                .map(|u| create_updated_u32_offset(&mut builder, &u)),
            plugins_json_version: dynamic_details_plugins_json_version
                .map(|u| create_updated_u32_offset(&mut builder, &u)),
            mpl_core_external_plugins: dynamic_details_mpl_core_external_plugins
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            mpl_core_unknown_external_plugins: dynamic_details_mpl_core_unknown_external_plugins
                .map(|u| create_updated_string_offset(&mut builder, &u)),
            mint_extensions: dynamic_details_mint_extensions
                .map(|u| create_updated_string_offset(&mut builder, &u)),
        };
        fb::AssetDynamicDetails::create(&mut builder, &args)
    });
    let authority = authority.map(|a| {
        let write_version =
            unsafe { a._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None) };
        let auth = a.authority().map(|x| builder.create_vector(x.bytes()));
        let pk = a.pubkey().map(|x| builder.create_vector(x.bytes()));
        let mut auth_builder = fb::AssetAuthorityBuilder::new(&mut builder);
        if let Some(wv) = write_version {
            auth_builder.add_write_version(wv);
        }
        auth_builder.add_slot_updated(a.slot_updated());
        if let Some(x) = auth {
            auth_builder.add_authority(x);
        }
        if let Some(x) = pk {
            auth_builder.add_pubkey(x);
        }
        auth_builder.finish()
    });

    let owner = owner_pubkey.map(|k| {
        let args = fb::AssetOwnerArgs {
            pubkey: Some(builder.create_vector(k.bytes())),
            owner: owner_owner.map(|u| create_updated_optional_pubkey_offset(&mut builder, &u)),
            delegate: owner_delegate
                .map(|u| create_updated_optional_pubkey_offset(&mut builder, &u)),
            owner_type: owner_owner_type
                .map(|u| create_updated_owner_type_offset(&mut builder, &u)),
            owner_delegate_seq: owner_owner_delegate_seq
                .map(|u| create_updated_u64_offset(&mut builder, &u)),
            is_current_owner: owner_is_current_owner
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
        };
        fb::AssetOwner::create(&mut builder, &args)
    });
    let collection = collection_pubkey.map(|c| {
        let args = fb::AssetCollectionArgs {
            pubkey: Some(builder.create_vector(c.bytes())),
            collection: collection_collection
                .map(|u| create_updated_pubkey_offset(&mut builder, &u)),
            is_collection_verified: collection_is_collection_verified
                .map(|u| create_updated_bool_offset(&mut builder, &u)),
            authority: collection_authority
                .map(|u| create_updated_optional_pubkey_offset(&mut builder, &u)),
        };
        fb::AssetCollection::create(&mut builder, &args)
    });
    //

    // Create other_known_owners offset
    let other_known_owners_offset = if !other_known_owners.is_empty() {
        // Get the updated_slot of the main owner
        let owner_owner_updated_slot = owner_owner.as_ref().map(|u| u.slot_updated()).unwrap_or(0);

        // Collect and sort the other known owners
        let mut owners_vec: Vec<_> = other_known_owners.values().collect();

        // Sort the owners by `owner.slot_updated()` descending
        owners_vec.sort_by(|a, b| {
            let a_slot = a.owner.as_ref().map(|u| u.slot_updated()).unwrap_or(0);
            let b_slot = b.owner.as_ref().map(|u| u.slot_updated()).unwrap_or(0);

            // Compare slots in descending order
            b_slot.cmp(&a_slot).then_with(|| {
                // If slots are equal, compare pubkeys in ascending order
                let a_pubkey = a.pubkey.as_ref().map(|k| k.bytes());
                let b_pubkey = b.pubkey.as_ref().map(|k| k.bytes());

                a_pubkey.cmp(&b_pubkey)
            })
        });

        // Initialize a vector to hold the selected owners
        let mut offsets = Vec::new();

        let top_slot_to_keep = max(
            owner_owner_updated_slot,
            owners_vec.first().and_then(|o| o.owner).map(|o| o.slot_updated()).unwrap_or(0),
        );
        // Iterate through the sorted owners
        for owner in owners_vec {
            let owner_slot = owner.owner.as_ref().map(|u| u.slot_updated()).unwrap_or(0);
            if (owner_slot < top_slot_to_keep) && (offsets.len() >= MAX_OTHER_OWNERS) {
                // Reached MAX_OTHER_OWNERS limit
                break;
            }
            offsets.push(create_owner_offset(&mut builder, owner));
        }
        Some(builder.create_vector(&offsets))
    } else {
        None
    };

    let res = fb::AssetCompleteDetails::create(
        &mut builder,
        &fb::AssetCompleteDetailsArgs {
            pubkey: pk,
            static_details,
            dynamic_details,
            authority,
            owner,
            collection,
            other_known_owners: other_known_owners_offset,
        },
    );
    builder.finish_minimal(res);
    Some(builder.finished_data().to_vec())
}

fn create_owner_offset<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    owner: &FbOwnerContainer<'a>,
) -> WIPOffset<fb::AssetOwner<'a>> {
    let pubkey_offset = owner.pubkey.map(|k| builder.create_vector(k.bytes()));

    let owner_field = owner.owner.map(|o| create_updated_optional_pubkey_offset(builder, &o));

    let delegate_field = owner.delegate.map(|d| create_updated_optional_pubkey_offset(builder, &d));

    let owner_type_field =
        owner.owner_type.map(|ot| create_updated_owner_type_offset(builder, &ot));

    let owner_delegate_seq_field =
        owner.owner_delegate_seq.map(|seq| create_updated_u64_offset(builder, &seq));

    let is_current_owner_field =
        owner.is_current_owner.map(|ico| create_updated_bool_offset(builder, &ico));

    fb::AssetOwner::create(
        builder,
        &fb::AssetOwnerArgs {
            pubkey: pubkey_offset,
            owner: owner_field,
            delegate: delegate_field,
            owner_type: owner_type_field,
            owner_delegate_seq: owner_delegate_seq_field,
            is_current_owner: is_current_owner_field,
        },
    )
}

fn merge_field<'a, T>(existing_field: &mut Option<T>, new_field: Option<T>)
where
    T: PartialOrd + 'a,
{
    if let Some(new_val) = new_field {
        match existing_field {
            None => {
                *existing_field = Some(new_val);
            },
            Some(existing_value) => {
                if new_val.partial_cmp(existing_value) == Some(std::cmp::Ordering::Greater) {
                    *existing_field = Some(new_val);
                }
            },
        }
    }
}

pub fn merge_complete_details_fb_raw<'a>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: impl Iterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let mut builder = FlatBufferBuilder::with_capacity(2500);
    // Deserialize existing value into an iterator
    let existing_iter = existing_val
        .and_then(|bytes| {
            fb::root_as_asset_complete_details(bytes)
                .map_err(|e| {
                    error!("RocksDB: AssetCompleteDetails deserialize existing_val: {}", e)
                })
                .ok()
        })
        .into_iter();

    // Deserialize operands into an iterator
    let operands_iter = operands.filter_map(|bytes| fb::root_as_asset_complete_details(bytes).ok());

    // Combine existing and operands into a single iterator
    let all_assets: Vec<_> = existing_iter.chain(operands_iter).collect();

    let pubkey = all_assets
        .iter()
        .find_map(|asset| asset.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pubkey?;

    let static_details = merge_static_details(
        &mut builder,
        all_assets.iter().filter_map(|a| a.static_details()).collect(),
    );
    let dynamic_details = merge_dynamic_details(
        &mut builder,
        all_assets.iter().filter_map(|a| a.dynamic_details()).collect(),
    );
    let authority =
        merge_authority(&mut builder, all_assets.iter().filter_map(|a| a.authority()).collect());
    let owner = merge_owner(&mut builder, all_assets.iter().filter_map(|a| a.owner()).collect());
    let collection =
        merge_collection(&mut builder, all_assets.iter().filter_map(|a| a.collection()).collect());
    let res = fb::AssetCompleteDetails::create(
        &mut builder,
        &fb::AssetCompleteDetailsArgs {
            pubkey,
            static_details,
            dynamic_details,
            authority,
            owner,
            collection,
            other_known_owners: None, // todo: if this ever used, we need to implement it
        },
    );
    builder.finish_minimal(res);
    Some(builder.finished_data().to_vec())
}

fn merge_static_details<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    iter: Vec<fb::AssetStaticDetails<'a>>,
) -> Option<WIPOffset<fb::AssetStaticDetails<'a>>> {
    let pk = iter
        .iter()
        .cloned()
        .find_map(|asset| asset.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pk?;
    let args = fb::AssetStaticDetailsArgs {
        pubkey: pk,
        specification_asset_class: iter
            .iter()
            .cloned()
            .map(|asset| asset.specification_asset_class())
            .next()
            .unwrap_or_default(),
        royalty_target_type: iter
            .iter()
            .cloned()
            .map(|asset| asset.royalty_target_type())
            .next()
            .unwrap_or_default(),
        created_at: iter.iter().cloned().map(|asset| asset.created_at()).next().unwrap_or_default(),
        edition_address: iter
            .iter()
            .cloned()
            .find_map(|asset| asset.edition_address())
            .map(|k| builder.create_vector(k.bytes())),
    };
    Some(fb::AssetStaticDetails::create(builder, &args))
}

macro_rules! merge_updated_primitive {
    ($func_name:ident, $updated_type:ident, $updated_args:ident) => {
        fn $func_name<'a, T, F>(
            builder: &mut flatbuffers::FlatBufferBuilder<'a>,
            iter: impl DoubleEndedIterator<Item = T>,
            extract_fn: F,
        ) -> Option<flatbuffers::WIPOffset<fb::$updated_type<'a>>>
        where
            F: Fn(T) -> Option<fb::$updated_type<'a>>,
            T: 'a,
        {
            iter.filter_map(extract_fn)
                .rev() // Reverse the iterator for max_by to get the first-most element for the case of multiple equal values
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(|v| {
                    let version_offset = v.update_version().map(|uv| {
                        fb::UpdateVersion::create(
                            builder,
                            &fb::UpdateVersionArgs {
                                version_type: uv.version_type(),
                                version_value: uv.version_value(),
                            },
                        )
                    });
                    fb::$updated_type::create(
                        builder,
                        &fb::$updated_args {
                            slot_updated: v.slot_updated(),
                            update_version: version_offset,
                            value: v.value(),
                        },
                    )
                })
        }
    };
}

macro_rules! merge_updated_offset {
    ($func_name:ident, $updated_type:ident, $updated_args:ident, $value_create_fn:path) => {
        fn $func_name<'a, T, F>(
            builder: &mut flatbuffers::FlatBufferBuilder<'a>,
            iter: impl DoubleEndedIterator<Item = T>,
            extract_fn: F,
        ) -> Option<flatbuffers::WIPOffset<fb::$updated_type<'a>>>
        where
            F: Fn(T) -> Option<fb::$updated_type<'a>>,
            T: 'a,
        {
            iter.filter_map(extract_fn)
                .rev() // Reverse the iterator for max_by to get the first-most element for the case of multiple equal values
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(|v| {
                    let version_offset = v.update_version().map(|uv| {
                        fb::UpdateVersion::create(
                            builder,
                            &fb::UpdateVersionArgs {
                                version_type: uv.version_type(),
                                version_value: uv.version_value(),
                            },
                        )
                    });
                    let value_offset = v.value().map(|value| $value_create_fn(builder, value));
                    fb::$updated_type::create(
                        builder,
                        &fb::$updated_args {
                            slot_updated: v.slot_updated(),
                            update_version: version_offset,
                            value: value_offset,
                        },
                    )
                })
        }
    };
}

merge_updated_primitive!(merge_updated_bool, UpdatedBool, UpdatedBoolArgs);
merge_updated_primitive!(merge_updated_u64, UpdatedU64, UpdatedU64Args);
merge_updated_primitive!(merge_updated_u32, UpdatedU32, UpdatedU32Args);
merge_updated_primitive!(
    merge_updated_chain_mutability,
    UpdatedChainMutability,
    UpdatedChainMutabilityArgs
);
merge_updated_primitive!(merge_updated_owner_type, UpdatedOwnerType, UpdatedOwnerTypeArgs);
merge_updated_offset!(merge_updated_string, UpdatedString, UpdatedStringArgs, create_string_offset);
fn create_string_offset<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    value: &str,
) -> flatbuffers::WIPOffset<&'a str> {
    builder.create_string(value)
}

fn create_vector_offset<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    value: flatbuffers::Vector<'a, u8>,
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>> {
    builder.create_vector(value.bytes())
}
merge_updated_offset!(merge_updated_pubkey, UpdatedPubkey, UpdatedPubkeyArgs, create_vector_offset);
merge_updated_offset!(
    merge_updated_optional_pubkey,
    UpdatedOptionalPubkey,
    UpdatedOptionalPubkeyArgs,
    create_vector_offset
);

fn merge_updated_creators<'a, T, F>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    iter: impl DoubleEndedIterator<Item = T>,
    extract_fn: F,
) -> Option<flatbuffers::WIPOffset<fb::UpdatedCreators<'a>>>
where
    F: Fn(T) -> Option<fb::UpdatedCreators<'a>>,
    T: 'a,
{
    iter.filter_map(extract_fn)
        .rev() // Reverse the iterator for max_by to get the first-most element for the case of multiple equal values
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|v| {
            // Create UpdateVersion
            let version_offset = v.update_version().map(|uv| {
                fb::UpdateVersion::create(
                    builder,
                    &fb::UpdateVersionArgs {
                        version_type: uv.version_type(),
                        version_value: uv.version_value(),
                    },
                )
            });
            let creators_fb = if let Some(creator_original) = v.value() {
                let mut creators = Vec::with_capacity(creator_original.len());
                for creator in &creator_original {
                    let pubkey_fb = creator.creator().map(|c| builder.create_vector(c.bytes()));

                    let creator_fb = fb::Creator::create(
                        builder,
                        &fb::CreatorArgs {
                            creator: pubkey_fb,
                            creator_verified: creator.creator_verified(),
                            creator_share: creator.creator_share(),
                        },
                    );
                    creators.push(creator_fb);
                }
                Some(builder.create_vector(creators.as_slice()))
            } else {
                None
            };

            // Create UpdatedCreators
            fb::UpdatedCreators::create(
                builder,
                &fb::UpdatedCreatorsArgs {
                    slot_updated: v.slot_updated(),
                    update_version: version_offset,
                    value: creators_fb,
                },
            )
        })
}

fn merge_dynamic_details<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    iter: Vec<fb::AssetDynamicDetails<'a>>,
) -> Option<WIPOffset<fb::AssetDynamicDetails<'a>>> {
    let pk = iter
        .iter()
        .cloned()
        .find_map(|asset| asset.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pk?;
    let is_compressible =
        merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.is_compressible());
    let is_compressed =
        merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.is_compressed());
    let is_frozen = merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.is_frozen());
    let supply = merge_updated_u64(builder, iter.iter().cloned(), |asset| asset.supply());
    let seq = merge_updated_u64(builder, iter.iter().cloned(), |asset| asset.seq());
    let is_burnt = merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.is_burnt());
    let was_decompressed =
        merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.was_decompressed());
    let onchain_data =
        merge_updated_string(builder, iter.iter().cloned(), |asset| asset.onchain_data());
    let creators = merge_updated_creators(builder, iter.iter().cloned(), |a| a.creators());
    let royalty_amount =
        merge_updated_u32(builder, iter.iter().cloned(), |asset| asset.royalty_amount());
    let url = merge_updated_string(builder, iter.iter().cloned(), |asset| asset.url());
    let chain_mutability = merge_updated_chain_mutability(builder, iter.iter().cloned(), |asset| {
        asset.chain_mutability()
    });
    let lamports = merge_updated_u64(builder, iter.iter().cloned(), |asset| asset.lamports());
    let executable = merge_updated_bool(builder, iter.iter().cloned(), |asset| asset.executable());
    let metadata_owner =
        merge_updated_string(builder, iter.iter().cloned(), |asset| asset.metadata_owner());
    let raw_name = merge_updated_string(builder, iter.iter().cloned(), |asset| asset.raw_name());
    let mpl_core_plugins =
        merge_updated_string(builder, iter.iter().cloned(), |asset| asset.mpl_core_plugins());
    let mpl_core_unknown_plugins = merge_updated_string(builder, iter.iter().cloned(), |asset| {
        asset.mpl_core_unknown_plugins()
    });
    let rent_epoch = merge_updated_u64(builder, iter.iter().cloned(), |asset| asset.rent_epoch());
    let num_minted = merge_updated_u32(builder, iter.iter().cloned(), |asset| asset.num_minted());
    let current_size =
        merge_updated_u32(builder, iter.iter().cloned(), |asset| asset.current_size());
    let plugins_json_version =
        merge_updated_u32(builder, iter.iter().cloned(), |asset| asset.plugins_json_version());
    let mpl_core_external_plugins = merge_updated_string(builder, iter.iter().cloned(), |asset| {
        asset.mpl_core_external_plugins()
    });
    let mpl_core_unknown_external_plugins =
        merge_updated_string(builder, iter.iter().cloned(), |asset| {
            asset.mpl_core_unknown_external_plugins()
        });
    let mint_extensions =
        merge_updated_string(builder, iter.iter().cloned(), |asset| asset.mint_extensions());

    Some(fb::AssetDynamicDetails::create(
        builder,
        &fb::AssetDynamicDetailsArgs {
            pubkey: pk,
            is_compressible,
            is_compressed,
            is_frozen,
            supply,
            seq,
            is_burnt,
            was_decompressed,
            onchain_data,
            creators,
            royalty_amount,
            url,
            chain_mutability,
            lamports,
            executable,
            metadata_owner,
            raw_name,
            mpl_core_plugins,
            mpl_core_unknown_plugins,
            rent_epoch,
            num_minted,
            current_size,
            plugins_json_version,
            mpl_core_external_plugins,
            mpl_core_unknown_external_plugins,
            mint_extensions,
        },
    ))
}

fn merge_authority<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    iter: Vec<fb::AssetAuthority<'a>>,
) -> Option<WIPOffset<fb::AssetAuthority<'a>>> {
    let pk = iter
        .iter()
        .cloned()
        .find_map(|asset| asset.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pk?;
    iter.iter()
        .cloned()
        .rev() // Reverse the iterator for max_by to get the first-most element for the case of multiple equal values
        .max_by(|a, b| {
            if let (Some(a_write_version), Some(b_write_version)) = unsafe {
                (
                    a._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None),
                    b._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None),
                )
            } {
                a_write_version.cmp(&b_write_version)
            } else {
                a.slot_updated().cmp(&b.slot_updated())
            }
        })
        .map(|authority_original| {
            let write_version = unsafe {
                authority_original._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None)
            };
            let auth = authority_original.authority().map(|x| builder.create_vector(x.bytes()));
            let mut auth_builder = fb::AssetAuthorityBuilder::new(builder);
            if let Some(wv) = write_version {
                auth_builder.add_write_version(wv);
            }
            auth_builder.add_slot_updated(authority_original.slot_updated());
            if let Some(x) = auth {
                auth_builder.add_authority(x);
            }
            if let Some(x) = pk {
                auth_builder.add_pubkey(x);
            }
            auth_builder.finish()
        })
}

fn merge_owner<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    iter: Vec<fb::AssetOwner<'a>>,
) -> Option<WIPOffset<fb::AssetOwner<'a>>> {
    let pk = iter
        .iter()
        .cloned()
        .find_map(|owner| owner.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pk?;
    let owner = merge_updated_optional_pubkey(builder, iter.iter().cloned(), |owner| owner.owner());
    let delegate =
        merge_updated_optional_pubkey(builder, iter.iter().cloned(), |owner| owner.delegate());
    let owner_type =
        merge_updated_owner_type(builder, iter.iter().cloned(), |owner| owner.owner_type());
    let owner_delegate_seq =
        merge_updated_u64(builder, iter.iter().cloned(), |owner| owner.owner_delegate_seq());
    let is_current_owner =
        merge_updated_bool(builder, iter.iter().cloned(), |owner| owner.is_current_owner());

    Some(fb::AssetOwner::create(
        builder,
        &fb::AssetOwnerArgs {
            pubkey: pk,
            owner,
            delegate,
            owner_type,
            owner_delegate_seq,
            is_current_owner,
        },
    ))
}
fn merge_collection<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    iter: Vec<fb::AssetCollection<'a>>,
) -> Option<WIPOffset<fb::AssetCollection<'a>>> {
    let pk = iter
        .iter()
        .cloned()
        .find_map(|collection| collection.pubkey())
        .map(|k| builder.create_vector(k.bytes()));
    pk?;
    let collection =
        merge_updated_pubkey(builder, iter.iter().cloned(), |collection| collection.collection());
    let is_collection_verified = merge_updated_bool(builder, iter.iter().cloned(), |collection| {
        collection.is_collection_verified()
    });
    let authority = merge_updated_optional_pubkey(builder, iter.iter().cloned(), |collection| {
        collection.authority()
    });

    Some(fb::AssetCollection::create(
        builder,
        &fb::AssetCollectionArgs { pubkey: pk, collection, is_collection_verified, authority },
    ))
}

impl AssetDynamicDetails {
    pub fn merge(&mut self, new_val: &Self) {
        update_field(&mut self.is_compressible, &new_val.is_compressible);
        update_field(&mut self.is_compressed, &new_val.is_compressed);
        update_field(&mut self.is_frozen, &new_val.is_frozen);
        update_optional_field(&mut self.supply, &new_val.supply);
        update_optional_field(&mut self.seq, &new_val.seq);
        update_field(&mut self.is_burnt, &new_val.is_burnt);
        update_field(&mut self.creators, &new_val.creators);
        update_field(&mut self.royalty_amount, &new_val.royalty_amount);
        update_optional_field(&mut self.was_decompressed, &new_val.was_decompressed);
        update_optional_field(&mut self.onchain_data, &new_val.onchain_data);
        update_field(&mut self.url, &new_val.url);
        update_optional_field(&mut self.chain_mutability, &new_val.chain_mutability);
        update_optional_field(&mut self.lamports, &new_val.lamports);
        update_optional_field(&mut self.executable, &new_val.executable);
        update_optional_field(&mut self.metadata_owner, &new_val.metadata_owner);
        update_optional_field(&mut self.raw_name, &new_val.raw_name);
        update_optional_field(&mut self.mpl_core_plugins, &new_val.mpl_core_plugins);
        update_optional_field(
            &mut self.mpl_core_unknown_plugins,
            &new_val.mpl_core_unknown_plugins,
        );
        update_optional_field(&mut self.num_minted, &new_val.num_minted);
        update_optional_field(&mut self.current_size, &new_val.current_size);
        update_optional_field(&mut self.rent_epoch, &new_val.rent_epoch);
        update_optional_field(&mut self.plugins_json_version, &new_val.plugins_json_version);
        update_optional_field(
            &mut self.mpl_core_external_plugins,
            &new_val.mpl_core_external_plugins,
        );
        update_optional_field(
            &mut self.mpl_core_unknown_external_plugins,
            &new_val.mpl_core_unknown_external_plugins,
        );
        update_optional_field(&mut self.mint_extensions, &new_val.mint_extensions);
    }

    pub fn merge_dynamic_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;

        // Deserialize existing value if present
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => result = Some(value),
                Err(e) => error!("RocksDB: AssetDynamicDetails deserialize existing_val: {}", e),
            }
        }

        // Iterate over operands and merge
        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    if let Some(ref mut current_val) = result {
                        current_val.merge(&new_val);
                    } else {
                        result = Some(new_val);
                    }
                },
                Err(e) => error!("RocksDB: AssetDynamicDetails deserialize new_val: {}", e),
            }
        }

        // Serialize the result back into bytes
        result.and_then(|result| serialize(&result).ok())
    }

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.is_compressible.slot_updated,
            self.is_compressed.slot_updated,
            self.is_frozen.slot_updated,
            self.supply.as_ref().map_or(0, |supply| supply.slot_updated),
            self.seq.as_ref().map_or(0, |seq| seq.slot_updated),
            self.is_burnt.slot_updated,
            self.was_decompressed
                .as_ref()
                .map_or(0, |was_decompressed| was_decompressed.slot_updated),
            self.onchain_data.as_ref().map_or(0, |onchain_data| onchain_data.slot_updated),
            self.creators.slot_updated,
            self.royalty_amount.slot_updated,
            self.chain_mutability.as_ref().map_or(0, |onchain_data| onchain_data.slot_updated),
            self.lamports.as_ref().map_or(0, |onchain_data| onchain_data.slot_updated),
            self.executable.as_ref().map_or(0, |onchain_data| onchain_data.slot_updated),
            self.metadata_owner.as_ref().map_or(0, |onchain_data| onchain_data.slot_updated),
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
    }
}

impl AssetAuthority {
    pub fn merge(&mut self, new_val: &Self) {
        if let (Some(self_write_version), Some(new_write_version)) =
            (self.write_version, new_val.write_version)
        {
            if new_write_version > self_write_version {
                *self = new_val.to_owned();
            }
        } else if new_val.slot_updated > self.slot_updated {
            *self = new_val.to_owned();
        }
        // If neither condition is met, retain existing `self`
    }

    pub fn merge_asset_authorities(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        let mut write_version = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetAuthority>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    write_version = value.write_version;
                    result = existing_val.to_vec();
                },
                Err(e) => {
                    error!("RocksDB: AssetAuthority deserialize existing_val: {}", e)
                },
            }
        }

        for op in operands {
            match deserialize::<AssetAuthority>(op) {
                Ok(new_val) => {
                    if write_version.is_some() && new_val.write_version.is_some() {
                        if new_val.write_version.unwrap() > write_version.unwrap() {
                            slot = new_val.slot_updated;
                            write_version = new_val.write_version;
                            result = op.to_vec();
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        write_version = new_val.write_version;
                        result = op.to_vec();
                    }
                },
                Err(e) => {
                    error!("RocksDB: AssetAuthority deserialize new_val: {}", e)
                },
            }
        }

        Some(result)
    }
}

impl TypedColumn for AssetOwnerDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_OWNER";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetOwner {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_OWNER_v2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}
impl AssetOwner {
    pub fn merge(&mut self, new_val: &Self) {
        update_field(&mut self.owner_type, &new_val.owner_type);
        update_field(&mut self.owner, &new_val.owner);
        update_field(&mut self.owner_delegate_seq, &new_val.owner_delegate_seq);
        update_field(&mut self.delegate, &new_val.delegate);
    }

    pub fn merge_asset_owner(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                },
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize existing_val: {}", e)
                },
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.owner_type, &new_val.owner_type);
                        update_field(&mut current_val.owner, &new_val.owner);
                        update_field(
                            &mut current_val.owner_delegate_seq,
                            &new_val.owner_delegate_seq,
                        );
                        update_field(&mut current_val.delegate, &new_val.delegate);

                        current_val
                    } else {
                        new_val
                    });
                },
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize new_val: {}", e)
                },
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.owner.slot_updated,
            self.delegate.slot_updated,
            self.owner_type.slot_updated,
            self.owner_delegate_seq.slot_updated,
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
    }
}

impl TypedColumn for AssetLeafDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_LEAF";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetLeaf {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_LEAF_V2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetLeaf {
    pub fn merge_asset_leaf(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0u64;
        let mut leaf_seq: Option<u64> = None;

        // Decode existing value as AssetLeaf, since historically only AssetLeaf was stored.
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetLeaf>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    leaf_seq = value.leaf_seq;
                    result = existing_val.to_vec();
                },
                Err(e) => {
                    error!("RocksDB: AssetLeaf deserialize existing_val: {}", e);
                },
            }
        }

        let len = operands.len();

        for (i, op) in operands.iter().enumerate() {
            // Try to decode operand as SourcedAssetLeaf first
            let new_val = match deserialize::<SourcedAssetLeaf>(op) {
                Ok(si) => si,
                Err(_e_sourced) => {
                    // If fails, try decoding as AssetLeaf
                    match deserialize::<AssetLeaf>(op) {
                        Ok(al) => SourcedAssetLeaf { leaf: al, is_from_finalized_source: false },
                        Err(e_leaf) => {
                            // If last operand and still no result chosen, store empty if needed
                            if i == len - 1 && result.is_empty() {
                                error!(
                                    "RocksDB: last operand in AssetLeaf new_val could not be \
                                     deserialized as SourcedAssetLeaf or AssetLeaf. Empty array will be saved: {}",
                                    e_leaf
                                );
                                return Some(vec![]);
                            } else {
                                error!("RocksDB: AssetLeaf deserialize new_val failed: {}", e_leaf);
                            }
                            continue;
                        },
                    }
                },
            };

            let new_slot = new_val.leaf.slot_updated;
            let new_seq = new_val.leaf.leaf_seq;

            // Determine if this new value outranks the existing one
            // Outranking conditions:
            // 1. Higher slot than current.
            // 2. If slot is equal, but leaf_seq is strictly greater.
            // 3. If from a finalized source and has a strictly greater leaf_seq than current.
            let newer = match new_slot.cmp(&slot) {
                Ordering::Greater => true,
                Ordering::Equal => match (leaf_seq, new_seq) {
                    (Some(current_seq), Some(candidate_seq)) => candidate_seq > current_seq,
                    (None, Some(_)) => true, // previously no sequence, now we have one, lets use it
                    _ => false, // either both none or candidate_seq is none and current_seq is some
                },
                Ordering::Less => false,
            };

            let finalized_newer = new_val.is_from_finalized_source
                && match (leaf_seq, new_seq) {
                    (Some(current_seq), Some(candidate_seq)) => candidate_seq > current_seq,
                    (None, Some(_)) => true, // previously no sequence, now we have one
                    _ => false,              // same logic as above
                };

            if newer || finalized_newer {
                // If this new_val outranks the existing value:
                // store only the AssetLeaf portion
                match serialize(&new_val.leaf) {
                    Ok(serialized) => {
                        result = serialized;
                        slot = new_slot;
                        leaf_seq = new_seq;
                    },
                    Err(e) => {
                        error!(
                            "RocksDB: Failed to serialize AssetLeaf from SourcedAssetLeaf: {}",
                            e
                        );
                    },
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for AssetCollectionDeprecated {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_COLLECTION";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetCollection {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_COLLECTION_v2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetCollection {
    pub fn merge(&mut self, new_val: &Self) {
        update_field(&mut self.collection, &new_val.collection);
        update_field(&mut self.is_collection_verified, &new_val.is_collection_verified);
        update_field(&mut self.authority, &new_val.authority);
    }

    pub fn merge_asset_collection(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                },
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
                },
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        current_val.merge(&new_val);
                        current_val
                    } else {
                        new_val
                    });
                },
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize new_val: {}", e)
                },
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.collection.slot_updated,
            self.is_collection_verified.slot_updated,
            self.authority.slot_updated,
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
    }

    pub fn get_seq(&self) -> Option<u64> {
        let seq_1 = match self.authority.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        let seq_2 = match self.collection.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        let seq_3 = match self.is_collection_verified.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        [seq_1, seq_2, seq_3].into_iter().flatten().max()
    }
}

// AssetsUpdateIdx is a column family that is used to query the assets that were updated in (or after) a particular slot.
// The key is a concatenation of the slot and the asset pubkey.
// This will be used in the batch updater to the secondary index database. The batches should be constructed based on the slot, with an overlap of 1 slot including the last processed slot.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetsUpdateIdx {}

impl TypedColumn for AssetsUpdateIdx {
    type KeyType = Vec<u8>;
    type ValueType = Self;
    const NAME: &'static str = "ASSETS_UPDATED_IN_SLOT_IDX";

    fn encode_key(key: Vec<u8>) -> Vec<u8> {
        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        Ok(bytes)
    }
}

/// FungibleAssetsUpdateIx is the same as AssetsUpdateIdx, but for fungible assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FungibleAssetsUpdateIdx {}

impl TypedColumn for FungibleAssetsUpdateIdx {
    type KeyType = Vec<u8>;
    type ValueType = Self;
    const NAME: &'static str = "FUNGIBLE_ASSETS_UPDATED_IN_SLOT_IDX";

    fn encode_key(key: Vec<u8>) -> Vec<u8> {
        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        Ok(bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlotAssetIdx {}

impl TypedColumn for SlotAssetIdx {
    type KeyType = SlotAssetIdxKey;
    type ValueType = Self;
    const NAME: &'static str = "SLOT_ASSET_IDX";

    fn encode_key(key: SlotAssetIdxKey) -> Vec<u8> {
        key.encode_to_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        SlotAssetIdxKey::decode_from_bypes(bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SlotAssetIdxKey {
    pub slot: u64,
    pub pubkey: Pubkey,
}

impl SlotAssetIdxKey {
    pub fn new(slot: u64, asset: Pubkey) -> SlotAssetIdxKey {
        SlotAssetIdxKey { slot, pubkey: asset }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        encode_u64_pubkey(self.slot, self.pubkey)
    }

    pub fn decode_from_bypes(bytes: Vec<u8>) -> Result<SlotAssetIdxKey> {
        decode_u64_pubkey(bytes).map(|(slot, asset)| SlotAssetIdxKey { slot, pubkey: asset })
    }
}
#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use entities::models::Creator;
    use itertools::Itertools;

    use super::*;

    const TEST_DATA:&str = "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";
    const EXISTING_OWNER: &str = "4ja2N12Zczh9K25zGFTfao6yPdTZSfA5Bw4QueSmQCYJ";

    fn create_full_complete_asset() -> AssetCompleteDetails {
        let pubkey = Pubkey::new_unique();
        let static_details = AssetStaticDetails {
            pubkey,
            specification_asset_class: SpecificationAssetClass::Nft,
            royalty_target_type: RoyaltyTargetType::Creators,
            created_at: 12345,
            edition_address: None,
        };

        let dynamic_details = AssetDynamicDetails {
            pubkey,
            is_compressible: Updated::new(58, Some(UpdateVersion::Sequence(500)), false),
            is_compressed: Updated::new(580, Some(UpdateVersion::Sequence(530)), true),
            is_frozen: Updated::new(50, None, false),
            supply: Some(Updated::new(50, None, 1)),
            seq: Some(Updated::new(580, Some(UpdateVersion::Sequence(530)), 530)),
            is_burnt: Updated::new(50, None, false),
            was_decompressed: Some(Updated::new(50, None, false)),
            onchain_data: Some(Updated::new(
                50,
                Some(UpdateVersion::Sequence(530)),
                "onchain_data".to_string(),
            )),
            creators: Updated::new(
                50,
                None,
                vec![Creator { creator: pubkey, creator_verified: true, creator_share: 100 }],
            ),
            royalty_amount: Updated::new(50, None, 100),
            url: Updated::new(50, None, "url".to_string()),
            chain_mutability: Some(Updated::new(50, None, ChainMutability::Mutable)),
            lamports: Some(Updated::new(50, None, 100)),
            executable: Some(Updated::new(50, Some(UpdateVersion::Sequence(531)), false)),
            metadata_owner: Some(Updated::new(
                50,
                Some(UpdateVersion::Sequence(533)),
                "metadata_owner".to_string(),
            )),
            raw_name: Some(Updated::new(50, None, "raw_name".to_string())),
            mpl_core_plugins: Some(Updated::new(50, None, "mpl_core_plugins".to_string())),
            mpl_core_unknown_plugins: Some(Updated::new(
                50,
                None,
                "mpl_core_unknown_plugins".to_string(),
            )),
            rent_epoch: Some(Updated::new(50, Some(UpdateVersion::Sequence(533)), 100)),
            num_minted: Some(Updated::new(50, None, 100)),
            current_size: Some(Updated::new(50, Some(UpdateVersion::Sequence(533)), 100)),
            plugins_json_version: Some(Updated::new(50, Some(UpdateVersion::Sequence(535)), 100)),
            mpl_core_external_plugins: Some(Updated::new(
                50,
                Some(UpdateVersion::Sequence(537)),
                "mpl_core_external_plugins".to_string(),
            )),
            mpl_core_unknown_external_plugins: Some(Updated::new(
                50,
                Some(UpdateVersion::Sequence(539)),
                "mpl_core_unknown_external_plugins".to_string(),
            )),
            mint_extensions: Some(Updated::new(50, None, "mint_extensions".to_string())),
        };

        let authority = AssetAuthority {
            pubkey,
            write_version: Some(500),
            slot_updated: 5000,
            authority: Pubkey::new_unique(),
        };
        let owner = AssetOwner {
            pubkey,
            owner_type: Updated::new(50, None, OwnerType::Single),
            owner: Updated::new(51, Some(UpdateVersion::Sequence(53)), Some(Pubkey::new_unique())),
            delegate: Updated::new(
                56,
                Some(UpdateVersion::Sequence(54)),
                Some(Pubkey::new_unique()),
            ),
            owner_delegate_seq: Updated::new(58, None, None),
            is_current_owner: Updated::new(50, None, true),
        };

        let collection = AssetCollection {
            pubkey,
            collection: Updated::new(50, None, Pubkey::new_unique()),
            is_collection_verified: Updated::new(50, None, true),
            authority: Updated::new(
                58,
                Some(UpdateVersion::Sequence(48)),
                Some(Pubkey::new_unique()),
            ),
        };

        AssetCompleteDetails {
            pubkey,
            static_details: Some(static_details),
            dynamic_details: Some(dynamic_details),
            authority: Some(authority),
            owner: Some(owner),
            collection: Some(collection),
        }
    }

    #[test]
    fn test_merge_complete_details_with_no_operands_keeps_object_unchanged() {
        let asset = create_full_complete_asset();
        let mut builder = FlatBufferBuilder::with_capacity(2500);
        let asset_fb = asset.convert_to_fb(&mut builder);
        builder.finish_minimal(asset_fb);
        let origin_bytes = builder.finished_data();
        let operands = vec![];
        let key = rand::random::<[u8; 32]>();
        let result = merge_complete_details_fb_raw(&key, Some(origin_bytes), operands.into_iter())
            .expect("should return a result");
        assert_eq!(result, origin_bytes);
        fb::root_as_asset_complete_details(&result).expect("should decode");
    }

    #[test]
    fn test_merge_on_empty_existing_value() {
        let asset = create_full_complete_asset();
        let mut builder = FlatBufferBuilder::with_capacity(2500);
        let asset_fb = asset.convert_to_fb(&mut builder);
        builder.finish_minimal(asset_fb);
        let origin_bytes = builder.finished_data();
        let operands = vec![origin_bytes];
        let key = rand::random::<[u8; 32]>();
        let result = merge_complete_details_fb_raw(&key, None, operands.into_iter())
            .expect("should return a result");
        assert_eq!(result, origin_bytes);
        fb::root_as_asset_complete_details(&result).expect("should decode");
    }

    #[test]
    fn test_merge_only_dynamic_data_on_existing_data_without_dynamic_data() {
        let original_asset = create_full_complete_asset();
        let mut asset = original_asset.clone();
        asset.dynamic_details = None;
        let operand_asset = AssetCompleteDetails {
            pubkey: original_asset.pubkey.clone(),
            dynamic_details: original_asset.dynamic_details.clone(),
            ..Default::default()
        };
        let mut builder = FlatBufferBuilder::with_capacity(2500);
        let existing_fb = asset.convert_to_fb(&mut builder);
        builder.finish_minimal(existing_fb);
        let existing_bytes = builder.finished_data().to_owned();
        builder.reset();
        let operand_asset_fb = operand_asset.convert_to_fb(&mut builder);
        builder.finish_minimal(operand_asset_fb);
        let operand_bytes = builder.finished_data().to_owned();
        let operands = vec![operand_bytes.as_slice()];
        let key = rand::random::<[u8; 32]>();
        builder.reset();
        let expected_fb = original_asset.convert_to_fb(&mut builder);
        builder.finish_minimal(expected_fb);
        let expected_bytes = builder.finished_data();
        let result =
            merge_complete_details_fb_raw(&key, Some(&existing_bytes), operands.into_iter())
                .expect("should return a result");
        assert_eq!(result, expected_bytes);
        let result_asset = fb::root_as_asset_complete_details(&result).expect("should decode");
        assert_eq!(AssetCompleteDetails::from(result_asset), original_asset);
    }

    #[test]
    fn test_merge_with_some_fields_updated_with_higher_sequences_while_others_have_lower_sequences()
    {
        let original_asset = create_full_complete_asset();
        let operand_asset = AssetCompleteDetails {
            pubkey: original_asset.pubkey.clone(),
            dynamic_details: Some(AssetDynamicDetails {
                pubkey: original_asset.pubkey.clone(),
                is_compressible: Updated::new(59, Some(UpdateVersion::Sequence(510)), true),
                is_compressed: Updated::new(580, Some(UpdateVersion::Sequence(530)), false), // should not be updated (should keep original value)
                onchain_data: Some(Updated::new(
                    50,
                    Some(UpdateVersion::Sequence(540)),
                    "new_onchain_data".to_string(),
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut expected_asset = original_asset.clone();
        expected_asset.dynamic_details.as_mut().map(|ref mut dd| {
            dd.is_compressible = Updated::new(59, Some(UpdateVersion::Sequence(510)), true)
        });
        expected_asset.dynamic_details.as_mut().map(|ref mut dd| {
            dd.onchain_data = Some(Updated::new(
                50,
                Some(UpdateVersion::Sequence(540)),
                "new_onchain_data".to_string(),
            ))
        });

        let mut builder = FlatBufferBuilder::with_capacity(2500);
        let existing_fb = original_asset.convert_to_fb(&mut builder);
        builder.finish_minimal(existing_fb);
        let existing_bytes = builder.finished_data().to_owned();
        builder.reset();
        let operand_asset_fb = operand_asset.convert_to_fb(&mut builder);
        builder.finish_minimal(operand_asset_fb);
        let operand_bytes = builder.finished_data().to_owned();
        let operands = vec![operand_bytes.as_slice()];
        let key = rand::random::<[u8; 32]>();
        builder.reset();
        let expected_fb = expected_asset.convert_to_fb(&mut builder);
        builder.finish_minimal(expected_fb);
        let expected_bytes = builder.finished_data();
        let result =
            merge_complete_details_fb_raw(&key, Some(&existing_bytes), operands.into_iter())
                .expect("should return a result");
        let result_asset = fb::root_as_asset_complete_details(&result).expect("should decode");
        assert_eq!(AssetCompleteDetails::from(result_asset), expected_asset);
        assert_eq!(result, expected_bytes);
    }

    #[test]
    fn test_verify_backward_compatibility_decoding() {
        let data_bytes = solana_sdk::bs58::decode(TEST_DATA).into_vec().unwrap();

        let asset;
        unsafe {
            asset =
                crate::generated::asset_generated::asset::root_as_asset_complete_details_unchecked(
                    data_bytes.as_slice(),
                );
        }
        let asset_mapped = AssetCompleteDetails::from(asset);
        println!("STATIC: {:#?}", asset.static_details().is_some());
        println!("DYNAMIC: {:#?}", asset.dynamic_details().is_some());
        println!("OWNER: {:#?}", asset.owner().is_some());
        println!("AUTHORITY: {:#?}", asset.authority().is_some());
        println!("COLLECTION: {:#?}", asset.collection().is_some());
        println!("SERIALIZED: {:#?}", asset_mapped);
    }

    #[test]
    fn test_merge_with_same_pubkey_higher_write_version() {
        let data_bytes = solana_sdk::bs58::decode(TEST_DATA).into_vec().unwrap();
        let new_owner = Pubkey::from_str("2jL7yFGXkKE9oi1xHsA45UzV5491PD55AugGJiTbUr9m").unwrap();
        let owner = AssetOwner {
            pubkey: Pubkey::from_str("DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ").unwrap(),
            // a new owner with smaller slot and higher write version
            owner: Updated {
                value: Some(new_owner.clone()),
                slot_updated: 1,
                update_version: Some(UpdateVersion::WriteVersion(388329656)),
            },
            is_current_owner: Updated {
                value: true,
                slot_updated: 1,
                update_version: Some(UpdateVersion::WriteVersion(388329656)),
            },
            ..Default::default()
        };
        let mut builder = FlatBufferBuilder::new();
        let asset_complete_details = owner.convert_to_fb(&mut builder);
        builder.finish_minimal(asset_complete_details);
        let operand_bytes = builder.finished_data();

        let merge_result = merge_complete_details_fb_simple_raw(
            &[],
            Some(&data_bytes.as_slice()),
            vec![operand_bytes].into_iter(),
        )
        .expect("expected merge to return some value");

        let asset;
        unsafe {
            asset =
                crate::generated::asset_generated::asset::root_as_asset_complete_details_unchecked(
                    merge_result.as_slice(),
                );
        }
        assert!(asset.other_known_owners().is_none());
        let asset_mapped = AssetCompleteDetails::from(asset);
        // Now that slot is prioritized over version, we expect the existing owner to be kept
        // because the new owner has a smaller slot (1) despite higher write version
        assert_eq!(
            asset_mapped.owner.as_ref().unwrap().owner.value.unwrap(),
            Pubkey::from_str(EXISTING_OWNER).unwrap()
        );
    }

    #[test]
    fn test_merge_with_same_pubkey_higher_slot_smaller_write_version() {
        let data_bytes = solana_sdk::bs58::decode(TEST_DATA).into_vec().unwrap();
        let new_owner = Pubkey::from_str("2jL7yFGXkKE9oi1xHsA45UzV5491PD55AugGJiTbUr9m").unwrap();
        let owner = AssetOwner {
            pubkey: Pubkey::from_str("DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ").unwrap(),
            // a new owner with higher slot and smaller write version
            owner: Updated {
                value: Some(new_owner.clone()),
                slot_updated: u64::MAX,
                update_version: Some(UpdateVersion::WriteVersion(388329654)),
            },
            is_current_owner: Updated {
                value: true,
                slot_updated: u64::MAX,
                update_version: Some(UpdateVersion::WriteVersion(388329654)),
            },
            ..Default::default()
        };
        let mut builder = FlatBufferBuilder::new();
        let asset_complete_details = owner.convert_to_fb(&mut builder);
        builder.finish_minimal(asset_complete_details);
        let operand_bytes = builder.finished_data();

        let merge_result = merge_complete_details_fb_simple_raw(
            &[],
            Some(&data_bytes.as_slice()),
            vec![operand_bytes].into_iter(),
        )
        .expect("expected merge to return some value");

        let asset;
        unsafe {
            asset =
                crate::generated::asset_generated::asset::root_as_asset_complete_details_unchecked(
                    merge_result.as_slice(),
                );
        }
        assert!(asset.other_known_owners().is_none());
        let asset_mapped = AssetCompleteDetails::from(asset);
        // With slot prioritized over version, the new owner should be used because it has a higher slot
        // even though it has a smaller write version
        assert_eq!(asset_mapped.owner.as_ref().unwrap().owner.value.unwrap(), new_owner);
        assert!(asset_mapped.owner.unwrap().is_current_owner.value);
    }

    #[test]
    fn test_merge_with_different_pubkeys_same_slot_3_owners_changed() {
        // The asset is transferred from owner A to owner B and then to owner C
        // This means the following updates will happen: owner A(is current owner = false), owner B(is current owner = true), owner B(is current owner = false), owner C(is current owner = true)
        // the final state should be owner C as the current owner and owner A and B in the other known owners
        // this should happen without any regards to the order of updates
        let original_data_bytes = solana_sdk::bs58::decode(TEST_DATA).into_vec().unwrap();
        let owner_a = Pubkey::from_str(EXISTING_OWNER).unwrap();
        let _owner_a_token_account_pubkey =
            Pubkey::from_str("DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ").unwrap();

        let owner_b = Pubkey::from_str("2jL7yFGXkKE9oi1xHsA45UzV5491PD55AugGJiTbUr9m").unwrap();
        let owner_b_token_account_pubkey = Pubkey::new_unique();

        let owner_c = Pubkey::from_str("9Rfs2otkZpsLPomKUGku7DaFv9YvtkV9a87nqTUgMBhC").unwrap();
        let owner_c_token_account_pubkey = Pubkey::new_unique();

        let slot = 26979338;

        let mut builder = FlatBufferBuilder::new();

        let owner_a_not_an_owner_data = AssetOwner {
            pubkey: Pubkey::from_str("DvpMQyF8sT6hPBewQf6VrVESw6L1zewPyNit1CSt1tDJ").unwrap(),
            owner: Updated {
                value: Some(owner_a.clone()),
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(388329657)),
            },
            is_current_owner: Updated {
                value: false,
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(388329657)),
            },
            ..Default::default()
        }
        .convert_to_fb(&mut builder);
        builder.finish_minimal(owner_a_not_an_owner_data);
        let owner_a_not_an_owner_data = builder.finished_data().to_vec();
        builder.reset();

        let owner_b_is_owner_data = AssetOwner {
            pubkey: owner_b_token_account_pubkey,
            owner: Updated {
                value: Some(owner_b.clone()),
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(10)),
            },
            is_current_owner: Updated {
                value: true,
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(10)),
            },
            ..Default::default()
        }
        .convert_to_fb(&mut builder);
        builder.finish_minimal(owner_b_is_owner_data);
        let owner_b_is_owner_data = builder.finished_data().to_vec();
        builder.reset();

        let owner_b_not_owner_data = AssetOwner {
            pubkey: owner_b_token_account_pubkey,
            owner: Updated {
                value: Some(owner_b.clone()),
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(11)),
            },
            is_current_owner: Updated {
                value: false,
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(11)),
            },
            ..Default::default()
        }
        .convert_to_fb(&mut builder);
        builder.finish_minimal(owner_b_not_owner_data);
        let owner_b_not_owner_data = builder.finished_data().to_vec();
        builder.reset();

        let owner_c_is_owner_data = AssetOwner {
            pubkey: owner_c_token_account_pubkey,
            owner: Updated {
                value: Some(owner_c.clone()),
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(12)),
            },
            is_current_owner: Updated {
                value: true,
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(12)),
            },
            ..Default::default()
        }
        .convert_to_fb(&mut builder);
        builder.finish_minimal(owner_c_is_owner_data);
        let owner_c_is_owner_data = builder.finished_data().to_vec();
        builder.reset();

        // collect all the possible combinations of the updates, as the order of the updates should not matter
        // first using a single call with multiple operands, then using multiple calls with a single operand
        // all the combinations should result in the same final bytes

        // Collect all the updates into a vector
        let updates = vec![
            ("A", owner_a_not_an_owner_data.as_slice()),
            ("B1", owner_b_is_owner_data.as_slice()),
            ("B2", owner_b_not_owner_data.as_slice()),
            ("C", owner_c_is_owner_data.as_slice()),
        ];

        // Generate all permutations of the updates
        let permutations = updates.iter().permutations(updates.len());
        let merge_result = merge_complete_details_fb_simple_raw(
            &[],
            Some(&original_data_bytes.as_slice()),
            vec![
                owner_a_not_an_owner_data.as_slice(),
                owner_b_is_owner_data.as_slice(),
                owner_b_not_owner_data.as_slice(),
                owner_c_is_owner_data.as_slice(),
            ]
            .into_iter(), //perm.into_iter().map(|d| *d),
        )
        .expect("expected merge to return some value");
        let expected_result = merge_result;

        for perm in permutations {
            let merge_result = merge_complete_details_fb_simple_raw(
                &[],
                Some(&original_data_bytes.as_slice()),
                perm.clone().into_iter().map(|(_, d)| *d),
            )
            .expect("expected merge to return some value");
            let perm_name = perm.iter().map(|(k, _)| k).join(", ");
            let asset;
            unsafe {
                asset = crate::generated::asset_generated::asset::root_as_asset_complete_details_unchecked(
                    merge_result.as_slice(),
                );
            }
            let asset_mapped = AssetCompleteDetails::from(asset);
            assert!(
                asset_mapped.owner.as_ref().unwrap().is_current_owner.value,
                "owner should be current for one permutation {}",
                perm_name
            );
            assert_eq!(
                asset_mapped.owner.as_ref().unwrap().owner.value.unwrap(),
                owner_c,
                "Owner should be C for one permutation {}",
                perm_name,
            );
            assert!(asset.other_known_owners().is_some());
            assert_eq!(asset.other_known_owners().unwrap().len(), 2);
            assert_eq!(
                asset.other_known_owners().unwrap().get(0).is_current_owner().unwrap().value(),
                false
            );
            assert_eq!(
                asset.other_known_owners().unwrap().get(1).is_current_owner().unwrap().value(),
                false
            );
            assert_eq!(
                merge_result, expected_result,
                "Merge result differs for one permutation {}",
                perm_name,
            );
        }
    }

    #[test]
    fn test_merge_with_different_pubkeys_different_slots_owner_not_changed() {
        // The asset recieves some stale update with is_owner set to false. No change of ownership is expected
        // The example is taken from Eclipse asset F9fyHSja6zTiXjgMPQXpZJuJ4GW97Mpc6UrSX21CBBJ2 that had issues.
        let original_data_bytes = solana_sdk::bs58::decode("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").into_vec().unwrap();
        let existing_owner =
            Pubkey::from_str("6wcxtwMH4ZTDFNDyVwgWUvMnTqW1v8gyNULDzDFtFEoA").unwrap();

        let updated_owner =
            Pubkey::from_str("3GTaP1A8qdNGGMme8mcvmRZURQYeFHaWS3tM9UhTH5V9").unwrap();
        let updated_owner_pubkey =
            Pubkey::from_str("DspBshRwNY1bJ9HCyWpgeNQZzv7BnAoPs4bzHZ82muPk").unwrap();

        let slot = 42751100;
        let wv = 9580929183;

        let mut builder = FlatBufferBuilder::new();

        let owner_a_not_an_owner_data = AssetOwner {
            pubkey: updated_owner_pubkey,
            owner: Updated {
                value: Some(updated_owner),
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(wv)),
            },
            is_current_owner: Updated {
                value: false,
                slot_updated: slot,
                update_version: Some(UpdateVersion::WriteVersion(wv)),
            },
            ..Default::default()
        }
        .convert_to_fb(&mut builder);
        builder.finish_minimal(owner_a_not_an_owner_data);
        let owner_a_not_an_owner_data = builder.finished_data().to_vec();
        builder.reset();

        let merge_result = merge_complete_details_fb_simple_raw(
            &[],
            Some(&original_data_bytes.as_slice()),
            vec![owner_a_not_an_owner_data.as_slice()].into_iter(),
        )
        .expect("expected merge to return some value");
        let resulting_asset = fb::root_as_asset_complete_details(&merge_result).unwrap();

        assert_eq!(
            resulting_asset.owner().unwrap().owner().unwrap().value().unwrap().bytes(),
            existing_owner.to_bytes()
        );
    }
}
