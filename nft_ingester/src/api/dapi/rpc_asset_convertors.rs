use std::{cmp::Ordering, collections::HashMap, path::Path};

use entities::{
    api_req_params::Pagination,
    enums::{Interface, SpecificationVersions},
    models::{AssetSignatureWithPagination, CoreFeesAccountWithSortingID, TokenAccResponse},
};
use jsonpath_lib::JsonPathError;
use mime_guess::Mime;
use mpl_bubblegum::Flags;
use num_traits::Pow;
use rocks_db::{
    columns::{
        asset::{AssetAuthority, AssetCollection, AssetDynamicDetails, AssetStaticDetails},
        offchain_data::OffChainData,
    },
    errors::StorageError,
};
use serde_json::Value;
use solana_program::pubkey::Pubkey;
use tracing::{error, warn};
use url::Url;
use usecase::response_prettier::filter_non_null_fields;

use super::{
    response::{AssetError, CoreFeesAccountsList, TokenAccountsList, TransactionSignatureList},
    rpc_asset_models::{
        Asset as RpcAsset, Authority, Compression, Content, Creator, File, FullAsset, Group,
        MetadataMap, MplCoreInfo, Ownership, Royalty, Scope, Supply, Uses,
    },
};
use crate::api::dapi::{
    asset::COLLECTION_GROUP_KEY,
    model::ChainMutability,
    response::InscriptionResponse,
    rpc_asset_models::{PriceInfo, TokenInfo},
};

pub fn to_uri(uri: String) -> Option<Url> {
    Url::parse(uri.as_str()).ok()
}

pub fn get_mime(url: Url) -> Option<Mime> {
    mime_guess::from_path(Path::new(url.path())).first()
}

pub fn get_mime_type_from_uri(uri: String) -> String {
    let default_mime_type = "image/png".to_string();
    to_uri(uri).and_then(get_mime).map_or(default_mime_type, |m| m.to_string())
}

pub fn file_from_str(str: String) -> File {
    let mime = get_mime_type_from_uri(str.clone());
    File { uri: Some(str), cdn_uri: None, mime: Some(mime), quality: None, contexts: None }
}

pub fn track_top_level_file(
    file_map: &mut HashMap<String, File>,
    top_level_file: Option<&serde_json::Value>,
) {
    if top_level_file.is_some() {
        let img = top_level_file.and_then(|x| x.as_str());
        if let Some(img) = img {
            let entry = file_map.get(img);
            if entry.is_none() {
                file_map.insert(img.to_string(), file_from_str(img.to_string()));
            }
        }
    }
}

pub fn safe_select<'a>(
    selector: &mut impl FnMut(&str) -> Result<Vec<&'a Value>, JsonPathError>,
    expr: &str,
) -> Option<&'a Value> {
    selector(expr).ok().filter(|d| !Vec::is_empty(d)).as_mut().and_then(|v| v.pop())
}

pub fn get_content(
    asset_dynamic: &AssetDynamicDetails,
    offchain_data: &OffChainData,
) -> Result<Content, StorageError> {
    let json_uri = asset_dynamic.url.value.clone();
    let metadata = serde_json::from_str(&offchain_data.metadata.clone().unwrap_or_default())
        .unwrap_or(Value::Null);
    let chain_data: Value =
        serde_json::from_str(asset_dynamic.onchain_data.as_ref().map_or("{}", |data| &data.value))
            .unwrap_or(Value::Null);

    let mut selector_fn = jsonpath_lib::selector(&metadata);
    let mut chain_data_selector_fn = jsonpath_lib::selector(&chain_data);
    let selector = &mut selector_fn;
    let chain_data_selector = &mut chain_data_selector_fn;
    let mut meta: MetadataMap = MetadataMap::new();

    let name = safe_select(chain_data_selector, "$.name");
    if let Some(name) = name {
        meta.set_item("name", name.clone());
    }

    let symbol = safe_select(chain_data_selector, "$.symbol");
    if let Some(symbol) = symbol {
        meta.set_item("symbol", symbol.clone());
    }

    let desc = safe_select(selector, "$.description");
    if let Some(desc) = desc {
        meta.set_item("description", desc.clone());
    }

    let attributes = safe_select(selector, "$.attributes");
    if let Some(attributes) = attributes {
        meta.set_item("attributes", attributes.clone());
    }

    let token_standard = safe_select(chain_data_selector, "$.token_standard");
    if let Some(standard) = token_standard {
        meta.set_item("token_standard", standard.clone());
    }

    let (links, mut files) = parse_files_from_selector(selector);

    // List the defined image file before the other files (if one exists).
    files.sort_by(|a, _: &File| match (a.uri.as_ref(), links.get("image")) {
        (Some(x), Some(y)) => {
            if x == y {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        },
        _ => Ordering::Equal,
    });

    Ok(Content {
        schema: "https://schema.metaplex.com/nft1.0.json".to_string(),
        json_uri,
        files: Some(files),
        metadata: meta,
        links: Some(links),
    })
}

pub fn parse_files_from_selector<'a>(
    selector: &mut impl FnMut(&str) -> Result<Vec<&'a Value>, JsonPathError>,
) -> (HashMap<String, Value>, Vec<File>) {
    let mut links = HashMap::new();
    let link_fields = vec!["image", "animation_url", "external_url"];
    for f in link_fields {
        let l = safe_select(selector, format!("$.{}", f).as_str());
        if let Some(l) = l {
            links.insert(f.to_string(), l.to_owned());
        }
    }

    let mut actual_files: HashMap<String, File> = HashMap::new();
    if let Some(files) = selector("$.properties.files[*]").ok().filter(|d| !d.is_empty()) {
        for v in files.iter() {
            if v.is_object() {
                // Some assets don't follow the standard and specifiy 'url' instead of 'uri'
                let mut uri = v.get("uri");
                if uri.is_none() {
                    uri = v.get("url");
                }
                let mime_type = v.get("type");

                match (uri, mime_type) {
                    (Some(u), Some(m)) => {
                        if let Some(str_uri) = u.as_str() {
                            let file = if let Some(str_mime) = m.as_str() {
                                File {
                                    uri: Some(str_uri.to_string()),
                                    cdn_uri: None,
                                    mime: Some(str_mime.to_string()),
                                    quality: None,
                                    contexts: None,
                                }
                            } else {
                                warn!("Mime is not string: {:?}", m);
                                file_from_str(str_uri.to_string())
                            };
                            actual_files.insert(str_uri.to_string(), file);
                        } else {
                            warn!("URI is not string: {:?}", u);
                        }
                    },
                    (Some(u), None) => {
                        let str_uri = serde_json::to_string(u).unwrap_or_default();
                        actual_files.insert(str_uri.clone(), file_from_str(str_uri));
                    },
                    _ => {},
                }
            } else if v.is_string() {
                let str_uri = v.as_str().unwrap().to_string();
                actual_files.insert(str_uri.clone(), file_from_str(str_uri));
            }
        }
    }

    track_top_level_file(&mut actual_files, links.get("image"));
    track_top_level_file(&mut actual_files, links.get("animation_url"));

    let files: Vec<File> = actual_files.into_values().collect();
    (links, files)
}

pub fn parse_files(metadata: &str) -> Option<Vec<File>> {
    serde_json::from_str(metadata)
        .map(|metadata_json| {
            let mut selector_fn = jsonpath_lib::selector(&metadata_json);
            let (_, files) = parse_files_from_selector(&mut selector_fn);
            files
        })
        .ok()
}

fn extract_collection_metadata(
    asset_dynamic: &AssetDynamicDetails,
    offchain_data: &OffChainData,
) -> MetadataMap {
    let metadata = serde_json::from_str(&offchain_data.metadata.clone().unwrap_or_default())
        .unwrap_or(Value::Null);
    let chain_data: Value =
        serde_json::from_str(asset_dynamic.onchain_data.as_ref().map_or("{}", |data| &data.value))
            .unwrap_or(Value::Null);

    let mut selector_fn = jsonpath_lib::selector(&metadata);
    let mut chain_data_selector_fn = jsonpath_lib::selector(&chain_data);
    let selector = &mut selector_fn;
    let chain_data_selector = &mut chain_data_selector_fn;
    let mut meta: MetadataMap = MetadataMap::new();

    let name = safe_select(chain_data_selector, "$.name");
    if let Some(name) = name {
        meta.set_item("name", name.clone());
    }

    let link_fields = vec!["name", "symbol"];
    for name in link_fields {
        let value = safe_select(chain_data_selector, format!("$.{}", name).as_str());
        if let Some(symbol) = value {
            meta.set_item(name, symbol.clone());
        } else {
            meta.set_item(name, "".into());
        }
    }

    let link_fields = vec!["image", "external_url", "description"];
    for name in link_fields {
        let value = safe_select(selector, format!("$.{}", name).as_str());
        if let Some(symbol) = value {
            meta.set_item(name, symbol.clone());
        } else {
            meta.set_item(name, "".into());
        }
    }

    meta
}

pub fn to_authority(
    authority: &Option<AssetAuthority>,
    mpl_core_collection: &Option<AssetCollection>,
) -> Vec<Authority> {
    let update_authority =
        mpl_core_collection.clone().and_then(|update_authority| update_authority.authority.value);

    // even if there is no authority for asset we should not set Pubkey::default(), just empty string
    let auth_key = update_authority.map(|update_authority| update_authority.to_string()).unwrap_or(
        authority.as_ref().map(|auth| auth.authority.to_string()).unwrap_or("".to_string()),
    );

    vec![Authority { address: auth_key, scopes: vec![Scope::Full] }]
}

pub fn to_creators(asset_dynamic: &AssetDynamicDetails) -> Vec<Creator> {
    asset_dynamic
        .creators
        .value
        .iter()
        .map(|creator| Creator {
            address: bs58::encode(&creator.creator).into_string(),
            share: creator.creator_share as i32,
            verified: creator.creator_verified,
        })
        .collect()
}

pub fn to_grouping(
    asset_collection: &Option<AssetCollection>,
    collection_dynamic_data: &Option<AssetDynamicDetails>,
    collection_offchain_data: &Option<OffChainData>,
) -> Vec<Group> {
    asset_collection.clone().map_or(vec![], |collection| {
        vec![Group {
            group_key: COLLECTION_GROUP_KEY.to_string(),
            group_value: Some(collection.collection.value.to_string()),
            verified: Some(collection.is_collection_verified.value),
            collection_metadata: collection_dynamic_data
                .as_ref()
                .zip(collection_offchain_data.as_ref())
                .map(|(collection_dynamic_data, collection_offchain_data)| {
                    extract_collection_metadata(collection_dynamic_data, collection_offchain_data)
                }),
        }]
    })
}

pub fn get_interface(asset_static: &AssetStaticDetails) -> Result<Interface, StorageError> {
    Ok(Interface::from((&SpecificationVersions::V1, &asset_static.specification_asset_class)))
}

pub fn asset_to_rpc(
    full_asset: FullAsset,
    owner_address: &Option<Pubkey>,
) -> Result<Option<RpcAsset>, StorageError> {
    let rpc_authorities =
        to_authority(&full_asset.assets_authority, &full_asset.mpl_core_collections);
    let rpc_creators = to_creators(&full_asset.asset_dynamic);
    let rpc_groups = to_grouping(
        &full_asset.asset_collections,
        &full_asset.collection_dynamic_data,
        &full_asset.collection_offchain_data,
    );
    let interface = get_interface(&full_asset.asset_static)?;

    let mut owner = full_asset
        .asset_owner
        .owner
        .value
        .map(|o| bs58::encode(o).into_string())
        .unwrap_or_default();
    let mut grouping = Some(rpc_groups);
    let mut frozen = full_asset.asset_dynamic.is_frozen.value;
    match interface {
        Interface::FungibleAsset | Interface::FungibleToken => {
            owner = "".to_string();
            grouping = Some(vec![]);
            frozen = false;
        },
        _ => {},
    }
    if let Some(owner_address) = owner_address {
        owner = owner_address.to_string()
    }
    let content = get_content(&full_asset.asset_dynamic, &full_asset.offchain_data)?;
    let ch_data = serde_json::from_str(
        &full_asset
            .asset_dynamic
            .onchain_data
            .as_ref()
            .map(|onchain_data| onchain_data.value.clone())
            .unwrap_or_default(),
    )
    .unwrap_or(serde_json::Value::Null);
    let mut chain_data_selector_fn = jsonpath_lib::selector(&ch_data);
    let chain_data_selector = &mut chain_data_selector_fn;
    let basis_points = safe_select(chain_data_selector, "$.primary_sale_happened")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let edition_nonce =
        safe_select(chain_data_selector, "$.edition_nonce").and_then(|v| v.as_u64());

    let mpl_core_info = match interface {
        Interface::MplCoreAsset | Interface::MplCoreCollection => Some(MplCoreInfo {
            num_minted: full_asset.asset_dynamic.num_minted.as_ref().map(|u| u.value),
            current_size: full_asset.asset_dynamic.current_size.as_ref().map(|u| u.value),
            plugins_json_version: full_asset
                .asset_dynamic
                .plugins_json_version
                .as_ref()
                .map(|u| u.value),
        }),
        _ => None,
    };
    let supply = match interface {
        Interface::V1NFT
        | Interface::LegacyNft
        | Interface::Nft
        | Interface::ProgrammableNFT
        | Interface::Custom => {
            if let Some(edition_info) = &full_asset.edition_data {
                Some(Supply {
                    edition_nonce,
                    print_current_supply: edition_info.supply,
                    print_max_supply: edition_info.max_supply,
                    edition_number: edition_info.edition_number,
                })
            } else {
                Some(Supply {
                    edition_nonce,
                    print_current_supply: 0,
                    print_max_supply: Some(0),
                    edition_number: None,
                })
            }
        },
        _ => None,
    };

    let non_transferable = if let Some(flags) = full_asset.asset_leaf.flags {
        let flags_bitfield = Flags::from_bytes([flags]);
        Some(flags_bitfield.asset_lvl_frozen() || flags_bitfield.permanent_lvl_frozen())
    } else {
        None
    };

    Ok(Some(RpcAsset {
        interface,
        id: full_asset.asset_static.pubkey.to_string(),
        content: Some(content),
        authorities: Some(rpc_authorities),
        mutable: full_asset
            .asset_dynamic
            .chain_mutability
            .clone()
            .map(|m| m.value.into())
            .unwrap_or(ChainMutability::Unknown)
            .into(),
        compression: Some(get_compression_info(&full_asset)),
        grouping,
        royalty: Some(Royalty {
            royalty_model: full_asset.asset_static.royalty_target_type.into(),
            target: None,
            percent: (full_asset.asset_dynamic.royalty_amount.value as f64) * 0.0001,
            basis_points: full_asset.asset_dynamic.royalty_amount.value as u32,
            primary_sale_happened: basis_points,
            locked: false,
        }),
        creators: Some(rpc_creators),
        ownership: Ownership {
            frozen,
            non_transferable,
            delegated: full_asset.asset_owner.delegate.value.is_some(),
            delegate: full_asset.asset_owner.delegate.value.map(|u| u.to_string()),
            ownership_model: full_asset.asset_owner.owner_type.value.into(),
            owner,
        },
        supply,
        uses: ch_data.get("uses").map(|u| Uses {
            use_method: u
                .get("use_method")
                .and_then(|s| s.as_str())
                .unwrap_or("Single")
                .to_string()
                .into(),
            total: u.get("total").and_then(|t| t.as_u64()).unwrap_or(0),
            remaining: u.get("remaining").and_then(|t| t.as_u64()).unwrap_or(0),
        }),
        burnt: full_asset.asset_dynamic.is_burnt.value,
        lamports: full_asset.asset_dynamic.lamports.map(|u| u.value),
        executable: full_asset.asset_dynamic.executable.map(|u| u.value),
        metadata_owner: full_asset.asset_dynamic.metadata_owner.map(|u| u.value),
        rent_epoch: full_asset.asset_dynamic.rent_epoch.map(|u| u.value),
        plugins: full_asset
            .asset_dynamic
            .mpl_core_plugins
            .map(|plugins| serde_json::from_str(&plugins.value).unwrap_or(serde_json::Value::Null)),
        unknown_plugins: full_asset
            .asset_dynamic
            .mpl_core_unknown_plugins
            .map(|plugins| serde_json::from_str(&plugins.value).unwrap_or(serde_json::Value::Null)),
        mpl_core_info,
        external_plugins: full_asset
            .asset_dynamic
            .mpl_core_external_plugins
            .map(|plugins| serde_json::from_str(&plugins.value).unwrap_or(serde_json::Value::Null)),
        unknown_external_plugins: full_asset
            .asset_dynamic
            .mpl_core_unknown_external_plugins
            .map(|plugins| serde_json::from_str(&plugins.value).unwrap_or(serde_json::Value::Null)),
        inscription: full_asset.inscription.map(|inscription| InscriptionResponse {
            authority: inscription.authority.to_string(),
            content_type: inscription.content_type,
            encoding: inscription.encoding,
            inscription_data_account: inscription.inscription_data_account.to_string(),
            order: inscription.order,
            size: inscription.size,
            validation_hash: inscription.validation_hash,
        }),
        spl20: full_asset
            .inscription_data
            .map(|inscription_data| serde_json::from_slice(inscription_data.data.as_slice()))
            .transpose()
            .ok()
            .flatten(),
        mint_extensions: filter_non_null_fields(
            full_asset
                .asset_dynamic
                .mint_extensions
                .and_then(|mint_extensions| serde_json::from_str(&mint_extensions.value).ok())
                .as_ref(),
        ),
        token_info: full_asset.spl_mint.map(|spl_mint| TokenInfo {
            symbol: full_asset.token_symbol,
            balance: full_asset.token_account.as_ref().map(|ta| ta.amount),
            supply: Some(spl_mint.supply),
            decimals: Some(spl_mint.decimals),
            token_program: Some(spl_mint.token_program.to_string()),
            associated_token_address: full_asset
                .token_account
                .as_ref()
                .map(|ta| ta.pubkey.to_string()),
            mint_authority: spl_mint.mint_authority.map(|a| a.to_string()),
            freeze_authority: spl_mint.freeze_authority.map(|a| a.to_string()),
            price_info: full_asset.token_price.map(|price| PriceInfo {
                price_per_token: Some(price),
                total_price: full_asset
                    .token_account
                    .as_ref()
                    .map(|ta| ta.amount as f64 * price / 10f64.pow(spl_mint.decimals as f64)),
                currency: Some("USDC".to_string()),
            }),
        }),
    }))
}

pub fn get_compression_info(full_asset: &FullAsset) -> Compression {
    let tree = if full_asset.asset_leaf.tree_id == Pubkey::default() {
        None
    } else {
        Some(full_asset.asset_leaf.tree_id.to_bytes().to_vec())
    };

    if let Some(was_decompressed) = &full_asset.asset_dynamic.was_decompressed {
        if was_decompressed.value {
            return Compression::default();
        }
    }

    Compression {
        eligible: full_asset.asset_dynamic.is_compressible.value,
        compressed: full_asset.asset_dynamic.is_compressed.value,
        leaf_id: full_asset.asset_leaf.nonce.unwrap_or(0),
        seq: std::cmp::max(
            full_asset.asset_dynamic.seq.clone().map(|u| u.value).unwrap_or(0),
            full_asset.asset_leaf.leaf_seq.unwrap_or(0),
        ),
        tree: tree.map(|s| bs58::encode(s).into_string()).unwrap_or_default(),
        asset_hash: full_asset
            .asset_leaf
            .leaf
            .as_ref()
            .map(|s| bs58::encode(s).into_string())
            .unwrap_or_default(),
        data_hash: full_asset.asset_leaf.data_hash.map(|e| e.to_string()).unwrap_or_default(),
        creator_hash: full_asset.asset_leaf.creator_hash.map(|e| e.to_string()).unwrap_or_default(),
        collection_hash: full_asset.asset_leaf.collection_hash.map(|e| e.to_string()),
        asset_data_hash: full_asset.asset_leaf.asset_data_hash.map(|e| e.to_string()),
        flags: full_asset.asset_leaf.flags,
    }
}

pub fn build_transaction_signatures_response(
    signatures: AssetSignatureWithPagination,
    limit: u64,
    page: Option<u64>,
) -> TransactionSignatureList {
    let items = signatures.asset_signatures.into_iter().map(|sig| sig.into()).collect::<Vec<_>>();
    TransactionSignatureList {
        total: items.len() as u32,
        limit: limit as u32,
        page: page.map(|x| x as u32),
        before: signatures.before.map(|before| before.to_string()),
        after: signatures.after.map(|after| after.to_string()),
        items,
    }
}

pub fn asset_list_to_rpc(
    asset_list: Vec<FullAsset>,
    owner_address: &Option<Pubkey>,
) -> (Vec<RpcAsset>, Vec<AssetError>) {
    asset_list.into_iter().fold((vec![], vec![]), |(mut assets, errors), asset| {
        match asset_to_rpc(asset.clone(), owner_address) {
            Ok(rpc_asset) => assets.push(rpc_asset.unwrap()),
            Err(e) => {
                error!(
                    "Could not cast asset to asset rpc type. Key: {:?}. Error: {:?}",
                    asset.asset_static.pubkey,
                    e.to_string()
                )
            },
        }
        (assets, errors)
    })
}

pub fn build_token_accounts_response(
    token_accounts: Vec<TokenAccResponse>,
    limit: u64,
    page: Option<u64>,
    cursor_enabled: bool,
) -> Result<TokenAccountsList, String> {
    let pagination = get_pagination_values(&token_accounts, &page, cursor_enabled)?;

    Ok(TokenAccountsList {
        total: token_accounts.len() as u32,
        limit: limit as u32,
        page: pagination.page,
        after: pagination.after,
        before: pagination.before,
        cursor: pagination.cursor,
        token_accounts: token_accounts.into_iter().map(|t| t.token_acc).collect(),
    })
}

pub fn build_core_fees_response(
    core_fees_account: Vec<CoreFeesAccountWithSortingID>,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
) -> Result<CoreFeesAccountsList, String> {
    Ok(CoreFeesAccountsList {
        total: core_fees_account.len() as u64,
        limit,
        page,
        core_fees_account: core_fees_account
            .into_iter()
            .map(|c| c.fees_account)
            .collect::<Vec<_>>(),
        after,
        before,
        cursor,
    })
}

fn get_pagination_values(
    token_accounts: &[TokenAccResponse],
    page: &Option<u64>,
    cursor_enabled: bool,
) -> Result<Pagination, String> {
    if cursor_enabled {
        if let Some(token_acc) = token_accounts.last() {
            Ok(Pagination { cursor: Some(token_acc.sorting_id.clone()), ..Default::default() })
        } else {
            Ok(Pagination::default())
        }
    } else if let Some(p) = page {
        Ok(Pagination { page: Some(*p as u32), ..Default::default() })
    } else {
        let first_row = token_accounts.first().map(|token_acc| token_acc.sorting_id.clone());

        let last_row = token_accounts.last().map(|token_acc| token_acc.sorting_id.clone());

        Ok(Pagination { after: last_row, before: first_row, ..Default::default() })
    }
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_eq;
    use entities::models::Updated;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_extract_collection_metadata() {
        let asset_dynamic = AssetDynamicDetails {
            url: Updated::new(12, None, "https://example.com".to_string()),
            onchain_data: Some(Updated::new(
                12,
                None,
                r#"{"name": "Test Asset", "symbol": "TST"}"#.to_string(),
            )),
            ..Default::default()
        };
        let offchain_data = OffChainData {
            metadata: Some(r#"{"image": "https://example.com/image.png", "external_url": "https://example.com", "description": "Test Description"}"#.to_string()),
            ..Default::default()
        };

        let expected_metadata = json!({
            "name": "Test Asset",
            "symbol": "TST",
            "image": "https://example.com/image.png",
            "external_url": "https://example.com",
            "description": "Test Description"
        });

        let result = extract_collection_metadata(&asset_dynamic, &offchain_data);

        assert_json_eq!(result, expected_metadata);
    }

    #[test]
    fn test_extract_collection_metadata_empty_fields() {
        let asset_dynamic = AssetDynamicDetails {
            url: Updated::new(12, None, "https://example.com".to_string()),
            onchain_data: Some(Updated::new(12, None, r#"{}"#.to_string())),
            ..Default::default()
        };
        let offchain_data =
            OffChainData { metadata: Some(r#"{}"#.to_string()), ..Default::default() };

        let expected_metadata = json!({
            "name": "",
            "symbol": "",
            "image": "",
            "external_url": "",
            "description": ""
        });

        let result = extract_collection_metadata(&asset_dynamic, &offchain_data);

        assert_json_eq!(result, expected_metadata);
    }
}
