use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::Path;

use entities::models::AssetSignatureWithPagination;
use entities::models::TokenAccResponse;
use jsonpath_lib::JsonPathError;
use log::error;
use log::warn;
use mime_guess::Mime;
use rocks_db::errors::StorageError;
use serde_json::Value;
use url::Url;

use crate::dao::{scopes::model, FullAsset};
use crate::rpc::response::{AssetError, TokenAccountsList, TransactionSignatureList};
use crate::rpc::{
    Asset as RpcAsset, Authority, Compression, Content, Creator, File, Group, Interface,
    MetadataMap, MplCoreInfo, Ownership, Royalty, Scope, Supply, Uses,
};
use entities::api_req_params::Pagination;

pub fn to_uri(uri: String) -> Option<Url> {
    Url::parse(uri.as_str()).ok()
}

pub fn get_mime(url: Url) -> Option<Mime> {
    mime_guess::from_path(Path::new(url.path())).first()
}

pub fn get_mime_type_from_uri(uri: String) -> String {
    let default_mime_type = "image/png".to_string();
    to_uri(uri)
        .and_then(get_mime)
        .map_or(default_mime_type, |m| m.to_string())
}

pub fn file_from_str(str: String) -> File {
    let mime = get_mime_type_from_uri(str.clone());
    File {
        uri: Some(str),
        mime: Some(mime),
        quality: None,
        contexts: None,
    }
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
    selector(expr)
        .ok()
        .filter(|d| !Vec::is_empty(d))
        .as_mut()
        .and_then(|v| v.pop())
}

pub fn v1_content_from_json(asset_data: &model::AssetDataModel) -> Result<Content, StorageError> {
    // todo -> move this to the bg worker for pre processing
    let json_uri = asset_data.metadata_url.clone();
    let metadata = &asset_data.metadata;
    let mut selector_fn = jsonpath_lib::selector(metadata);
    let mut chain_data_selector_fn = jsonpath_lib::selector(&asset_data.chain_data);
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

    let symbol = safe_select(selector, "$.attributes");
    if let Some(symbol) = symbol {
        meta.set_item("attributes", symbol.clone());
    }

    let token_standard = safe_select(chain_data_selector, "$.token_standard");
    if let Some(standard) = token_standard {
        meta.set_item("token_standard", standard.clone());
    }

    // TODO: remove
    // let mut links = HashMap::new();
    // let link_fields = vec!["image", "animation_url", "external_url"];
    // for f in link_fields {
    //     let l = safe_select(selector, format!("$.{}", f).as_str());
    //     if let Some(l) = l {
    //         links.insert(f.to_string(), l.to_owned());
    //     }
    // }

    // let _metadata = safe_select(selector, "description");
    // let mut actual_files: HashMap<String, File> = HashMap::new();
    // if let Some(files) = selector("$.properties.files[*]")
    //     .ok()
    //     .filter(|d| !Vec::is_empty(d))
    // {
    //     for v in files.iter() {
    //         if v.is_object() {
    //             // Some assets don't follow the standard and specifiy 'url' instead of 'uri'
    //             let mut uri = v.get("uri");
    //             if uri.is_none() {
    //                 uri = v.get("url");
    //             }
    //             let mime_type = v.get("type");

    //             match (uri, mime_type) {
    //                 (Some(u), Some(m)) => {
    //                     if let Some(str_uri) = u.as_str() {
    //                         let file = if let Some(str_mime) = m.as_str() {
    //                             File {
    //                                 uri: Some(str_uri.to_string()),
    //                                 mime: Some(str_mime.to_string()),
    //                                 quality: None,
    //                                 contexts: None,
    //                             }
    //                         } else {
    //                             warn!("Mime is not string: {:?}", m);
    //                             file_from_str(str_uri.to_string())
    //                         };
    //                         actual_files.insert(str_uri.to_string().clone(), file);
    //                     } else {
    //                         warn!("URI is not string: {:?}", u);
    //                     }
    //                 }
    //                 (Some(u), None) => {
    //                     let str_uri = serde_json::to_string(u).unwrap_or_else(|_| String::new());
    //                     actual_files.insert(str_uri.clone(), file_from_str(str_uri));
    //                 }
    //                 _ => {}
    //             }
    //         } else if v.is_string() {
    //             let str_uri = v.as_str().unwrap().to_string();
    //             actual_files.insert(str_uri.clone(), file_from_str(str_uri));
    //         }
    //     }
    // }

    // track_top_level_file(&mut actual_files, links.get("image"));
    // track_top_level_file(&mut actual_files, links.get("animation_url"));

    // let mut files: Vec<File> = actual_files.into_values().collect();

    let (links, mut files) = parse_files_from_selector(selector);

    // List the defined image file before the other files (if one exists).
    files.sort_by(|a, _: &File| match (a.uri.as_ref(), links.get("image")) {
        (Some(x), Some(y)) => {
            if x == y {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
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
    selector: &mut impl FnMut(&str) -> Result<Vec<&'a Value>, JsonPathError>
) -> (HashMap<String,Value>, Vec<File>) {
    let mut links = HashMap::new();
    let link_fields = vec!["image", "animation_url", "external_url"];
    for f in link_fields {
        let l = safe_select(selector, format!("$.{}", f).as_str());
        if let Some(l) = l {
            links.insert(f.to_string(), l.to_owned());
        }
    }

    let _metadata = safe_select(selector, "description");
    let mut actual_files: HashMap<String, File> = HashMap::new();
    if let Some(files) = selector("$.properties.files[*]")
        .ok()
        .filter(|d| !Vec::is_empty(d))
    {
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
                                    mime: Some(str_mime.to_string()),
                                    quality: None,
                                    contexts: None,
                                }
                            } else {
                                warn!("Mime is not string: {:?}", m);
                                file_from_str(str_uri.to_string())
                            };
                            actual_files.insert(str_uri.to_string().clone(), file);
                        } else {
                            warn!("URI is not string: {:?}", u);
                        }
                    }
                    (Some(u), None) => {
                        let str_uri = serde_json::to_string(u).unwrap_or_else(|_| String::new());
                        actual_files.insert(str_uri.clone(), file_from_str(str_uri));
                    }
                    _ => {}
                }
            } else if v.is_string() {
                let str_uri = v.as_str().unwrap().to_string();
                actual_files.insert(str_uri.clone(), file_from_str(str_uri));
            }
        }
    }

    track_top_level_file(&mut actual_files, links.get("image"));
    track_top_level_file(&mut actual_files, links.get("animation_url"));

    let mut files: Vec<File> = actual_files.into_values().collect();
    (links, files)
}

pub fn parse_files(metadata: &str) -> Option<Vec<File>> {
    serde_json::from_str(metadata)
        .map(|metadata_json| {
            let mut selector_fn = jsonpath_lib::selector(&metadata_json);
            let (_, files) = parse_files_from_selector(&mut selector_fn);
            files
        }).ok()
}

pub fn get_content(
    asset: &model::AssetModel,
    data: &model::AssetDataModel,
) -> Result<Content, StorageError> {
    match asset.specification_version {
        Some(model::SpecificationVersions::V1) | Some(model::SpecificationVersions::V0) => {
            v1_content_from_json(data)
        }
        Some(_) => Err(StorageError::Common("Version Not Implemented".to_string())),
        None => Err(StorageError::Common(
            "Specification version not found".to_string(),
        )),
    }
}

pub fn to_authority(authority: Vec<model::AssetAuthorityModel>) -> Vec<Authority> {
    authority
        .iter()
        .map(|a| Authority {
            address: bs58::encode(&a.authority).into_string(),
            scopes: vec![Scope::Full],
        })
        .collect()
}

pub fn to_creators(creators: Vec<model::AssetCreatorsModel>) -> Vec<Creator> {
    creators
        .iter()
        .map(|a| Creator {
            address: bs58::encode(&a.creator).into_string(),
            share: a.share,
            verified: a.verified,
        })
        .collect()
}

pub fn to_grouping(groups: Vec<model::AssetGroupingModel>) -> Result<Vec<Group>, StorageError> {
    fn find_group(model: &model::AssetGroupingModel) -> Result<Group, StorageError> {
        Ok(Group {
            group_key: model.group_key.clone(),
            group_value: model.group_value.clone(),
            verified: model.verified,
        })
    }

    groups.iter().map(find_group).collect()
}

pub fn get_interface(asset: &model::AssetModel) -> Result<Interface, StorageError> {
    Ok(Interface::from((
        asset
            .specification_version
            .as_ref()
            .ok_or(StorageError::Common(
                "Specification version not found".to_string(),
            ))?,
        asset
            .specification_asset_class
            .as_ref()
            .unwrap_or(&model::SpecificationAssetClass::Unknown),
    )))
}

//TODO -> impl custom error type
pub fn asset_to_rpc(asset: FullAsset) -> Result<Option<RpcAsset>, StorageError> {
    let FullAsset {
        asset,
        data,
        authorities,
        creators,
        groups,
        edition_data,
    } = asset;
    let rpc_authorities = to_authority(authorities);
    let rpc_creators = to_creators(creators);
    let rpc_groups = to_grouping(groups)?;
    let interface = get_interface(&asset)?;

    let mut owner = asset
        .owner
        .clone()
        .map(|o| bs58::encode(o).into_string())
        .unwrap_or("".to_string());
    let mut grouping = Some(rpc_groups);
    let mut frozen = asset.frozen;

    match interface {
        Interface::FungibleAsset | Interface::FungibleToken => {
            owner = "".to_string();
            grouping = Some(vec![]);
            frozen = false;
        }
        _ => {}
    }

    let content = get_content(&asset, &data.asset)?;
    let mut chain_data_selector_fn = jsonpath_lib::selector(&data.asset.chain_data);
    let chain_data_selector = &mut chain_data_selector_fn;
    let basis_points = safe_select(chain_data_selector, "$.primary_sale_happened")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let edition_nonce =
        safe_select(chain_data_selector, "$.edition_nonce").and_then(|v| v.as_u64());
    let mpl_core_info = match interface {
        Interface::MplCoreAsset | Interface::MplCoreCollection => Some(MplCoreInfo {
            num_minted: asset.num_minted,
            current_size: asset.current_supply,
            plugins_json_version: asset.plugins_json_version,
        }),
        _ => None,
    };

    Ok(Some(RpcAsset {
        interface: interface.clone(),
        id: bs58::encode(asset.id).into_string(),
        content: Some(content),
        authorities: Some(rpc_authorities),
        mutable: data.asset.chain_data_mutability.into(),
        compression: Some(Compression {
            eligible: asset.compressible,
            compressed: asset.compressed,
            leaf_id: asset.nonce.unwrap_or(0_i64),
            seq: asset.seq.unwrap_or(0_i64),
            tree: asset
                .tree_id
                .map(|s| bs58::encode(s).into_string())
                .unwrap_or_default(),
            asset_hash: asset
                .leaf
                .map(|s| bs58::encode(s).into_string())
                .unwrap_or_default(),
            data_hash: asset
                .data_hash
                .map(|e| e.trim().to_string())
                .unwrap_or_default(),
            creator_hash: asset
                .creator_hash
                .map(|e| e.trim().to_string())
                .unwrap_or_default(),
        }),
        grouping,
        royalty: Some(Royalty {
            royalty_model: asset.royalty_target_type.into(),
            target: asset.royalty_target.map(|s| bs58::encode(s).into_string()),
            percent: (asset.royalty_amount as f64) * 0.0001,
            basis_points: asset.royalty_amount as u32,
            primary_sale_happened: basis_points,
            locked: false,
        }),
        creators: Some(rpc_creators),
        ownership: Ownership {
            frozen,
            delegated: asset.delegate.is_some(),
            delegate: asset.delegate.map(|s| bs58::encode(s).into_string()),
            ownership_model: asset.owner_type.into(),
            owner,
        },
        supply: match interface {
            Interface::V1NFT => edition_data.map(|e| Supply {
                edition_nonce,
                print_current_supply: e.supply,
                print_max_supply: e.max_supply,
                edition_number: e.edition_number,
            }),
            _ => None,
        },
        uses: data.asset.chain_data.get("uses").map(|u| Uses {
            use_method: u
                .get("use_method")
                .and_then(|s| s.as_str())
                .unwrap_or("Single")
                .to_string()
                .into(),
            total: u.get("total").and_then(|t| t.as_u64()).unwrap_or(0),
            remaining: u.get("remaining").and_then(|t| t.as_u64()).unwrap_or(0),
        }),
        burnt: asset.burnt,
        lamports: data.lamports,
        executable: data.executable,
        metadata_owner: data.metadata_owner,
        rent_epoch: data.rent_epoch,
        plugins: asset.plugins,
        unknown_plugins: asset.unknown_plugins,
        mpl_core_info,
        external_plugins: asset.external_plugins,
        unknown_external_plugins: asset.unknown_external_plugins,
    }))
}

pub fn build_transaction_signatures_response(
    signatures: AssetSignatureWithPagination,
    limit: u64,
    page: Option<u64>,
) -> TransactionSignatureList {
    let items = signatures
        .asset_signatures
        .into_iter()
        .map(|sig| sig.into())
        .collect::<Vec<_>>();
    TransactionSignatureList {
        total: items.len() as u32,
        limit: limit as u32,
        page: page.map(|x| x as u32),
        before: signatures.before.map(|before| before.to_string()),
        after: signatures.after.map(|after| after.to_string()),
        items,
    }
}

pub fn asset_list_to_rpc(asset_list: Vec<FullAsset>) -> (Vec<RpcAsset>, Vec<AssetError>) {
    asset_list
        .into_iter()
        .fold((vec![], vec![]), |(mut assets, errors), asset| {
            match asset_to_rpc(asset.clone()) {
                Ok(rpc_asset) => assets.push(rpc_asset.unwrap()),
                Err(e) => {
                    error!(
                        "Could not cast asset to asset rpc type. Key: {:?}. Error: {:?}",
                        bs58::encode(asset.asset.id).into_string(),
                        e.to_string()
                    )
                }
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

fn get_pagination_values(
    token_accounts: &[TokenAccResponse],
    page: &Option<u64>,
    cursor_enabled: bool,
) -> Result<Pagination, String> {
    if cursor_enabled {
        if let Some(token_acc) = token_accounts.last() {
            Ok(Pagination {
                cursor: Some(token_acc.sorting_id.clone()),
                ..Default::default()
            })
        } else {
            Ok(Pagination::default())
        }
    } else if let Some(p) = page {
        Ok(Pagination {
            page: Some(*p as u32),
            ..Default::default()
        })
    } else {
        let first_row = token_accounts
            .first()
            .map(|token_acc| token_acc.sorting_id.clone());

        let last_row = token_accounts
            .last()
            .map(|token_acc| token_acc.sorting_id.clone());

        Ok(Pagination {
            after: last_row,
            before: first_row,
            ..Default::default()
        })
    }
}
