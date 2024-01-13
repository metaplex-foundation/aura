use entities::models::Updated;
use nft_ingester::config::IngesterConfig;
use nft_ingester::db_v2::DBClient;
use rocks_db::column::TypedColumn;
use rocks_db::Storage;
use rocks_db_v0::Storage as StorageV0;
use solana_sdk::pubkey::Pubkey;
use sqlx::{QueryBuilder, Row};
use std::collections::HashMap;
use std::sync::Arc;

pub async fn migrate_cnft(
    rocks_db_st: Arc<Storage>,
    database_pool: DBClient,
    config: &IngesterConfig,
) {
    let storage_v0 = StorageV0::open(
        &config
            .rocks_db_v0_path
            .clone()
            .unwrap_or("./my_rocksdb_v0".to_string()),
    )
    .unwrap();

    let rocks_db_st_v0 = Arc::new(storage_v0);

    let batch_to_migrate = 1000;

    let mut assets_to_migrate: HashMap<Pubkey, AllAssetInfo> = HashMap::new();

    let mut asset_static_details = rocks_db_st_v0.asset_static_data.iter_start();

    while let Some(res) = asset_static_details.next() {
        match res {
            Ok((key, value)) => {
                let static_info: rocks_db::AssetStaticDetails =
                    bincode::deserialize(&value).unwrap();

                let pubkey = Pubkey::new(&key);

                let mut max_asset_slot = 0;

                let old_dynamic_info: rocks_db_v0::AssetDynamicDetails = rocks_db_st_v0
                    .asset_dynamic_data
                    .get(pubkey.clone())
                    .unwrap()
                    .unwrap();
                let old_authority_info: rocks_db_v0::AssetAuthority = rocks_db_st_v0
                    .asset_authority_data
                    .get(pubkey.clone())
                    .unwrap()
                    .unwrap();
                let old_collection_info: rocks_db_v0::asset::AssetCollection = rocks_db_st_v0
                    .asset_collection_data
                    .get(pubkey.clone())
                    .unwrap()
                    .unwrap();
                let old_owner_info: rocks_db_v0::AssetOwner = rocks_db_st_v0
                    .asset_owner_data
                    .get(pubkey.clone())
                    .unwrap()
                    .unwrap();
                let old_leaf_info: rocks_db_v0::asset::AssetLeaf = rocks_db_st_v0
                    .asset_leaf_data
                    .get(pubkey.clone())
                    .unwrap()
                    .unwrap();

                let supply = {
                    if let Some(s) = old_dynamic_info.supply {
                        Some(Updated {
                            slot_updated: old_dynamic_info.slot_updated,
                            seq: old_dynamic_info.seq,
                            value: s,
                        })
                    } else {
                        None
                    }
                };

                let seq = {
                    if let Some(s) = old_dynamic_info.seq {
                        Some(Updated {
                            slot_updated: old_dynamic_info.slot_updated,
                            seq: old_dynamic_info.seq,
                            value: s,
                        })
                    } else {
                        None
                    }
                };

                let onchain_data = {
                    if let Some(s) = old_dynamic_info.onchain_data {
                        Some(Updated {
                            slot_updated: old_dynamic_info.slot_updated,
                            seq: old_dynamic_info.seq,
                            value: s,
                        })
                    } else {
                        None
                    }
                };

                if old_dynamic_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_dynamic_info.slot_updated;
                }

                let dynamic_info = rocks_db::AssetDynamicDetails {
                    pubkey: pubkey.clone(),
                    is_compressible: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.is_compressible,
                    },
                    is_compressed: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.is_compressed,
                    },
                    is_frozen: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.is_frozen,
                    },
                    supply,
                    seq,
                    is_burnt: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.is_burnt,
                    },
                    was_decompressed: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.was_decompressed,
                    },
                    onchain_data,
                    creators: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.creators,
                    },
                    royalty_amount: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: old_dynamic_info.royalty_amount,
                    },
                    url: Updated {
                        slot_updated: old_dynamic_info.slot_updated,
                        seq: old_dynamic_info.seq,
                        value: "".to_string(),
                    },
                };

                if old_dynamic_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_dynamic_info.slot_updated;
                }

                let authority_info = rocks_db::AssetAuthority {
                    pubkey: pubkey.clone(),
                    authority: old_authority_info.authority,
                    slot_updated: old_authority_info.slot_updated,
                };

                if old_authority_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_authority_info.slot_updated;
                }

                let collection_info = rocks_db::asset::AssetCollection {
                    pubkey: pubkey.clone(),
                    collection: old_collection_info.collection,
                    is_collection_verified: old_collection_info.is_collection_verified,
                    collection_seq: old_collection_info.collection_seq,
                    slot_updated: old_collection_info.slot_updated,
                };

                if old_collection_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_collection_info.slot_updated;
                }

                let delegate = {
                    if let Some(s) = old_owner_info.delegate {
                        Some(Updated {
                            slot_updated: old_owner_info.slot_updated,
                            seq: old_owner_info.owner_delegate_seq,
                            value: s,
                        })
                    } else {
                        None
                    }
                };

                let owner_delegate_seq = {
                    if let Some(s) = old_owner_info.owner_delegate_seq {
                        Some(Updated {
                            slot_updated: old_owner_info.slot_updated,
                            seq: old_owner_info.owner_delegate_seq,
                            value: s,
                        })
                    } else {
                        None
                    }
                };

                let owner_info = rocks_db::AssetOwner {
                    pubkey: pubkey.clone(),
                    owner: Updated {
                        slot_updated: old_owner_info.slot_updated,
                        seq: old_owner_info.owner_delegate_seq,
                        value: old_owner_info.owner,
                    },
                    delegate,
                    owner_type: Updated {
                        slot_updated: old_owner_info.slot_updated,
                        seq: old_owner_info.owner_delegate_seq,
                        value: old_owner_info.owner_type,
                    },
                    owner_delegate_seq,
                };

                if old_owner_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_owner_info.slot_updated;
                }

                let leaf_info = rocks_db::asset::AssetLeaf {
                    pubkey: pubkey.clone(),
                    tree_id: old_leaf_info.tree_id,
                    leaf: old_leaf_info.leaf,
                    nonce: old_leaf_info.nonce,
                    data_hash: old_leaf_info.data_hash,
                    creator_hash: old_leaf_info.creator_hash,
                    leaf_seq: old_leaf_info.leaf_seq,
                    slot_updated: old_leaf_info.slot_updated,
                };

                if old_leaf_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_leaf_info.slot_updated;
                }

                let offchain_data = rocks_db::offchain_data::OffChainData {
                    url: "".to_string(),
                    metadata: "".to_string(),
                };

                let all_asset_info = AllAssetInfo {
                    static_info,
                    dynamic_info,
                    authority_info,
                    collection_info,
                    owner_info,
                    leaf_info,
                    offchain_data,
                    max_asset_slot,
                };

                assets_to_migrate.insert(pubkey, all_asset_info);

                if assets_to_migrate.len() >= batch_to_migrate {
                    drain_batch(&mut assets_to_migrate, &database_pool, rocks_db_st.clone()).await;
                }
            }
            Err(e) => {
                println!("Error while iterating over asset static data: {}", e);

                // TODO: test if this really return error when there is no more data
                drain_batch(&mut assets_to_migrate, &database_pool, rocks_db_st.clone()).await;

                break;
            }
        }
    }

    let mut cl_items = rocks_db_st_v0.cl_items.iter_start();

    while let Some(res) = cl_items.next() {
        match res {
            Ok((key, value)) => {
                let cl_item: rocks_db::cl_items::ClItem = bincode::deserialize(&value).unwrap();

                let k = rocks_db::cl_items::ClItem::decode_key(key.to_vec()).unwrap();

                match rocks_db_st.cl_items.merge(k, &cl_item) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error while merging cl_item: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Error while iterating over cl_items: {}", e);
            }
        }
    }

    let mut cl_leaf = rocks_db_st_v0.cl_leafs.iter_start();

    while let Some(res) = cl_leaf.next() {
        match res {
            Ok((key, value)) => {
                let cl_leaf: rocks_db::cl_items::ClLeaf = bincode::deserialize(&value).unwrap();

                let k = rocks_db::cl_items::ClLeaf::decode_key(key.to_vec()).unwrap();

                match rocks_db_st.cl_leafs.merge(k, &cl_leaf) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error while merging cl_leaf: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Error while iterating over cl_leafs: {}", e);
            }
        }
    }
}

struct AllAssetInfo {
    pub static_info: rocks_db::AssetStaticDetails,
    pub dynamic_info: rocks_db::AssetDynamicDetails,
    pub authority_info: rocks_db::AssetAuthority,
    pub collection_info: rocks_db::asset::AssetCollection,
    pub owner_info: rocks_db::AssetOwner,
    pub leaf_info: rocks_db::asset::AssetLeaf,
    pub offchain_data: rocks_db::offchain_data::OffChainData,
    pub max_asset_slot: u64,
}

async fn drain_batch(
    assets_to_migrate: &mut HashMap<Pubkey, AllAssetInfo>,
    database_pool: &DBClient,
    rocks_db_st: Arc<Storage>,
) {
    let mut query = QueryBuilder::new(
        "select pbk_key, ofd_metadata_url, ofd_metadata from offchain_data inner join pubkeys on offchain_data.ofd_pubkey = pubkeys.pbk_id where pbk_key in (".to_string(),
    );

    for (i, (k, _)) in assets_to_migrate.iter().enumerate() {
        query.push_bind(k.to_bytes());
        if i != assets_to_migrate.len() - 1 {
            query.push(",");
        }
    }

    query.push(")");

    let query = query.build();

    let rows = query.fetch_all(&database_pool.pool).await.unwrap();

    for r in rows.iter() {
        let pubkey: Pubkey = Pubkey::new(r.get("pbk_key"));
        let metadata_url: String = r.get("ofd_metadata_url");
        let metadata: String = r.get("ofd_metadata");

        let asset_info = assets_to_migrate.get_mut(&pubkey).unwrap();
        asset_info.dynamic_info.url.value = metadata_url.clone();
        asset_info.offchain_data.url = metadata_url;
        // in Postgre we marked processing metadata as "processing"
        if metadata != "processing" {
            asset_info.offchain_data.metadata = metadata;
        }
    }

    for (k, v) in assets_to_migrate.iter() {
        match rocks_db_st.asset_updated(v.max_asset_slot, k.clone()) {
            Ok(_) => {}
            Err(e) => {
                println!("Error while updating asset: {}", e);
            }
        }

        match rocks_db_st.asset_static_data.merge(*k, &v.static_info) {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset static data: {}", e);
            }
        }
        match rocks_db_st.asset_dynamic_data.merge(*k, &v.dynamic_info) {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset dynamic data: {}", e);
            }
        }
        match rocks_db_st
            .asset_authority_data
            .merge(*k, &v.authority_info)
        {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset authority data: {}", e);
            }
        }
        match rocks_db_st
            .asset_collection_data
            .merge(*k, &v.collection_info)
        {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset collection data: {}", e);
            }
        }
        match rocks_db_st.asset_owner_data.merge(*k, &v.owner_info) {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset owner data: {}", e);
            }
        }
        match rocks_db_st.asset_leaf_data.merge(*k, &v.leaf_info) {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset leaf data: {}", e);
            }
        }
        match rocks_db_st
            .asset_offchain_data
            .put(v.offchain_data.url.clone(), &v.offchain_data)
        {
            Ok(_) => {}
            Err(e) => {
                println!("Error while merging asset offchain data: {}", e);
            }
        }
    }
    assets_to_migrate.clear();
}
