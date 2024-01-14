use entities::models::Updated;
use log::{error, info};
use metrics_utils::{CnftMigratorMetricsConfig, MetricStatus};
use nft_ingester::config::IngesterConfig;
use nft_ingester::db_v2::DBClient;
use nft_ingester::index_syncronizer::Synchronizer;
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rocks_db::column::TypedColumn;
use rocks_db::Storage;
use rocks_db_v0::Storage as StorageV0;
use solana_sdk::pubkey::Pubkey;
use sqlx::{QueryBuilder, Row};
use std::collections::HashMap;
use std::sync::Arc;

pub async fn migrate_cnft(
    rocks_db_st: Arc<Storage>,
    pg_client: Arc<PgClient>,
    database_pool: DBClient,
    config: &IngesterConfig,
    metrics: Arc<CnftMigratorMetricsConfig>,
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

    let asset_static_details = rocks_db_st_v0.asset_static_data.iter_start();

    for res in asset_static_details {
        match res {
            Ok((key, value)) => {
                let static_info_res: bincode::Result<rocks_db::AssetStaticDetails> =
                    bincode::deserialize(&value);

                match static_info_res {
                    Ok(static_info) => {
                        let pubkey = Pubkey::new(&key);

                        let mut max_asset_slot = 0u64;

                        if static_info.created_at as u64 > max_asset_slot {
                            max_asset_slot = static_info.created_at as u64;
                        }

                        let dynamic_info: Option<rocks_db::AssetDynamicDetails> = {
                            match rocks_db_st_v0.asset_dynamic_data.get(pubkey.clone()) {
                                Ok(data) => {
                                    metrics.inc_getting_data_from_v0(
                                        "dynamic_data",
                                        MetricStatus::SUCCESS,
                                    );

                                    if let Some(old_dynamic_info) = data {
                                        if old_dynamic_info.slot_updated > max_asset_slot {
                                            max_asset_slot = old_dynamic_info.slot_updated;
                                        }

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

                                        Some(rocks_db::AssetDynamicDetails {
                                            pubkey,
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
                                                slot_updated: 0,
                                                seq: old_dynamic_info.seq,
                                                value: "".to_string(),
                                            },
                                        })
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => {
                                    metrics.inc_getting_data_from_v0(
                                        "dynamic_data",
                                        MetricStatus::FAILURE,
                                    );
                                    None
                                }
                            }
                        };

                        let authority_info: Option<rocks_db::AssetAuthority> = {
                            match rocks_db_st_v0.asset_authority_data.get(pubkey) {
                                Ok(data) => {
                                    metrics.inc_getting_data_from_v0(
                                        "authority_data",
                                        MetricStatus::SUCCESS,
                                    );

                                    if let Some(old_authority_info) = data {
                                        if old_authority_info.slot_updated > max_asset_slot {
                                            max_asset_slot = old_authority_info.slot_updated;
                                        }

                                        Some(rocks_db::AssetAuthority {
                                            pubkey,
                                            authority: old_authority_info.authority,
                                            slot_updated: old_authority_info.slot_updated,
                                        })
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => {
                                    metrics.inc_getting_data_from_v0(
                                        "authority_data",
                                        MetricStatus::FAILURE,
                                    );
                                    None
                                }
                            }
                        };

                        let collection_info: Option<rocks_db::asset::AssetCollection> = {
                            match rocks_db_st_v0.asset_collection_data.get(pubkey) {
                                Ok(data) => {
                                    metrics.inc_getting_data_from_v0(
                                        "collection_data",
                                        MetricStatus::SUCCESS,
                                    );

                                    if let Some(old_collection_info) = data {
                                        if old_collection_info.slot_updated > max_asset_slot {
                                            max_asset_slot = old_collection_info.slot_updated;
                                        }

                                        Some(rocks_db::asset::AssetCollection {
                                            pubkey,
                                            collection: old_collection_info.collection,
                                            is_collection_verified: old_collection_info
                                                .is_collection_verified,
                                            collection_seq: old_collection_info.collection_seq,
                                            slot_updated: old_collection_info.slot_updated,
                                        })
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => {
                                    metrics.inc_getting_data_from_v0(
                                        "collection_data",
                                        MetricStatus::FAILURE,
                                    );
                                    None
                                }
                            }
                        };

                        let owner_info: Option<rocks_db::AssetOwner> = {
                            match rocks_db_st_v0.asset_owner_data.get(pubkey) {
                                Ok(data) => {
                                    metrics.inc_getting_data_from_v0(
                                        "owner_data",
                                        MetricStatus::SUCCESS,
                                    );

                                    if let Some(old_owner_info) = data {
                                        if old_owner_info.slot_updated > max_asset_slot {
                                            max_asset_slot = old_owner_info.slot_updated;
                                        }

                                        let delegate = old_owner_info.delegate.map(|s| Updated {
                                            slot_updated: old_owner_info.slot_updated,
                                            seq: old_owner_info.owner_delegate_seq,
                                            value: s,
                                        });

                                        let owner_delegate_seq =
                                            old_owner_info.owner_delegate_seq.map(|s| Updated {
                                                slot_updated: old_owner_info.slot_updated,
                                                seq: old_owner_info.owner_delegate_seq,
                                                value: s,
                                            });

                                        Some(rocks_db::AssetOwner {
                                            pubkey,
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
                                        })
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => {
                                    metrics.inc_getting_data_from_v0(
                                        "owner_data",
                                        MetricStatus::FAILURE,
                                    );
                                    None
                                }
                            }
                        };

                        let leaf_info: Option<rocks_db::asset::AssetLeaf> = {
                            match rocks_db_st_v0.asset_leaf_data.get(pubkey) {
                                Ok(data) => {
                                    metrics.inc_getting_data_from_v0(
                                        "leaf_data",
                                        MetricStatus::SUCCESS,
                                    );

                                    if let Some(old_leaf_info) = data {
                                        if old_leaf_info.slot_updated > max_asset_slot {
                                            max_asset_slot = old_leaf_info.slot_updated;
                                        }

                                        Some(rocks_db::asset::AssetLeaf {
                                            pubkey,
                                            tree_id: old_leaf_info.tree_id,
                                            leaf: old_leaf_info.leaf,
                                            nonce: old_leaf_info.nonce,
                                            data_hash: old_leaf_info.data_hash,
                                            creator_hash: old_leaf_info.creator_hash,
                                            leaf_seq: old_leaf_info.leaf_seq,
                                            slot_updated: old_leaf_info.slot_updated,
                                        })
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => {
                                    metrics.inc_getting_data_from_v0(
                                        "leaf_data",
                                        MetricStatus::FAILURE,
                                    );
                                    None
                                }
                            }
                        };

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
                            drain_batch(
                                &mut assets_to_migrate,
                                &database_pool,
                                rocks_db_st.clone(),
                                pg_client.clone(),
                                metrics.clone(),
                            )
                            .await;
                        }
                    }
                    Err(_) => {
                        metrics.inc_getting_data_from_v0(
                            "dynamic_data_deserialise",
                            MetricStatus::FAILURE,
                        );
                    }
                }
            }
            Err(_) => {
                metrics.inc_getting_data_from_v0("dynamic_data_iterating", MetricStatus::FAILURE);

                // TODO: test if this really return error when there is no more data
                drain_batch(
                    &mut assets_to_migrate,
                    &database_pool,
                    rocks_db_st.clone(),
                    pg_client.clone(),
                    metrics.clone(),
                )
                .await;

                break;
            }
        }
    }
    drain_batch(
        &mut assets_to_migrate,
        &database_pool,
        rocks_db_st.clone(),
        pg_client.clone(),
        metrics.clone(),
    )
    .await;

    let cl_items = rocks_db_st_v0.cl_items.iter_start();

    for res in cl_items {
        match res {
            Ok((key, value)) => {
                let cl_item: rocks_db::cl_items::ClItem = bincode::deserialize(&value).unwrap();

                let k = rocks_db::cl_items::ClItem::decode_key(key.to_vec()).unwrap();

                match rocks_db_st.cl_items.merge(k, &cl_item) {
                    Ok(_) => {
                        metrics.inc_merging_data_to_v1("cl_item", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics.inc_merging_data_to_v1("cl_item", MetricStatus::FAILURE);
                    }
                }
            }
            Err(_) => {
                metrics.inc_getting_data_from_v0("cl_items_iterating", MetricStatus::FAILURE);
            }
        }
    }

    let cl_leaf = rocks_db_st_v0.cl_leafs.iter_start();

    for res in cl_leaf {
        match res {
            Ok((key, value)) => {
                let cl_leaf: rocks_db::cl_items::ClLeaf = bincode::deserialize(&value).unwrap();

                let k = rocks_db::cl_items::ClLeaf::decode_key(key.to_vec()).unwrap();

                match rocks_db_st.cl_leafs.merge(k, &cl_leaf) {
                    Ok(_) => {
                        metrics.inc_merging_data_to_v1("cl_leaf", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics.inc_merging_data_to_v1("cl_leaf", MetricStatus::FAILURE);
                    }
                }
            }
            Err(_) => {
                metrics.inc_getting_data_from_v0("cl_leaf_iterating", MetricStatus::FAILURE);
            }
        }
    }
}

struct AllAssetInfo {
    pub static_info: rocks_db::AssetStaticDetails,
    pub dynamic_info: Option<rocks_db::AssetDynamicDetails>,
    pub authority_info: Option<rocks_db::AssetAuthority>,
    pub collection_info: Option<rocks_db::asset::AssetCollection>,
    pub owner_info: Option<rocks_db::AssetOwner>,
    pub leaf_info: Option<rocks_db::asset::AssetLeaf>,
    pub offchain_data: rocks_db::offchain_data::OffChainData,
    pub max_asset_slot: u64,
}

async fn drain_batch(
    assets_to_migrate: &mut HashMap<Pubkey, AllAssetInfo>,
    database_pool: &DBClient,
    rocks_db_st: Arc<Storage>,
    pg_client: Arc<PgClient>,
    metrics: Arc<CnftMigratorMetricsConfig>,
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
        let pubkey: [u8; 32] = r.get("pbk_key");
        let pubkey: Pubkey = Pubkey::from(pubkey);
        let metadata_url: String = r.get("ofd_metadata_url");
        let metadata: String = r.get("ofd_metadata");

        let asset_info = assets_to_migrate.get_mut(&pubkey).unwrap();
        if let Some(dynamic_info) = &mut asset_info.dynamic_info {
            dynamic_info.url.value = metadata_url.clone();
        } else {
            metrics.inc_missed_dynamic_data_during_url_setting("dynamic_data_url");
        }

        asset_info.offchain_data.url = metadata_url;
        // in Postgre we marked processing metadata as "processing"
        if metadata != "processing" {
            asset_info.offchain_data.metadata = metadata;
        }
    }

    for (k, v) in assets_to_migrate.iter() {
        match rocks_db_st.asset_static_data.merge(*k, &v.static_info) {
            Ok(_) => {
                metrics.inc_merging_data_to_v1("static_data", MetricStatus::SUCCESS);
            }
            Err(_) => {
                metrics.inc_merging_data_to_v1("static_data", MetricStatus::FAILURE);
            }
        }

        if let Some(dynamic_info) = &v.dynamic_info {
            match rocks_db_st.asset_dynamic_data.merge(*k, &dynamic_info) {
                Ok(_) => {
                    metrics.inc_merging_data_to_v1("dynamic_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics.inc_merging_data_to_v1("dynamic_data", MetricStatus::FAILURE);
                }
            }
        }

        if let Some(authority_info) = &v.authority_info {
            match rocks_db_st.asset_authority_data.merge(*k, &authority_info) {
                Ok(_) => {
                    metrics.inc_merging_data_to_v1("authority_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics.inc_merging_data_to_v1("authority_data", MetricStatus::FAILURE);
                }
            }
        }

        if let Some(collection_info) = &v.collection_info {
            match rocks_db_st
                .asset_collection_data
                .merge(*k, &collection_info)
            {
                Ok(_) => {
                    metrics.inc_merging_data_to_v1("collection_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics.inc_merging_data_to_v1("collection_data", MetricStatus::FAILURE);
                }
            }
        }

        if let Some(owner_info) = &v.owner_info {
            match rocks_db_st.asset_owner_data.merge(*k, &owner_info) {
                Ok(_) => {
                    metrics.inc_merging_data_to_v1("owner_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics.inc_merging_data_to_v1("owner_data", MetricStatus::FAILURE);
                }
            }
        }

        if let Some(leaf_info) = &v.leaf_info {
            match rocks_db_st.asset_leaf_data.merge(*k, &leaf_info) {
                Ok(_) => {
                    metrics.inc_merging_data_to_v1("asset_leaf_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics.inc_merging_data_to_v1("asset_leaf_data", MetricStatus::FAILURE);
                }
            }
        }

        match rocks_db_st
            .asset_offchain_data
            .put(v.offchain_data.url.clone(), &v.offchain_data)
        {
            Ok(_) => {
                metrics.inc_merging_data_to_v1("asset_offchain_data", MetricStatus::SUCCESS);
            }
            Err(_) => {
                metrics.inc_merging_data_to_v1("asset_offchain_data", MetricStatus::FAILURE);
            }
        }
        match rocks_db_st.asset_updated(v.max_asset_slot, *k) {
            Ok(_) => {
                metrics.inc_merging_data_to_v1("asset_updated", MetricStatus::SUCCESS);
            }
            Err(_) => {
                metrics.inc_merging_data_to_v1("asset_updated", MetricStatus::FAILURE);
            }
        }
    }
    let keys: Vec<Pubkey> = assets_to_migrate.iter().map(|(k, _)| *k).collect();

    let last_incl_rocks_key_res = pg_client.fetch_last_synced_id().await;

    match last_incl_rocks_key_res {
        Ok(last_incl_rocks_key) => match last_incl_rocks_key {
            Some(last_incl_rocks_key) => {
                let data_sync_res = Synchronizer::syncronize_batch(
                    rocks_db_st.clone(),
                    pg_client.clone(),
                    keys.as_slice(),
                    last_incl_rocks_key,
                )
                .await;

                match data_sync_res {
                    Ok(_) => {
                        metrics.inc_synchronizer_status(
                            "synchronized",
                            MetricStatus::SUCCESS,
                            assets_to_migrate.len() as u64,
                        );
                    }
                    Err(_) => {
                        metrics.inc_synchronizer_status(
                            "synchronized",
                            MetricStatus::FAILURE,
                            assets_to_migrate.len() as u64,
                        );
                    }
                }
            }
            None => {
                error!("Last synced id is None");
            }
        },
        Err(e) => {
            error!("Error while fetching last synced id: {}", e);
        }
    }

    metrics.inc_number_of_assets_migrated("assets_migrated", assets_to_migrate.len() as u64);

    assets_to_migrate.clear();
}
