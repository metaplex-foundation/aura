use entities::models::Updated;
use log::error;
use metrics_utils::{CnftMigratorMetricsConfig, MetricStatus};
use nft_ingester::config::IngesterConfig;
use nft_ingester::db_v2::DBClient;
use nft_ingester::index_syncronizer::Synchronizer;
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rocks_db::column::TypedColumn;
use rocks_db::{AssetStaticDetails, Storage};
use rocks_db_v0::Storage as StorageV0;
use solana_sdk::pubkey::Pubkey;
use sqlx::{QueryBuilder, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::info;

fn build_dynamic_info(
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> (Option<rocks_db::AssetDynamicDetails>, u64) {
    let mut max_asset_slot = 0u64;
    match rocks_db_st_v0.asset_dynamic_data.get(pubkey.clone()) {
        Ok(data) => {
            metrics.inc_getting_data_from_v0("dynamic_data", MetricStatus::SUCCESS);

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

                (
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
                    }),
                    max_asset_slot,
                )
            } else {
                (None, 0)
            }
        }
        Err(_) => {
            metrics.inc_getting_data_from_v0("dynamic_data", MetricStatus::FAILURE);
            (None, 0)
        }
    }
}

fn build_authority_info(
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> (Option<rocks_db::AssetAuthority>, u64) {
    let mut max_asset_slot = 0u64;
    match rocks_db_st_v0.asset_authority_data.get(pubkey) {
        Ok(data) => {
            metrics.inc_getting_data_from_v0("authority_data", MetricStatus::SUCCESS);

            if let Some(old_authority_info) = data {
                if old_authority_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_authority_info.slot_updated;
                }

                (
                    Some(rocks_db::AssetAuthority {
                        pubkey,
                        authority: old_authority_info.authority,
                        slot_updated: old_authority_info.slot_updated,
                    }),
                    max_asset_slot,
                )
            } else {
                (None, 0)
            }
        }
        Err(_) => {
            metrics.inc_getting_data_from_v0("authority_data", MetricStatus::FAILURE);
            (None, 0)
        }
    }
}

fn build_collection_info(
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> (Option<rocks_db::asset::AssetCollection>, u64) {
    let mut max_asset_slot = 0u64;
    match rocks_db_st_v0.asset_collection_data.get(pubkey) {
        Ok(data) => {
            metrics.inc_getting_data_from_v0("collection_data", MetricStatus::SUCCESS);

            if let Some(old_collection_info) = data {
                if old_collection_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_collection_info.slot_updated;
                }

                (
                    Some(rocks_db::asset::AssetCollection {
                        pubkey,
                        collection: old_collection_info.collection,
                        is_collection_verified: old_collection_info.is_collection_verified,
                        collection_seq: old_collection_info.collection_seq,
                        slot_updated: old_collection_info.slot_updated,
                    }),
                    max_asset_slot,
                )
            } else {
                (None, 0)
            }
        }
        Err(_) => {
            metrics.inc_getting_data_from_v0("collection_data", MetricStatus::FAILURE);
            (None, 0)
        }
    }
}

fn build_owner_info(
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> (Option<rocks_db::AssetOwner>, u64) {
    let mut max_asset_slot = 0u64;
    match rocks_db_st_v0.asset_owner_data.get(pubkey) {
        Ok(data) => {
            metrics.inc_getting_data_from_v0("owner_data", MetricStatus::SUCCESS);

            if let Some(old_owner_info) = data {
                if old_owner_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_owner_info.slot_updated;
                }

                let delegate = old_owner_info.delegate.map(|s| Updated {
                    slot_updated: old_owner_info.slot_updated,
                    seq: old_owner_info.owner_delegate_seq,
                    value: s,
                });

                let owner_delegate_seq = old_owner_info.owner_delegate_seq.map(|s| Updated {
                    slot_updated: old_owner_info.slot_updated,
                    seq: old_owner_info.owner_delegate_seq,
                    value: s,
                });

                (
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
                    }),
                    max_asset_slot,
                )
            } else {
                (None, 0)
            }
        }
        Err(_) => {
            metrics.inc_getting_data_from_v0("owner_data", MetricStatus::FAILURE);
            (None, 0)
        }
    }
}

fn build_leaf_info(
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> (Option<rocks_db::asset::AssetLeaf>, u64) {
    let mut max_asset_slot = 0u64;
    match rocks_db_st_v0.asset_leaf_data.get(pubkey) {
        Ok(data) => {
            metrics.inc_getting_data_from_v0("leaf_data", MetricStatus::SUCCESS);

            if let Some(old_leaf_info) = data {
                if old_leaf_info.slot_updated > max_asset_slot {
                    max_asset_slot = old_leaf_info.slot_updated;
                }

                (
                    Some(rocks_db::asset::AssetLeaf {
                        pubkey,
                        tree_id: old_leaf_info.tree_id,
                        leaf: old_leaf_info.leaf,
                        nonce: old_leaf_info.nonce,
                        data_hash: old_leaf_info.data_hash,
                        creator_hash: old_leaf_info.creator_hash,
                        leaf_seq: old_leaf_info.leaf_seq,
                        slot_updated: old_leaf_info.slot_updated,
                    }),
                    max_asset_slot,
                )
            } else {
                (None, 0)
            }
        }
        Err(_) => {
            metrics.inc_getting_data_from_v0("leaf_data", MetricStatus::FAILURE);
            (None, 0)
        }
    }
}

async fn build_all_asset_info(
    static_info: AssetStaticDetails,
    pubkey: Pubkey,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) -> Result<AllAssetInfo, JoinError> {
    let pubkey_clone = pubkey.clone();
    let rocks_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    let dynamic_info_task = tokio::task::spawn_blocking(move || {
        build_dynamic_info(pubkey_clone, rocks_clone, metrics_clone)
    });

    let pubkey_clone = pubkey.clone();
    let rocks_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    let authority_info_task = tokio::task::spawn_blocking(move || {
        build_authority_info(pubkey_clone, rocks_clone, metrics_clone)
    });

    let pubkey_clone = pubkey.clone();
    let rocks_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    let collection_info_task = tokio::task::spawn_blocking(move || {
        build_collection_info(pubkey_clone, rocks_clone, metrics_clone)
    });

    let pubkey_clone = pubkey.clone();
    let rocks_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    let owner_info_task = tokio::task::spawn_blocking(move || {
        build_owner_info(pubkey_clone, rocks_clone, metrics_clone)
    });

    let pubkey_clone = pubkey.clone();
    let rocks_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    let leaf_info_task = tokio::task::spawn_blocking(move || {
        build_leaf_info(pubkey_clone, rocks_clone, metrics_clone)
    });

    let (dynamic_info, max_slot_dynamic) = dynamic_info_task.await?;
    let (authority_info, max_slot_authority) = authority_info_task.await?;
    let (collection_info, max_slot_collection) = collection_info_task.await?;
    let (owner_info, max_slot_owner) = owner_info_task.await?;
    let (leaf_info, max_slot_leaf) = leaf_info_task.await?;

    let mut max_asset_slot = 0u64;
    if static_info.created_at as u64 > max_asset_slot {
        max_asset_slot = static_info.created_at as u64;
    }
    for slot in &[
        max_slot_dynamic,
        max_slot_authority,
        max_slot_collection,
        max_slot_owner,
        max_slot_leaf,
    ] {
        if *slot > max_asset_slot {
            max_asset_slot = *slot;
        }
    }

    let offchain_data = rocks_db::offchain_data::OffChainData {
        url: "".to_string(),
        metadata: "".to_string(),
    };

    Ok(AllAssetInfo {
        static_info,
        dynamic_info,
        authority_info,
        collection_info,
        owner_info,
        leaf_info,
        offchain_data,
        max_asset_slot,
    })
}

async fn migrate_asset_detail(
    rocks_db_st: Arc<Storage>,
    rocks_db_st_v0: Arc<StorageV0>,
    database_pool: DBClient,
    pg_client: Arc<PgClient>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) {
    let batch_to_migrate = 1000;
    let assets_to_migrate: Arc<Mutex<HashMap<Pubkey, AllAssetInfo>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let pubkeys_to_migrate: Arc<Mutex<Vec<Pubkey>>> = Arc::new(Mutex::new(Vec::new()));
    let asset_static_details = rocks_db_st_v0.asset_static_data.iter_start();
    let database_pool = Arc::new(database_pool);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(750)); // there is not blocking tasks
    let mut tasks = JoinSet::new();

    for _ in 0..5 {
        let pg_client_clone = pg_client.clone();
        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let pubkeys_to_migrate_clone = pubkeys_to_migrate.clone();
        tasks.spawn(tokio::spawn(async move {
            synchronize(
                pg_client_clone,
                rocks_clone,
                metrics_clone,
                pubkeys_to_migrate_clone,
            )
            .await
        }));
    }

    for res in asset_static_details {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let assets_to_migrate_clone = assets_to_migrate.clone();
        let pubkeys_to_migrate_clone = pubkeys_to_migrate.clone();
        let rocks_clone = rocks_db_st.clone();
        let rocks_v0_clone = rocks_db_st_v0.clone();
        let metrics_clone = metrics.clone();
        let database_pool_clone = database_pool.clone();

        tasks.spawn(tokio::spawn(async move {
            let _permit = permit;
            match res {
                Ok((key, value)) => {
                    let static_info_res: bincode::Result<rocks_db::AssetStaticDetails> =
                        bincode::deserialize(&value);

                    match static_info_res {
                        Ok(static_info) => {
                            let pubkey = Pubkey::new(&key);
                            let all_asset_info = match build_all_asset_info(
                                static_info,
                                pubkey.clone(),
                                rocks_v0_clone.clone(),
                                metrics_clone.clone(),
                            )
                            .await
                            {
                                Ok(all_asset_info) => all_asset_info,
                                Err(e) => {
                                    error!("Error on build_all_asset_info: {}", e);
                                    return;
                                }
                            };

                            assets_to_migrate_clone
                                .lock()
                                .await
                                .insert(pubkey, all_asset_info);
                            pubkeys_to_migrate_clone.lock().await.push(pubkey);

                            if assets_to_migrate_clone.lock().await.len() >= batch_to_migrate {
                                drain_batch(
                                    assets_to_migrate_clone.clone(),
                                    database_pool_clone.clone(),
                                    rocks_clone.clone(),
                                    metrics_clone.clone(),
                                )
                                .await;
                            }
                        }
                        Err(_) => {
                            metrics_clone.inc_getting_data_from_v0(
                                "dynamic_data_deserialise",
                                MetricStatus::FAILURE,
                            );
                        }
                    }
                }
                Err(_) => {
                    metrics_clone
                        .inc_getting_data_from_v0("dynamic_data_iterating", MetricStatus::FAILURE);

                    drain_batch(
                        assets_to_migrate_clone.clone(),
                        database_pool_clone.clone(),
                        rocks_clone.clone(),
                        metrics_clone.clone(),
                    )
                    .await;

                    return;
                }
            }
        }));
    }

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the migrations assets was finished")
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Migrations assets task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Migrations assets task error: {}", err);
            }
        }
    }

    drain_batch(
        assets_to_migrate.clone(),
        database_pool.clone(),
        rocks_db_st.clone(),
        metrics.clone(),
    )
    .await;
}

async fn migrate_cl_items(
    rocks_db_st: Arc<Storage>,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) {
    let cl_items = rocks_db_st_v0.cl_items.iter_start();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(150)); // by default tokio have 500 workers for spawn_blocking
    let mut tasks = Vec::new();
    for res in cl_items {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match res {
                Ok((key, value)) => {
                    let cl_item: rocks_db::cl_items::ClItem = bincode::deserialize(&value).unwrap();

                    let k = rocks_db::cl_items::ClItem::decode_key(key.to_vec()).unwrap();

                    match rocks_clone.cl_items.merge(k, &cl_item) {
                        Ok(_) => {
                            metrics_clone.inc_merging_data_to_v1("cl_item", MetricStatus::SUCCESS);
                        }
                        Err(_) => {
                            metrics_clone.inc_merging_data_to_v1("cl_item", MetricStatus::FAILURE);
                        }
                    }
                }
                Err(_) => {
                    metrics_clone
                        .inc_getting_data_from_v0("cl_items_iterating", MetricStatus::FAILURE);
                }
            }
        }));
    }

    for task in tasks {
        task.await
            .unwrap_or_else(|e| error!("migrate_cl_items task {}", e));
    }
}

async fn migrate_cl_leaf(
    rocks_db_st: Arc<Storage>,
    rocks_db_st_v0: Arc<StorageV0>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) {
    let cl_leaf = rocks_db_st_v0.cl_leafs.iter_start();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(150)); // by default tokio have 500 workers for spawn_blocking
    let mut tasks = Vec::new();
    for res in cl_leaf {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match res {
                Ok((key, value)) => {
                    let cl_leaf: rocks_db::cl_items::ClLeaf = bincode::deserialize(&value).unwrap();
                    let k = rocks_db::cl_items::ClLeaf::decode_key(key.to_vec()).unwrap();
                    match rocks_clone.cl_leafs.put(k, &cl_leaf) {
                        Ok(_) => {
                            metrics_clone.inc_merging_data_to_v1("cl_leaf", MetricStatus::SUCCESS);
                        }
                        Err(_) => {
                            metrics_clone.inc_merging_data_to_v1("cl_leaf", MetricStatus::FAILURE);
                        }
                    }
                }
                Err(_) => {
                    metrics_clone
                        .inc_getting_data_from_v0("cl_leaf_iterating", MetricStatus::FAILURE);
                }
            }
        }));
    }

    for task in tasks {
        task.await
            .unwrap_or_else(|e| error!("migrate_cl_leaf task {}", e));
    }
}

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
    let mut tasks = JoinSet::new();

    let rocks_clone = rocks_db_st.clone();
    let rocks_v0_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    tasks.spawn(tokio::spawn(async move {
        migrate_asset_detail(
            rocks_clone,
            rocks_v0_clone,
            database_pool,
            pg_client,
            metrics_clone,
        )
        .await;
    }));

    let rocks_clone = rocks_db_st.clone();
    let rocks_v0_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    tasks.spawn(tokio::spawn(async move {
        migrate_cl_items(rocks_clone, rocks_v0_clone, metrics_clone).await;
    }));

    let rocks_clone = rocks_db_st.clone();
    let rocks_v0_clone = rocks_db_st_v0.clone();
    let metrics_clone = metrics.clone();
    tasks.spawn(tokio::spawn(async move {
        migrate_cl_leaf(rocks_clone, rocks_v0_clone, metrics_clone).await;
    }));

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the migrations was finished")
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Migrations task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Migrations task error: {}", err);
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
    assets_to_migrate_mutex: Arc<Mutex<HashMap<Pubkey, AllAssetInfo>>>,
    database_pool: Arc<DBClient>,
    rocks_db_st: Arc<Storage>,
    metrics: Arc<CnftMigratorMetricsConfig>,
) {
    let mut query = QueryBuilder::new(
        "select pbk_key, ofd_metadata_url, ofd_metadata from offchain_data inner join pubkeys on offchain_data.ofd_pubkey = pubkeys.pbk_id where pbk_key in (".to_string(),
    );

    let mut assets_to_migrate: HashMap<Pubkey, AllAssetInfo> = HashMap::new();
    {
        let mut assets = assets_to_migrate_mutex.lock().await;
        assets_to_migrate.extend(assets.drain());
    }

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

    let semaphore = Arc::new(tokio::sync::Semaphore::new(150)); // by default tokio have 500 workers for spawn_blocking
    let mut tasks = Vec::new();
    for (k, v) in assets_to_migrate.iter() {
        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let static_details = v.static_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match rocks_clone.asset_static_data.merge(key, &static_details) {
                Ok(_) => {
                    metrics_clone.inc_merging_data_to_v1("static_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics_clone.inc_merging_data_to_v1("static_data", MetricStatus::FAILURE);
                }
            };
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let dynamic_info = v.dynamic_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            if let Some(dynamic_info) = dynamic_info {
                let _permit = permit;
                match rocks_clone.asset_dynamic_data.merge(key, &dynamic_info) {
                    Ok(_) => {
                        metrics_clone.inc_merging_data_to_v1("dynamic_data", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics_clone.inc_merging_data_to_v1("dynamic_data", MetricStatus::FAILURE);
                    }
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let authority_info = v.authority_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            if let Some(authority_info) = &authority_info {
                let _permit = permit;
                match rocks_clone.asset_authority_data.merge(key, &authority_info) {
                    Ok(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("authority_data", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("authority_data", MetricStatus::FAILURE);
                    }
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let collection_info = v.collection_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            if let Some(collection_info) = &collection_info {
                let _permit = permit;
                match rocks_clone
                    .asset_collection_data
                    .merge(key, &collection_info)
                {
                    Ok(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("collection_data", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("collection_data", MetricStatus::FAILURE);
                    }
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let owner_info = v.owner_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            if let Some(owner_info) = &owner_info {
                let _permit = permit;
                match rocks_clone.asset_owner_data.merge(key, &owner_info) {
                    Ok(_) => {
                        metrics_clone.inc_merging_data_to_v1("owner_data", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics_clone.inc_merging_data_to_v1("owner_data", MetricStatus::FAILURE);
                    }
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let leaf_info = v.leaf_info.to_owned();
        let key = k.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            if let Some(leaf_info) = &leaf_info {
                let _permit = permit;
                match rocks_clone.asset_leaf_data.merge(key, &leaf_info) {
                    Ok(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("asset_leaf_data", MetricStatus::SUCCESS);
                    }
                    Err(_) => {
                        metrics_clone
                            .inc_merging_data_to_v1("asset_leaf_data", MetricStatus::FAILURE);
                    }
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let offchain_data = v.offchain_data.to_owned();
        let key = v.offchain_data.url.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match rocks_clone.asset_offchain_data.put(key, &offchain_data) {
                Ok(_) => {
                    metrics_clone
                        .inc_merging_data_to_v1("asset_offchain_data", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics_clone
                        .inc_merging_data_to_v1("asset_offchain_data", MetricStatus::FAILURE);
                }
            }
        }));

        let rocks_clone = rocks_db_st.clone();
        let metrics_clone = metrics.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let update = k.to_owned();
        let key = v.max_asset_slot.to_owned();
        tasks.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match rocks_clone.asset_updated(key, update) {
                Ok(_) => {
                    metrics_clone.inc_merging_data_to_v1("asset_updated", MetricStatus::SUCCESS);
                }
                Err(_) => {
                    metrics_clone.inc_merging_data_to_v1("asset_updated", MetricStatus::FAILURE);
                }
            }
        }));
    }

    for task in tasks {
        task.await
            .unwrap_or_else(|e| error!("drain_buffer task {}", e));
    }

    metrics.inc_number_of_assets_migrated("assets_migrated", assets_to_migrate.len() as u64);
}

async fn synchronize(
    pg_client: Arc<PgClient>,
    rocks_db_st: Arc<Storage>,
    metrics: Arc<CnftMigratorMetricsConfig>,
    pubkeys_mutex: Arc<Mutex<Vec<Pubkey>>>,
) {
    let mut keys = Vec::new();
    {
        let mut pubkeys = pubkeys_mutex.lock().await;
        let drain_len = pubkeys.len().min(1000);
        keys.extend(pubkeys.drain(0..drain_len));
    }

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
                            keys.len() as u64,
                        );
                    }
                    Err(_) => {
                        metrics.inc_synchronizer_status(
                            "synchronized",
                            MetricStatus::FAILURE,
                            keys.len() as u64,
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
}
