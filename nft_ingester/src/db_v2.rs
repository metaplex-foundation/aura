use std::collections::{HashMap, HashSet};

use anchor_lang::prelude::Pubkey;
use blockbuster::{
    programs::bubblegum::Payload,
    token_metadata::state::{TokenStandard, UseMethod, Uses},
};
use mpl_bubblegum::state::leaf_schema::{LeafSchema, LeafSchemaEvent};
use num_traits::FromPrimitive;
use spl_account_compression::events::ChangeLogEventV1;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, Postgres},
    ConnectOptions, PgPool, QueryBuilder, Row,
};

use digital_asset_types::json::ChainDataV1;
use rocks_db::columns::Mint;

use crate::config::DatabaseConfig;
use crate::error::IngesterError;

#[derive(Clone)]
pub struct DBClient {
    pub pool: PgPool,
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
)]
#[sqlx(type_name = "royalty_target_type", rename_all = "lowercase")]
pub enum RoyaltyTargetType {
    Unknown,
    Creators,
    Fanout,
    Single,
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
)]
#[sqlx(type_name = "specification_asset_class", rename_all = "lowercase")]
#[allow(non_camel_case_types)]
pub enum SpecificationAssetClass {
    Unknown,
    Fungible_Token,
    Fungible_Asset,
    Nft,
    Printable_Nft,
    Print,
    Transfer_Restricted_Nft,
    Non_Transferable_Nft,
    Identity_Nft,
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
)]
#[sqlx(type_name = "owner_type", rename_all = "lowercase")]
pub enum OwnerType {
    Unknown,
    Token,
    Single,
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
)]
#[sqlx(type_name = "task_status", rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed,
}

pub struct Asset {
    pub ast_pubkey: Vec<u8>,
    pub ast_owner: Option<Vec<u8>>,
    pub ast_delegate: Option<Vec<u8>>,
    pub ast_authority: Option<Vec<u8>>,
    pub ast_collection: Option<Vec<u8>>,
    pub ast_is_collection_verified: bool,
    pub ast_is_compressed: bool,
    pub ast_is_frozen: bool,
    pub ast_supply: Option<i64>,
    pub ast_seq: Option<i64>,
    pub ast_tree_id: Option<Vec<u8>>,
    pub ast_leaf: Option<Vec<u8>>,
    pub ast_nonce: Option<i64>,
    pub ast_royalty_target_type: RoyaltyTargetType,
    pub ast_royalty_target: Option<Vec<u8>>,
    pub ast_royalty_amount: i64,
    pub ast_is_burnt: bool,
    pub ast_slot_updated: i64,
    pub ast_data_hash: Option<String>,
    pub ast_creator_hash: Option<String>,
    pub ast_owner_delegate_seq: Option<i64>,
    pub ast_was_decompressed: bool,
    pub ast_leaf_seq: Option<i64>,
    pub ast_specification_asset_class: SpecificationAssetClass,
    pub ast_owner_type: OwnerType,
    pub ast_onchain_data: String,
    pub ast_supply_slot_updated: Option<i64>,
}

pub struct AssetForInsert<'a> {
    pub ast_pubkey: Option<&'a i64>,
    pub ast_owner: Option<&'a i64>,
    pub ast_delegate: Option<&'a i64>,
    pub ast_authority: Option<&'a i64>,
    pub ast_collection: Option<&'a i64>,
    pub ast_is_collection_verified: bool,
    pub ast_is_compressed: bool,
    pub ast_is_frozen: bool,
    pub ast_supply: Option<i64>,
    pub ast_seq: Option<i64>,
    pub ast_tree_id: Option<&'a i64>,
    pub ast_leaf: Option<Vec<u8>>,
    pub ast_nonce: Option<i64>,
    pub ast_royalty_target_type: RoyaltyTargetType,
    pub ast_royalty_target: Option<&'a i64>,
    pub ast_royalty_amount: i64,
    pub ast_is_burnt: bool,
    pub ast_slot_updated: i64,
    pub ast_data_hash: Option<String>,
    pub ast_creator_hash: Option<String>,
    pub ast_owner_delegate_seq: Option<i64>,
    pub ast_was_decompressed: bool,
    pub ast_leaf_seq: Option<i64>,
    pub ast_specification_asset_class: SpecificationAssetClass,
    pub ast_owner_type: OwnerType,
    pub ast_onchain_data: String,
    pub ast_supply_slot_updated: Option<i64>,
}

pub struct SupplyToUpdate<'a> {
    pub ast_pubkey: &'a i64,
    pub ast_supply: i64,
    pub ast_supply_slot_updated: i64,
}

#[derive(Debug)]
pub struct Task {
    pub ofd_metadata_url: String,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
}

pub struct TaskForInsert {
    pub ofd_metadata_url: i64,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
}

pub struct Creator {
    pub asc_asset: Vec<u8>,
    pub asc_creator: Vec<u8>,
    pub asc_share: i32,
    pub asc_verified: bool,
    pub asc_seq: i32,
    pub asc_slot_updated: i64,
    pub asc_position: i32,
}

#[derive(Debug, Clone)]
pub struct JsonDownloadTask {
    pub metadata_url: String,
    pub status: TaskStatus,
    pub attempts: i16,
    pub max_attempts: i16,
}

pub struct UpdatedTask {
    pub status: TaskStatus,
    pub metadata_url: String,
    pub attempts: i16,
    pub error: String,
}

impl DBClient {
    pub async fn new(config: &DatabaseConfig) -> Result<Self, IngesterError> {
        let max = config.get_max_postgres_connections().unwrap_or(100);

        let url = config.get_database_url()?;

        let mut options: PgConnectOptions =
            url.parse()
                .map_err(|err| IngesterError::ConfigurationError {
                    msg: format!("URL parse: {}", err),
                })?;
        options.log_statements(log::LevelFilter::Trace);

        options.log_slow_statements(
            log::LevelFilter::Debug,
            std::time::Duration::from_millis(500),
        );

        let pool = PgPoolOptions::new()
            .min_connections(100)
            .max_connections(max)
            .connect_with(options)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Connect :{}", err)))?;

        Ok(Self { pool })
    }

    pub async fn update_tasks(&self, data: Vec<UpdatedTask>) -> Result<(), IngesterError> {
        let metadata_id = self
            .insert_metadata(
                &data
                    .iter()
                    .map(|task| task.metadata_url.as_str())
                    .collect::<Vec<_>>(),
            )
            .await?;
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO tasks (tsk_metadata_url, tsk_status, tsk_attempts, tsk_error) ",
        );

        query_builder.push_values(data, |mut b, off_d| {
            b.push_bind(metadata_id.get(&off_d.metadata_url));
            b.push_bind(off_d.status);
            b.push_bind(off_d.attempts);
            b.push_bind(off_d.error);
        });

        query_builder.push(" ON CONFLICT (tsk_id) DO UPDATE SET tsk_status = EXCLUDED.tsk_status, tsk_metadata_url = EXCLUDED.tsk_metadata_url, tsk_attempts = EXCLUDED.tsk_attempts,
                                    tsk_error = EXCLUDED.tsk_error;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Update tasks: {}", err)))?;

        Ok(())
    }

    pub async fn get_pending_tasks(&self) -> Result<Vec<JsonDownloadTask>, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("WITH cte AS (
                                        SELECT tsk_id
                                        FROM tasks
                                        WHERE tsk_status != 'success' AND tsk_locked_until < NOW() AND tsk_attempts < tsk_max_attempts
                                        LIMIT 100
                                        FOR UPDATE
                                    )
                                    UPDATE tasks t
                                    SET tsk_status = 'running',
                                    tsk_locked_until = NOW() + INTERVAL '20 seconds'
                                    FROM cte
                                    WHERE t.tsk_id = cte.tsk_id
                                    RETURNING t.tsk_metadata_url, t.tsk_status, t.tsk_attempts, t.tsk_max_attempts;");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Get pending tasks: {}", err)))?;

        let mut tasks = Vec::new();

        for row in rows {
            let metadata_url: String = row.get("tsk_metadata_url");
            let status: TaskStatus = row.get("tsk_status");
            let attempts: i16 = row.get("tsk_attempts");
            let max_attempts: i16 = row.get("tsk_max_attempts");

            tasks.push(JsonDownloadTask {
                metadata_url,
                status,
                attempts,
                max_attempts,
            });
        }

        Ok(tasks)
    }

    pub async fn insert_pubkeys(&self, keys: &[Vec<u8>]) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO pubkeys (pbk_key) ");

        let mut ordered_keys = keys.to_owned();
        ordered_keys.sort();

        query_builder.push_values(ordered_keys, |mut b, key| {
            b.push_bind(key);
        });

        query_builder.push("ON CONFLICT (pbk_key) DO NOTHING;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert pubkeys: {}", err)))?;

        Ok(())
    }

    pub async fn update_supply(&self, mints: Vec<Mint>) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for key in mints.iter() {
            keys.insert(key.pubkey.to_bytes().to_vec());
        }

        let keys = keys.into_iter().collect::<Vec<Vec<u8>>>();
        let ids_keys = self.get_pubkey_ids(&keys).await?;
        let keys_map = ids_keys.into_iter().collect::<HashMap<Vec<u8>, i64>>();

        let mut supply_to_update = Vec::new();

        for mint in mints.iter() {
            supply_to_update.push(SupplyToUpdate {
                ast_pubkey: keys_map.get(&mint.pubkey.to_bytes().to_vec()).unwrap(),
                ast_supply: mint.supply,
                ast_supply_slot_updated: mint.slot_updated,
            });
        }

        supply_to_update.sort_by(|a, b| a.ast_pubkey.cmp(b.ast_pubkey));

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE assets SET ast_supply = tmp.supply, ast_supply_slot_updated = tmp.supply_slot_updated FROM (");

        query_builder.push_values(supply_to_update, |mut b, key| {
            b.push_bind(key.ast_pubkey);
            b.push_bind(key.ast_supply);
            b.push_bind(key.ast_supply_slot_updated);
        });

        query_builder.push(") as tmp (id, supply, supply_slot_updated) WHERE ast_pubkey = tmp.id AND ast_supply_slot_updated < tmp.supply_slot_updated;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Update supplies: {}", err)))?;

        Ok(())
    }

    pub async fn insert_pubkey(&self, key: &Vec<u8>) -> Result<i64, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO pubkeys (pbk_key)");

        query_builder.push_values(vec![key], |mut b, key| {
            b.push_bind(key);
        });

        query_builder.push("ON CONFLICT (pbk_key) DO NOTHING RETURNING pbk_id;");

        let query = query_builder.build();
        let row = query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert one pubkey: {}", err)))?;

        let id: i64 = row.get("pbk_id");

        Ok(id)
    }

    pub async fn insert_metadata(
        &self,
        urls: &Vec<&str>,
    ) -> Result<HashMap<String, i64>, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO metadata (mtd_url)");

        query_builder.push_values(urls, |mut b, key| {
            b.push_bind(key);
        });

        query_builder.push("ON CONFLICT (mtd_url) DO NOTHING RETURNING mtd_id, mtd_url;");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert one metadata: {}", err)))?;

        let res: HashMap<String, i64> = rows
            .iter()
            .map(|row| (row.get("mtd_url"), row.get("mtd_id")))
            .collect();

        Ok(res)
    }

    pub async fn get_pubkey_ids(
        &self,
        keys: &Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, i64)>, IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("SELECT pbk_id, pbk_key FROM pubkeys WHERE pbk_key IN (");

        for (i, k) in keys.iter().enumerate() {
            query_builder.push_bind(k);
            if i < keys.len() - 1 {
                query_builder.push(",");
            }
        }
        query_builder.push(")");

        let query = query_builder.build();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Get pubkey ids: {}", err)))?;

        Ok(rows
            .iter()
            .map(|r| (r.get("pbk_key"), r.get("pbk_id")))
            .collect())
    }

    pub async fn upsert_assets(&self, data: &Vec<Asset>) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for asset in data {
            keys.insert(asset.ast_pubkey.clone());

            if let Some(owner) = &asset.ast_owner {
                keys.insert(owner.clone());
            }

            if let Some(delegate) = &asset.ast_delegate {
                keys.insert(delegate.clone());
            }

            if let Some(authority) = &asset.ast_authority {
                keys.insert(authority.clone());
            }

            if let Some(collection) = &asset.ast_collection {
                keys.insert(collection.clone());
            }

            if let Some(tree_id) = &asset.ast_tree_id {
                keys.insert(tree_id.clone());
            }

            if let Some(leaf) = &asset.ast_leaf {
                keys.insert(leaf.clone());
            }

            if let Some(royalty_target) = &asset.ast_royalty_target {
                keys.insert(royalty_target.clone());
            }
        }

        let keys = keys.into_iter().collect::<Vec<Vec<u8>>>();
        let ids_keys = self.get_pubkey_ids(&keys).await?;
        let keys_map = ids_keys
            .into_iter()
            .collect::<std::collections::HashMap<Vec<u8>, i64>>();

        let mut assets_to_insert = Vec::new();

        for asset in data.iter() {
            assets_to_insert.push(AssetForInsert {
                ast_pubkey: keys_map.get(&asset.ast_pubkey),
                ast_owner: None,
                ast_delegate: asset
                    .ast_delegate
                    .as_ref()
                    .and_then(|d| keys_map.get(d))
                    .or(None),
                ast_authority: asset
                    .ast_authority
                    .as_ref()
                    .and_then(|a| keys_map.get(a))
                    .or(None),
                ast_collection: asset
                    .ast_collection
                    .as_ref()
                    .and_then(|c| keys_map.get(c))
                    .or(None),
                ast_is_collection_verified: asset.ast_is_collection_verified,
                ast_is_compressed: asset.ast_is_compressed,
                ast_is_frozen: asset.ast_is_frozen,
                ast_supply: asset.ast_supply,
                ast_seq: asset.ast_seq,
                ast_tree_id: asset
                    .ast_tree_id
                    .as_ref()
                    .and_then(|t| keys_map.get(t))
                    .or(None),
                ast_leaf: asset.ast_leaf.clone(),
                ast_nonce: asset.ast_nonce,
                ast_royalty_target_type: asset.ast_royalty_target_type,
                ast_royalty_target: asset
                    .ast_royalty_target
                    .as_ref()
                    .and_then(|r| keys_map.get(r))
                    .or(None),
                ast_royalty_amount: asset.ast_royalty_amount,
                ast_is_burnt: asset.ast_is_burnt,
                ast_slot_updated: asset.ast_slot_updated,
                ast_data_hash: asset.ast_data_hash.clone(),
                ast_creator_hash: asset.ast_creator_hash.clone(),
                ast_owner_delegate_seq: asset.ast_owner_delegate_seq,
                ast_was_decompressed: asset.ast_was_decompressed,
                ast_leaf_seq: asset.ast_leaf_seq,
                ast_specification_asset_class: asset.ast_specification_asset_class,
                ast_owner_type: asset.ast_owner_type,
                ast_onchain_data: asset.ast_onchain_data.clone(),
                ast_supply_slot_updated: asset.ast_supply_slot_updated,
            });
        }

        assets_to_insert.sort_by(|a, b| a.ast_pubkey.cmp(&b.ast_pubkey));

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO assets (
                ast_pubkey,
                ast_owner,
                ast_delegate,
                ast_authority,
                ast_collection,
                ast_is_collection_verified,
                ast_is_compressed,
                ast_is_frozen,
                ast_supply,
                ast_seq,
                ast_tree_id,
                ast_leaf,
                ast_nonce,
                ast_royalty_target_type,
                ast_royalty_target,
                ast_royalty_amount,
                ast_is_burnt,
                ast_slot_updated,
                ast_data_hash,
                ast_creator_hash,
                ast_owner_delegate_seq,
                ast_was_decompressed,
                ast_leaf_seq,
                ast_specification_asset_class,
                ast_owner_type,
                ast_onchain_data,
                ast_supply_slot_updated
            ) ",
        );

        query_builder.push_values(assets_to_insert, |mut b, asset| {
            b.push_bind(asset.ast_pubkey);
            b.push_bind(asset.ast_owner);
            b.push_bind(asset.ast_delegate);
            b.push_bind(asset.ast_authority);
            b.push_bind(asset.ast_collection);
            b.push_bind(asset.ast_is_collection_verified);
            b.push_bind(asset.ast_is_compressed);
            b.push_bind(asset.ast_is_frozen);
            b.push_bind(asset.ast_supply);
            b.push_bind(asset.ast_seq);
            b.push_bind(asset.ast_tree_id);
            b.push_bind(asset.ast_leaf.clone());
            b.push_bind(asset.ast_nonce);
            b.push_bind(asset.ast_royalty_target_type);
            b.push_bind(asset.ast_royalty_target);
            b.push_bind(asset.ast_royalty_amount);
            b.push_bind(asset.ast_is_burnt);
            b.push_bind(asset.ast_slot_updated);
            b.push_bind(asset.ast_data_hash.clone());
            b.push_bind(asset.ast_creator_hash.clone());
            b.push_bind(asset.ast_owner_delegate_seq);
            b.push_bind(asset.ast_was_decompressed);
            b.push_bind(asset.ast_leaf_seq);
            b.push_bind(asset.ast_specification_asset_class);
            b.push_bind(asset.ast_owner_type);
            b.push_bind(asset.ast_onchain_data.clone());
            b.push_bind(asset.ast_supply_slot_updated);
        });

        query_builder.push(
            " ON CONFLICT (ast_pubkey)
                            DO UPDATE SET
                                ast_owner = EXCLUDED.ast_owner,
                                ast_delegate = EXCLUDED.ast_delegate,
                                ast_authority = EXCLUDED.ast_authority,
                                ast_collection = EXCLUDED.ast_collection,
                                ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
                                ast_is_compressed = EXCLUDED.ast_is_compressed,
                                ast_is_frozen = EXCLUDED.ast_is_frozen,
                                ast_supply = EXCLUDED.ast_supply,
                                ast_seq = EXCLUDED.ast_seq,
                                ast_tree_id = EXCLUDED.ast_tree_id,
                                ast_leaf = EXCLUDED.ast_leaf,
                                ast_nonce = EXCLUDED.ast_nonce,
                                ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
                                ast_royalty_target = EXCLUDED.ast_royalty_target,
                                ast_royalty_amount = EXCLUDED.ast_royalty_amount,
                                ast_is_burnt = EXCLUDED.ast_is_burnt,
                                ast_slot_updated = EXCLUDED.ast_slot_updated,
                                ast_data_hash = EXCLUDED.ast_data_hash,
                                ast_creator_hash = EXCLUDED.ast_creator_hash,
                                ast_owner_delegate_seq = EXCLUDED.ast_owner_delegate_seq,
                                ast_was_decompressed = EXCLUDED.ast_was_decompressed,
                                ast_leaf_seq = EXCLUDED.ast_leaf_seq,
                                ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
                                ast_owner_type = EXCLUDED.ast_owner_type,
                                ast_onchain_data = EXCLUDED.ast_onchain_data,
                                ast_supply_slot_updated = EXCLUDED.ast_supply_slot_updated
                            WHERE assets.ast_slot_updated < EXCLUDED.ast_slot_updated OR assets.ast_slot_updated IS NULL;",
        );

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Upsert assets: {}", err)))?;

        Ok(())
    }

    pub async fn insert_tasks(&self, data: &Vec<Task>) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for off_d in data {
            keys.insert(off_d.ofd_metadata_url.as_str());
        }

        let keys = keys.into_iter().collect::<Vec<_>>();
        let ids_keys = self.insert_metadata(&keys).await?;

        let mut offchain_data_to_insert = Vec::new();

        for offchain_d in data.iter() {
            offchain_data_to_insert.push(TaskForInsert {
                ofd_metadata_url: ids_keys
                    .get(&offchain_d.ofd_metadata_url)
                    .copied()
                    .unwrap_or_default(),
                ofd_locked_until: offchain_d.ofd_locked_until,
                ofd_attempts: offchain_d.ofd_attempts,
                ofd_max_attempts: offchain_d.ofd_max_attempts,
                ofd_error: offchain_d.ofd_error.clone(),
            });
        }

        offchain_data_to_insert.sort_by(|a, b| a.ofd_metadata_url.cmp(&b.ofd_metadata_url));

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO tasks (
                tsk_metadata_url,
                tsk_locked_until,
                tsk_attempts,
                tsk_max_attempts,
                tsk_error,
                tsk_status
            ) ",
        );

        query_builder.push_values(offchain_data_to_insert, |mut b, off_d| {
            b.push_bind(off_d.ofd_metadata_url);
            b.push_bind(off_d.ofd_locked_until);
            b.push_bind(off_d.ofd_attempts);
            b.push_bind(off_d.ofd_max_attempts);
            b.push_bind(off_d.ofd_error);
            b.push_bind(TaskStatus::Pending);
        });

        query_builder.push("ON CONFLICT (tsk_id) DO NOTHING;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Insert tasks: {}", err)))?;

        Ok(())
    }

    pub async fn drop_creators_for_assets(&self, assets: &[Vec<u8>]) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for key in assets.iter() {
            keys.insert(key.clone());
        }

        let keys = keys.into_iter().collect::<Vec<Vec<u8>>>();
        let ids_keys = self.get_pubkey_ids(&keys).await?;
        let keys: Vec<i64> = ids_keys.into_iter().map(|k| k.1).collect();

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DELETE FROM asset_creators WHERE asc_asset IN (");

        for (i, k) in keys.iter().enumerate() {
            query_builder.push_bind(k);
            if i < keys.len() - 1 {
                query_builder.push(",");
            }
        }
        query_builder.push(")");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Drop creators for assets: {}", err))
        })?;

        Ok(())
    }

    pub async fn insert_creators_for_assets(
        &self,
        creators: &Vec<Creator>,
    ) -> Result<(), IngesterError> {
        let mut keys = HashSet::new();
        for creator in creators.iter() {
            keys.insert(creator.asc_asset.clone());
            keys.insert(creator.asc_creator.clone());
        }

        let keys = keys.into_iter().collect::<Vec<Vec<u8>>>();
        let ids_keys = self.get_pubkey_ids(&keys).await?;
        let keys_map = ids_keys
            .into_iter()
            .collect::<std::collections::HashMap<Vec<u8>, i64>>();

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO asset_creators (
                asc_asset,
                asc_creator,
                asc_share,
                asc_verified,
                asc_seq,
                asc_slot_updated,
                asc_position
            ) ",
        );

        query_builder.push_values(creators, |mut b, creator| {
            let asset = keys_map.get(&creator.asc_asset);
            b.push_bind(asset);
            let creator_k = keys_map.get(&creator.asc_creator);
            b.push_bind(creator_k);
            b.push_bind(creator.asc_share);
            b.push_bind(creator.asc_verified);
            b.push_bind(creator.asc_seq);
            b.push_bind(creator.asc_slot_updated);
            b.push_bind(creator.asc_position);
        });

        query_builder.push("ON CONFLICT (asc_asset, asc_creator) DO NOTHING;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Insert creators for assets: {}", err))
        })?;

        Ok(())
    }

    async fn get_map_of_keys(
        &self,
        pubkeys: Vec<Vec<u8>>,
    ) -> Result<HashMap<Vec<u8>, i64>, IngesterError> {
        self.insert_pubkeys(&pubkeys).await?;

        let ids_keys = self.get_pubkey_ids(&pubkeys).await?;
        let keys_map = ids_keys.into_iter().collect::<HashMap<Vec<u8>, i64>>();

        Ok(keys_map)
    }

    pub async fn mark_asset_as_burned(
        &self,
        asset: Vec<u8>,
        seq: u64,
    ) -> Result<(), IngesterError> {
        let keys_map = self.get_map_of_keys(vec![asset.clone()]).await?;

        let asset_id = keys_map.get(&asset).unwrap();

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO assets (ast_pubkey, ast_is_burnt, ast_seq, ast_is_collection_verified, ast_is_compressed) ");

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(true);
            b.push_bind(seq as i64);
            b.push_bind(false);
            b.push_bind(true);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_is_burnt = EXCLUDED.ast_is_burnt, ast_seq = EXCLUDED.ast_seq,
                                    ast_is_compressed = EXCLUDED.ast_is_compressed;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Mark asset as burnt: {}", err)))?;

        Ok(())
    }

    pub async fn save_changelog(
        &self,
        change_log_event: &ChangeLogEventV1,
    ) -> Result<(), IngesterError> {
        let tree = change_log_event.id.as_ref().to_vec();

        let keys_map = self.get_map_of_keys(vec![tree.clone()]).await?;

        let tree_id = keys_map.get(&tree).unwrap();

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO cl_items (cli_tree, cli_node_idx, cli_leaf_idx, cli_seq, cli_level, cli_hash) ");

        let mut values = Vec::new();

        let mut i: i64 = 0;
        let depth = change_log_event.path.len() - 1;
        for p in change_log_event.path.iter() {
            let node_idx = p.index as i64;

            let leaf_idx = if i == 0 {
                Some(self.node_idx_to_leaf_idx(node_idx, depth as u32))
            } else {
                None
            };

            i += 1;

            values.push((
                tree_id,
                node_idx,
                leaf_idx,
                change_log_event.seq as i64,
                i,
                p.node.as_ref(),
            ));
        }

        query_builder.push_values(values, |mut b, value| {
            b.push_bind(value.0);
            b.push_bind(value.1);
            b.push_bind(value.2);
            b.push_bind(value.3);
            b.push_bind(value.4);
            b.push_bind(value.5);
        });

        query_builder.push(" ON CONFLICT (cli_tree, cli_node_idx) DO UPDATE SET cli_hash = EXCLUDED.cli_hash, cli_seq = EXCLUDED.cli_seq,
                                cli_leaf_idx = EXCLUDED.cli_leaf_idx, cli_level = EXCLUDED.cli_level
                                WHERE cl_items.cli_seq < EXCLUDED.cli_seq OR cl_items.cli_seq IS NULL;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Save changelog: {}", err)))?;

        Ok(())
    }

    pub async fn asset_redeem(
        &self,
        change_log_event: &ChangeLogEventV1,
    ) -> Result<(), IngesterError> {
        let leaf_index = change_log_event.index;
        let (asset, _) = Pubkey::find_program_address(
            &[
                "asset".as_bytes(),
                change_log_event.id.as_ref(),
                self.u32_to_u8_array(leaf_index).as_ref(),
            ],
            &mpl_bubblegum::ID,
        );
        let asset = asset.to_bytes().to_vec();
        let tree = change_log_event.id.to_bytes().to_vec();
        let nonce = change_log_event.index as i64;

        let keys_map = self
            .get_map_of_keys(vec![asset.clone(), tree.clone()])
            .await?;

        let asset_id = keys_map.get(&asset).unwrap();
        let tree_id = keys_map.get(&tree).unwrap();

        self.update_asset_leaf_info(
            *asset_id,
            nonce,
            *tree_id,
            vec![0; 32],
            change_log_event.seq as i64,
            vec![0; 32],
            vec![0; 32],
        )
        .await?;

        self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
            .await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_asset_leaf_info(
        &self,
        asset_id: i64,
        nonce: i64,
        tree_id: i64,
        leaf: Vec<u8>,
        leaf_seq: i64,
        data_hash: Vec<u8>,
        creator_hash: Vec<u8>,
    ) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO assets (ast_pubkey, ast_nonce, ast_tree_id, ast_leaf, ast_leaf_seq, ast_data_hash,
                                ast_creator_hash, ast_is_collection_verified, ast_is_compressed, ast_is_frozen) ");

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(nonce);
            b.push_bind(tree_id);
            b.push_bind(leaf.clone());
            b.push_bind(leaf_seq);
            b.push_bind(data_hash.clone());
            b.push_bind(creator_hash.clone());
            b.push_bind(false);
            b.push_bind(true);
            b.push_bind(false);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_nonce = EXCLUDED.ast_nonce, ast_tree_id = EXCLUDED.ast_tree_id,
                                    ast_leaf = EXCLUDED.ast_leaf, ast_leaf_seq = EXCLUDED.ast_leaf_seq, ast_data_hash = EXCLUDED.ast_data_hash,
                                    ast_creator_hash = EXCLUDED.ast_creator_hash
                                WHERE assets.ast_leaf_seq < EXCLUDED.ast_leaf_seq OR assets.ast_leaf_seq IS NULL;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Asset update leaf info: {}", err))
        })?;

        Ok(())
    }

    async fn update_asset_owner_and_delegate_info(
        &self,
        asset_id: i64,
        owner_id: i64,
        delegate_id: i64,
        owner_seq: i64,
    ) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO assets (ast_pubkey, ast_owner, ast_delegate, ast_owner_delegate_seq, ast_is_compressed)");

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(owner_id);
            b.push_bind(delegate_id);
            b.push_bind(owner_seq);
            b.push_bind(true);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_owner = EXCLUDED.ast_owner,
                                    ast_delegate = EXCLUDED.ast_delegate, ast_owner_delegate_seq = EXCLUDED.ast_owner_delegate_seq
                                    WHERE assets.ast_owner_delegate_seq < EXCLUDED.ast_owner_delegate_seq OR assets.ast_owner_delegate_seq IS NULL;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Update asset owner and delegate info: {}", err))
        })?;

        Ok(())
    }

    async fn upsert_asset_seq(&self, asset_id: i64, seq: i64) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO assets (ast_pubkey, ast_seq) ");

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(seq);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_seq = EXCLUDED.ast_seq WHERE assets.ast_seq < EXCLUDED.ast_seq OR assets.ast_seq IS NULL;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Upsert asset seq: {}", err)))?;

        Ok(())
    }

    pub async fn asset_cancel_redeem(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
    ) -> Result<(), IngesterError> {
        match leaf_schema.schema {
            LeafSchema::V1 {
                id,
                owner,
                delegate,
                ..
            } => {
                let asset = id.to_bytes().to_vec();
                let owner = owner.to_bytes().to_vec();
                let tree = change_log_event.id.to_bytes().to_vec();
                let delegate = delegate.to_bytes().to_vec();
                let nonce = change_log_event.index as i64;

                let keys_map = self
                    .get_map_of_keys(vec![
                        asset.clone(),
                        owner.clone(),
                        tree.clone(),
                        delegate.clone(),
                    ])
                    .await?;

                let asset_id = keys_map.get(&asset).unwrap();
                let owner_id = keys_map.get(&owner).unwrap();
                let tree_id = keys_map.get(&tree).unwrap();
                let delegate_id = keys_map.get(&delegate).unwrap();

                self.update_asset_leaf_info(
                    *asset_id,
                    nonce,
                    *tree_id,
                    leaf_schema.leaf_hash.to_vec(),
                    change_log_event.seq as i64,
                    leaf_schema.schema.data_hash().to_vec(),
                    leaf_schema.schema.creator_hash().to_vec(),
                )
                .await?;

                self.update_asset_owner_and_delegate_info(
                    *asset_id,
                    *owner_id,
                    *delegate_id,
                    change_log_event.seq as i64,
                )
                .await?;

                self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn asset_collection_verified(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
        payload: &Payload,
    ) -> Result<(), IngesterError> {
        let (collection, verify) = match payload {
            Payload::CollectionVerification {
                collection, verify, ..
            } => (*collection, *verify),
            _ => {
                return Err(IngesterError::DatabaseError(
                    "Ix not parsed correctly".to_string(),
                ));
            }
        };

        let asset = match leaf_schema.schema {
            LeafSchema::V1 { id, .. } => id.to_bytes().to_vec(),
        };
        let tree = change_log_event.id.to_bytes().to_vec();
        let collection = collection.to_bytes().to_vec();
        let nonce = change_log_event.index as i64;

        let keys_map = self
            .get_map_of_keys(vec![asset.clone(), tree.clone(), collection.clone()])
            .await?;

        let asset_id = keys_map.get(&asset).unwrap();
        let tree_id = keys_map.get(&tree).unwrap();
        let collection_id = keys_map.get(&collection).unwrap();

        self.update_asset_leaf_info(
            *asset_id,
            nonce,
            *tree_id,
            leaf_schema.leaf_hash.to_vec(),
            change_log_event.seq as i64,
            leaf_schema.schema.data_hash().to_vec(),
            leaf_schema.schema.creator_hash().to_vec(),
        )
        .await?;

        self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
            .await?;

        self.update_asset_collection_info(
            *asset_id,
            *collection_id,
            verify,
            change_log_event.seq as i64,
        )
        .await?;

        Ok(())
    }

    async fn update_asset_collection_info(
        &self,
        asset_id: i64,
        collection_id: i64,
        verified: bool,
        collection_seq: i64,
    ) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO assets (ast_pubkey, ast_collection, ast_is_collection_verified, ast_collection_seq) ");

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(collection_id);
            b.push_bind(verified);
            b.push_bind(collection_seq);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_collection = EXCLUDED.ast_collection,
                                    ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
                                    ast_collection_seq = EXCLUDED.ast_collection_seq
                                    WHERE assets.ast_collection_seq < EXCLUDED.ast_collection_seq OR assets.ast_collection_seq IS NULL;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Update asset collection: {}", err))
        })?;

        Ok(())
    }

    async fn update_creator_verified(
        &self,
        asset_id: i64,
        creator_id: i64,
        verified: bool,
        seq: i64,
    ) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO asset_creators (asc_asset, asc_creator, asc_verified, asc_seq) ",
        );

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(creator_id);
            b.push_bind(verified);
            b.push_bind(seq);
        });

        query_builder.push(" ON CONFLICT (asc_asset, asc_creator) DO UPDATE SET asc_creator = EXCLUDED.asc_creator,
                                    asc_verified = EXCLUDED.asc_verified, asc_seq = EXCLUDED.asc_seq
                                    WHERE asset_creators.asc_seq < EXCLUDED.asc_seq OR asset_creators.asc_seq IS NULL;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Update asset collection: {}", err))
        })?;

        Ok(())
    }

    pub async fn asset_creator_verified(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
        payload: &Payload,
    ) -> Result<(), IngesterError> {
        let (creator, verify) = match payload {
            Payload::CreatorVerification {
                creator, verify, ..
            } => (creator, verify),
            _ => {
                return Err(IngesterError::DatabaseError(
                    "Ix not parsed correctly".to_string(),
                ));
            }
        };

        match leaf_schema.schema {
            LeafSchema::V1 {
                id,
                owner,
                delegate,
                ..
            } => {
                let asset = id.to_bytes().to_vec();
                let owner = owner.to_bytes().to_vec();
                let tree = change_log_event.id.to_bytes().to_vec();
                let creator = creator.to_bytes().to_vec();
                let delegate = delegate.to_bytes().to_vec();
                let nonce = change_log_event.index as i64;

                let keys_map = self
                    .get_map_of_keys(vec![
                        asset.clone(),
                        owner.clone(),
                        tree.clone(),
                        creator.clone(),
                        delegate.clone(),
                    ])
                    .await?;

                let asset_id = keys_map.get(&asset).unwrap();
                let owner_id = keys_map.get(&owner).unwrap();
                let tree_id = keys_map.get(&tree).unwrap();
                let creator_id = keys_map.get(&creator).unwrap();
                let delegate_id = keys_map.get(&delegate).unwrap();

                self.update_asset_leaf_info(
                    *asset_id,
                    nonce,
                    *tree_id,
                    leaf_schema.leaf_hash.to_vec(),
                    change_log_event.seq as i64,
                    leaf_schema.schema.data_hash().to_vec(),
                    leaf_schema.schema.creator_hash().to_vec(),
                )
                .await?;

                self.update_asset_owner_and_delegate_info(
                    *asset_id,
                    *owner_id,
                    *delegate_id,
                    change_log_event.seq as i64,
                )
                .await?;

                self.update_creator_verified(
                    *asset_id,
                    *creator_id,
                    *verify,
                    change_log_event.seq as i64,
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn asset_decompress(&self, asset: Vec<u8>) -> Result<(), IngesterError> {
        let keys_map = self.get_map_of_keys(vec![asset.clone()]).await?;

        let asset_id = keys_map.get(&asset).unwrap();

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO assets (ast_pubkey, ast_tree_id, ast_leaf, ast_nonce,
                                        ast_data_hash, ast_creator_hash, ast_is_compressed,
                                        ast_supply, ast_was_decompressed, ast_seq) ",
        );

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(None::<i64>);
            b.push_bind(None::<i64>);
            b.push_bind(0);
            b.push_bind(None::<Vec<u8>>);
            b.push_bind(None::<Vec<u8>>);
            b.push_bind(false);
            b.push_bind(1);
            b.push_bind(true);
            b.push_bind(0);
        });

        query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET ast_tree_id = EXCLUDED.ast_tree_id,
                                    ast_leaf = EXCLUDED.ast_leaf, ast_nonce = EXCLUDED.ast_nonce, ast_data_hash = EXCLUDED.ast_data_hash,
                                    ast_creator_hash = EXCLUDED.ast_creator_hash, ast_is_compressed = EXCLUDED.ast_is_compressed,
                                    ast_supply = EXCLUDED.ast_supply, ast_was_decompressed = EXCLUDED.ast_was_decompressed, ast_seq = EXCLUDED.ast_seq;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Update asset decompress: {}", err))
        })?;

        Ok(())
    }

    pub async fn asset_delegate(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
    ) -> Result<(), IngesterError> {
        match leaf_schema.schema {
            LeafSchema::V1 {
                id,
                owner,
                delegate,
                ..
            } => {
                let asset = id.to_bytes().to_vec();
                let owner = owner.to_bytes().to_vec();
                let tree = change_log_event.id.to_bytes().to_vec();
                let delegate = delegate.to_bytes().to_vec();

                let keys_map = self
                    .get_map_of_keys(vec![
                        asset.clone(),
                        owner.clone(),
                        tree.clone(),
                        delegate.clone(),
                    ])
                    .await?;

                let asset_id = keys_map.get(&asset).unwrap();
                let owner_id = keys_map.get(&owner).unwrap();
                let tree_id = keys_map.get(&tree).unwrap();
                let delegate_id = keys_map.get(&delegate).unwrap();

                self.update_asset_leaf_info(
                    *asset_id,
                    change_log_event.index as i64,
                    *tree_id,
                    leaf_schema.leaf_hash.to_vec(),
                    change_log_event.seq as i64,
                    leaf_schema.schema.data_hash().to_vec(),
                    leaf_schema.schema.creator_hash().to_vec(),
                )
                .await?;

                self.update_asset_owner_and_delegate_info(
                    *asset_id,
                    *owner_id,
                    *delegate_id,
                    change_log_event.seq as i64,
                )
                .await?;

                self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn asset_mint(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
        payload: &Option<Payload>,
        slot_updated: u64,
        tree: Vec<u8>,
        authority: Vec<u8>,
    ) -> Result<(), IngesterError> {
        let metadata = match payload {
            Some(Payload::MintV1 { args }) => args,
            _ => {
                return Err(IngesterError::DatabaseError(
                    "Ix not parsed correctly".to_string(),
                ));
            }
        };

        match leaf_schema.schema {
            LeafSchema::V1 {
                id,
                delegate,
                owner,
                nonce,
                ..
            } => {
                let asset = id.to_bytes().to_vec();
                let uri = metadata.uri.trim().replace('\0', "");
                let mut chain_data = ChainDataV1 {
                    name: metadata.name.clone(),
                    symbol: metadata.symbol.clone(),
                    edition_nonce: metadata.edition_nonce,
                    primary_sale_happened: metadata.primary_sale_happened,
                    token_standard: Some(TokenStandard::NonFungible),
                    uses: metadata.uses.clone().map(|u| Uses {
                        use_method: UseMethod::from_u8(u.use_method as u8).unwrap(),
                        remaining: u.remaining,
                        total: u.total,
                    }),
                };
                chain_data.sanitize();
                let chain_data_json = serde_json::to_string(&chain_data)
                    .map_err(|e| IngesterError::DatabaseError(e.to_string()))?;
                if uri.is_empty() {
                    return Err(IngesterError::DatabaseError("URI is empty".to_string()));
                }

                let delegate = delegate.to_bytes().to_vec();
                let owner = owner.to_bytes().to_vec();

                let collection = metadata
                    .collection
                    .clone()
                    .map_or(vec![0; 32], |v| v.key.to_bytes().to_vec());
                let collection_verified = metadata.collection.clone().map_or(false, |v| v.verified);

                let mut all_necessary_keys = Vec::new();
                for c in metadata.creators.iter() {
                    all_necessary_keys.push(c.address.to_bytes().to_vec());
                }
                all_necessary_keys.push(asset.clone());
                all_necessary_keys.push(owner.clone());
                all_necessary_keys.push(delegate.clone());
                all_necessary_keys.push(collection.clone());
                all_necessary_keys.push(tree.clone());
                all_necessary_keys.push(authority.clone());

                let keys_map = self.get_map_of_keys(all_necessary_keys).await?;

                let asset_id = keys_map.get(&asset).unwrap();
                let owner_id = keys_map.get(&owner).unwrap();
                let delegate_id = keys_map.get(&delegate).unwrap();
                let collection_id = keys_map.get(&collection).unwrap();
                let tree_id = keys_map.get(&tree).unwrap();
                let authority_id = keys_map.get(&authority).unwrap();

                let mut query_builder: QueryBuilder<'_, Postgres> =
                    QueryBuilder::new("INSERT INTO assets (ast_pubkey, ast_is_compressed, ast_is_frozen, ast_supply, ast_tree_id,
                                        ast_nonce, ast_royalty_target_type, ast_royalty_target, ast_royalty_amount, ast_is_burnt,
                                        ast_slot_updated, ast_was_decompressed, ast_is_collection_verified, ast_specification_asset_class, ast_onchain_data) ");

                query_builder.push_values(vec![*asset_id], |mut b, asset| {
                    b.push_bind(asset);
                    b.push_bind(true);
                    b.push_bind(false);
                    b.push_bind(1);
                    b.push_bind(tree_id);
                    b.push_bind(nonce as i64);
                    b.push_bind(RoyaltyTargetType::Creators);
                    b.push_bind(None::<i64>);
                    b.push_bind(metadata.seller_fee_basis_points as i64);
                    b.push_bind(false);
                    b.push_bind(slot_updated as i64);
                    b.push_bind(false);
                    b.push_bind(false);
                    b.push_bind(SpecificationAssetClass::Nft);
                    b.push_bind(chain_data_json.clone());
                });

                query_builder.push(" ON CONFLICT (ast_pubkey) DO UPDATE SET
                                            ast_is_frozen = EXCLUDED.ast_is_frozen, ast_supply = EXCLUDED.ast_supply,
                                            ast_tree_id = EXCLUDED.ast_tree_id, ast_nonce = EXCLUDED.ast_nonce,
                                            ast_royalty_target_type = EXCLUDED.ast_royalty_target_type, ast_royalty_target = EXCLUDED.ast_royalty_target,
                                            ast_royalty_amount = EXCLUDED.ast_royalty_amount, ast_is_burnt = EXCLUDED.ast_is_burnt,
                                            ast_slot_updated = EXCLUDED.ast_slot_updated, ast_was_decompressed = EXCLUDED.ast_was_decompressed,
                                            ast_onchain_data = EXCLUDED.ast_onchain_data
                                        WHERE assets.ast_slot_updated < EXCLUDED.ast_slot_updated OR assets.ast_slot_updated IS NULL;");

                let query = query_builder.build();
                query
                    .execute(&self.pool)
                    .await
                    .map_err(|err| IngesterError::DatabaseError(format!("Asset mint: {}", err)))?;

                self.update_asset_leaf_info(
                    *asset_id,
                    nonce as i64,
                    *tree_id,
                    leaf_schema.leaf_hash.to_vec(),
                    change_log_event.seq as i64,
                    leaf_schema.schema.data_hash().to_vec(),
                    leaf_schema.schema.creator_hash().to_vec(),
                )
                .await?;

                self.update_asset_owner_and_delegate_info(
                    *asset_id,
                    *owner_id,
                    *delegate_id,
                    change_log_event.seq as i64,
                )
                .await?;

                self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
                    .await?;

                self.update_asset_collection_info(
                    *asset_id,
                    *collection_id,
                    collection_verified,
                    change_log_event.seq as i64,
                )
                .await?;

                self.insert_asset_offchain_data(*asset_id, uri).await?;

                if !metadata.creators.is_empty() {
                    let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO asset_creators (asc_asset, asc_creator, asc_share, asc_verified, asc_seq, asc_slot_updated, asc_position) ",
                    );

                    let mut i: i64 = 0;
                    query_builder.push_values(metadata.creators.iter(), |mut b, c| {
                        b.push_bind(asset_id);
                        b.push_bind(keys_map.get(&c.address.to_bytes().to_vec()).unwrap());
                        b.push_bind(c.share as i32);
                        b.push_bind(c.verified);
                        b.push_bind(change_log_event.seq as i64);
                        b.push_bind(slot_updated as i64);
                        b.push_bind(i);
                        i += 1;
                    });

                    query_builder.push(" ON CONFLICT (asc_asset, asc_creator) DO NOTHING;");

                    let query = query_builder.build();
                    query.execute(&self.pool).await.map_err(|err| {
                        IngesterError::DatabaseError(format!("Insert asset offchain data: {}", err))
                    })?;
                }

                // TODO: drop and insert with command above once have stable version
                self.update_authority(*asset_id, *authority_id).await?;
            }
        }
        Ok(())
    }

    async fn update_authority(
        &self,
        asset_id: i64,
        authority_id: i64,
    ) -> Result<(), IngesterError> {
        sqlx::query("UPDATE assets SET ast_authority = $1 WHERE ast_pubkey = $2;")
            .bind(authority_id)
            .bind(asset_id)
            .execute(&self.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Update authority: {}", err)))?;

        Ok(())
    }

    pub async fn asset_transfer(
        &self,
        change_log_event: &ChangeLogEventV1,
        leaf_schema: &LeafSchemaEvent,
    ) -> Result<(), IngesterError> {
        match leaf_schema.schema {
            LeafSchema::V1 {
                id,
                owner,
                delegate,
                ..
            } => {
                let asset = id.to_bytes().to_vec();
                let owner = owner.to_bytes().to_vec();
                let tree = change_log_event.id.to_bytes().to_vec();
                let delegate = delegate.to_bytes().to_vec();

                let keys_map = self
                    .get_map_of_keys(vec![
                        asset.clone(),
                        owner.clone(),
                        tree.clone(),
                        delegate.clone(),
                    ])
                    .await?;

                let asset_id = keys_map.get(&asset).unwrap();
                let owner_id = keys_map.get(&owner).unwrap();
                let tree_id = keys_map.get(&tree).unwrap();
                let delegate_id = keys_map.get(&delegate).unwrap();

                self.update_asset_leaf_info(
                    *asset_id,
                    change_log_event.index as i64,
                    *tree_id,
                    leaf_schema.leaf_hash.to_vec(),
                    change_log_event.seq as i64,
                    leaf_schema.schema.data_hash().to_vec(),
                    leaf_schema.schema.creator_hash().to_vec(),
                )
                .await?;

                self.update_asset_owner_and_delegate_info(
                    *asset_id,
                    *owner_id,
                    *delegate_id,
                    change_log_event.seq as i64,
                )
                .await?;

                self.upsert_asset_seq(*asset_id, change_log_event.seq as i64)
                    .await?;
            }
        }
        Ok(())
    }

    // TODO: update in assets table
    async fn insert_asset_offchain_data(
        &self,
        asset_id: i64,
        metadata_url: String,
    ) -> Result<(), IngesterError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO offchain_data (ofd_pubkey, ofd_metadata_url, ofd_status) ",
        );

        query_builder.push_values(vec![asset_id], |mut b, asset| {
            b.push_bind(asset);
            b.push_bind(metadata_url.clone());
            b.push_bind(TaskStatus::Pending);
        });

        query_builder.push(" ON CONFLICT (ofd_pubkey) DO NOTHING;");

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            IngesterError::DatabaseError(format!("Insert asset offchain data: {}", err))
        })?;

        Ok(())
    }

    fn node_idx_to_leaf_idx(&self, index: i64, tree_height: u32) -> i64 {
        index - 2i64.pow(tree_height)
    }

    // PDA lookup requires an 8-byte array.
    fn u32_to_u8_array(&self, value: u32) -> [u8; 8] {
        let bytes: [u8; 4] = value.to_le_bytes();
        let mut result: [u8; 8] = [0; 8];
        result[..4].copy_from_slice(&bytes);
        result
    }
}
