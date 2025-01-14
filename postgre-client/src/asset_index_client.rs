use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::Arc,
    vec,
};

use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Executor, Postgres, QueryBuilder, Transaction};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    error::IndexDbError,
    model::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    storage_traits::{AssetIndexStorage, NFTSemaphores},
    PgClient, BATCH_DELETE_ACTION, BATCH_SELECT_ACTION, BATCH_UPSERT_ACTION, CREATE_ACTION,
    DROP_ACTION, INSERT_TASK_PARAMETERS_COUNT, POSTGRES_PARAMETERS_COUNT_LIMIT, SELECT_ACTION,
    SQL_COMPONENT, TRANSACTION_ACTION, UPDATE_ACTION,
};
use entities::{
    enums::AssetType,
    models::{AssetIndex, Creator, FungibleAssetIndex, FungibleToken, UrlWithStatus},
};

pub const INSERT_ASSET_PARAMETERS_COUNT: usize = 19;
pub const DELETE_ASSET_CREATOR_PARAMETERS_COUNT: usize = 2;
pub const INSERT_ASSET_CREATOR_PARAMETERS_COUNT: usize = 4;
pub const INSERT_AUTHORITY_PARAMETERS_COUNT: usize = 3;
pub const INSERT_FUNGIBLE_TOKEN_PARAMETERS_COUNT: usize = 4;

impl PgClient {
    pub(crate) async fn fetch_last_synced_id_impl(
        &self,
        table_name: &str,
        executor: impl Executor<'_, Database = Postgres>,
        asset_type: AssetType,
    ) -> Result<Option<Vec<u8>>, IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("SELECT last_synced_asset_update_key FROM ");
        query_builder.push(table_name);
        query_builder.push(format!(" WHERE id = {}", asset_type as i32));
        let start_time = chrono::Utc::now();
        let query = query_builder.build_query_as::<(Option<Vec<u8>>,)>();
        let result = query.fetch_one(executor).await.inspect_err(|_e| {
            self.metrics
                .observe_error(SQL_COMPONENT, SELECT_ACTION, table_name);
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, table_name, start_time);
        Ok(result.0)
    }

    pub(crate) async fn upsert_batched_fungible(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table_name: FungibleTokenTable,
        fungible_tokens: Vec<FungibleToken>,
    ) -> Result<(), IndexDbError> {
        for fungible_tokens_chunk in fungible_tokens
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / INSERT_FUNGIBLE_TOKEN_PARAMETERS_COUNT)
        {
            self.insert_fungible_tokens(transaction, fungible_tokens_chunk, &table_name)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn upsert_batched_nft(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table_names: TableNames,
        updated_components: AssetComponenents,
    ) -> Result<(), IndexDbError> {
        for chunk in updated_components
            .metadata_urls
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / INSERT_TASK_PARAMETERS_COUNT)
        {
            self.insert_tasks(transaction, chunk, table_names.metadata_table.as_str())
                .await?;
        }
        for chunk in updated_components
            .authorities
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / INSERT_AUTHORITY_PARAMETERS_COUNT)
        {
            self.insert_authorities(transaction, chunk, table_names.authorities_table.as_str())
                .await?;
        }
        for chunk in updated_components
            .asset_indexes
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / INSERT_ASSET_PARAMETERS_COUNT)
        {
            self.insert_assets(transaction, chunk, table_names.assets_table.as_str())
                .await?;
        }

        let mut existing_creators: Vec<(Pubkey, Creator)> = vec![];
        for chunk in updated_components
            .updated_keys
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT)
        {
            let creators = self
                .batch_get_creators(transaction, chunk, table_names.creators_table.as_str())
                .await?;
            existing_creators.extend(creators.iter().map(|(pk, c, _)| (*pk, c.clone())));
        }
        let creator_updates =
            Self::diff(updated_components.all_creators.clone(), existing_creators);
        self.update_creators(
            transaction,
            creator_updates,
            table_names.creators_table.as_str(),
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn update_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        creator_updates: CreatorsUpdates,
        table_name: &str,
    ) -> Result<(), IndexDbError> {
        for chunk in creator_updates
            .to_remove
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / DELETE_ASSET_CREATOR_PARAMETERS_COUNT)
        {
            self.delete_creators(transaction, chunk, table_name).await?;
        }
        for chunk in creator_updates
            .new_or_updated
            .chunks(POSTGRES_PARAMETERS_COUNT_LIMIT / INSERT_ASSET_CREATOR_PARAMETERS_COUNT)
        {
            self.insert_creators(transaction, chunk, table_name).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Authority {
    pub key: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: i64,
}
pub(crate) struct AssetComponenents {
    pub metadata_urls: Vec<UrlWithStatus>,
    pub asset_indexes: Vec<AssetIndex>,
    pub all_creators: Vec<(Pubkey, Creator, i64)>,
    pub updated_keys: Vec<Vec<u8>>,
    pub authorities: Vec<Authority>,
}

pub(crate) struct TableNames {
    pub metadata_table: String,
    pub assets_table: String,
    pub creators_table: String,
    pub authorities_table: String,
}

pub(crate) struct FungibleTokenTable(String);

impl FungibleTokenTable {
    pub fn new(table_name: &str) -> Self {
        Self(table_name.to_string())
    }
}

impl Deref for FungibleTokenTable {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) fn split_into_fungible_tokens(
    asset_indexes: &[FungibleAssetIndex],
) -> Vec<FungibleToken> {
    asset_indexes
        .iter()
        .map(|asset_index| {
            FungibleToken {
                key: asset_index.pubkey,
                slot_updated: asset_index.slot_updated,
                // it's unlikely that rows below will not be filled for fungible token
                // but even if that happens we will save asset with default values
                owner: asset_index.owner.unwrap_or_default(),
                asset: asset_index.fungible_asset_mint.unwrap_or_default(),
                balance: asset_index.fungible_asset_balance.unwrap_or_default() as i64,
            }
        })
        .collect()
}

pub(crate) fn split_assets_into_components(asset_indexes: &[AssetIndex]) -> AssetComponenents {
    // First we need to bulk upsert metadata_url into metadata and get back ids for each metadata_url to upsert into assets_v3
    let mut metadata_urls: Vec<_> = asset_indexes
        .iter()
        .filter_map(|asset_index| asset_index.metadata_url.clone())
        .collect::<HashSet<_>>()
        .iter()
        .map(|url| UrlWithStatus::new(url.metadata_url.as_str(), url.is_downloaded))
        .collect();
    metadata_urls.sort_by_key(|a| a.get_metadata_id());

    let mut asset_indexes = asset_indexes.to_vec();
    asset_indexes.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));

    let asset_indexes = asset_indexes
        .into_iter()
        .filter(|asset| asset.fungible_asset_mint.is_none())
        .collect::<Vec<AssetIndex>>();

    // Collect all creators from all assets
    let mut all_creators: Vec<(Pubkey, Creator, i64)> = asset_indexes
        .iter()
        .flat_map(|asset_index: &AssetIndex| {
            asset_index.creators.iter().map(move |creator| {
                (
                    asset_index.pubkey,
                    creator.clone(),
                    asset_index.slot_updated,
                )
            })
        })
        .collect();

    all_creators.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.creator.cmp(&b.1.creator),
        other => other,
    });

    let updated_keys: Vec<Vec<u8>> = asset_indexes
        .iter()
        .map(|asset_index| asset_index.pubkey.to_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>();
    let mut authorities: HashMap<Pubkey, Authority> = HashMap::new();
    for asset in asset_indexes.iter() {
        let authority = asset.update_authority.or(asset.authority);
        let authority_key = if asset.update_authority.is_some() {
            asset.collection
        } else {
            Some(asset.pubkey)
        };
        if let (Some(authority_key), Some(authority)) = (authority_key, authority) {
            let new_entry = Authority {
                key: authority_key,
                authority,
                slot_updated: asset.slot_updated,
            };

            authorities
                .entry(authority_key)
                .and_modify(|existing_entry| {
                    if new_entry.slot_updated > existing_entry.slot_updated {
                        *existing_entry = new_entry;
                    }
                })
                .or_insert(new_entry);
        }
    }
    let mut authorities = authorities.into_values().collect::<Vec<_>>();
    authorities.sort_by(|a, b| a.key.cmp(&b.key));

    AssetComponenents {
        metadata_urls,
        asset_indexes,
        all_creators,
        updated_keys,
        authorities,
    }
}

#[async_trait]
impl AssetIndexStorage for PgClient {
    async fn fetch_last_synced_id(
        &self,
        asset_type: AssetType,
    ) -> Result<Option<Vec<u8>>, IndexDbError> {
        self.fetch_last_synced_id_impl("last_synced_key", &self.pool, asset_type)
            .await
    }

    async fn update_nft_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
    ) -> Result<(), IndexDbError> {
        let updated_components = split_assets_into_components(asset_indexes);
        let table_names = TableNames {
            metadata_table: "tasks".to_string(),
            assets_table: "assets_v3".to_string(),
            creators_table: "asset_creators_v3".to_string(),
            authorities_table: "assets_authorities".to_string(),
        };
        let operation_start_time = chrono::Utc::now();
        let mut transaction = self.start_transaction().await?;

        // Perform transactional operations
        let result = self
            .upsert_batched_nft(&mut transaction, table_names, updated_components)
            .await;

        match result {
            Ok(_) => {
                // Commit the transaction and record the time taken
                self.commit_transaction(transaction).await?;
                self.metrics.observe_request(
                    SQL_COMPONENT,
                    TRANSACTION_ACTION,
                    "transaction_total",
                    operation_start_time,
                );
                Ok(())
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                self.metrics.observe_request(
                    SQL_COMPONENT,
                    TRANSACTION_ACTION,
                    "transaction_failed_total",
                    operation_start_time,
                );
                Err(e)
            }
        }
    }

    async fn update_fungible_asset_indexes_batch(
        &self,
        fungible_asset_indexes: &[FungibleAssetIndex],
    ) -> Result<(), IndexDbError> {
        let operation_start_time = chrono::Utc::now();
        let mut transaction = self.start_transaction().await?;
        let table_name = FungibleTokenTable::new("fungible_tokens");

        // Perform transactional operations
        let result = self
            .upsert_batched_fungible(
                &mut transaction,
                table_name,
                split_into_fungible_tokens(fungible_asset_indexes),
            )
            .await;

        match result {
            Ok(_) => {
                // Commit the transaction and record the time taken
                self.commit_transaction(transaction).await?;
                self.metrics.observe_request(
                    SQL_COMPONENT,
                    TRANSACTION_ACTION,
                    "transaction_total",
                    operation_start_time,
                );
                Ok(())
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                self.metrics.observe_request(
                    SQL_COMPONENT,
                    TRANSACTION_ACTION,
                    "transaction_failed_total",
                    operation_start_time,
                );
                Err(e)
            }
        }
    }
    async fn load_from_dump_nfts(
        &self,
        assets_file_name: &str,
        creators_file_name: &str,
        authority_file_name: &str,
        metadata_file_name: &str,
        semaphore: Arc<NFTSemaphores>,
    ) -> Result<(), IndexDbError> {
        let Some(ref base_path) = self.base_dump_path else {
            return Err(IndexDbError::BadArgument(
                "base_dump_path is not set".to_string(),
            ));
        };
        let temp_postfix = Uuid::new_v4().to_string().replace('-', "");
        let mut copy_tasks: JoinSet<Result<(), IndexDbError>> = JoinSet::new();
        for (file_path, table, columns, semaphore) in [
            ( base_path.join(creators_file_name),
                "asset_creators_v3",
                "asc_pubkey, asc_creator, asc_verified, asc_slot_updated",
                semaphore.creators.clone(),
            ),
            (
                base_path.join(authority_file_name),
                "assets_authorities",
                "auth_pubkey, auth_authority, auth_slot_updated",
                semaphore.authority.clone(),
            ),
            (base_path.join(assets_file_name),
                "assets_v3",
                "ast_pubkey, ast_specification_version, ast_specification_asset_class, ast_royalty_target_type, ast_royalty_amount, ast_slot_created, ast_owner_type, ast_owner, ast_delegate, ast_authority_fk, ast_collection, ast_is_collection_verified, ast_is_burnt, ast_is_compressible, ast_is_compressed, ast_is_frozen, ast_supply, ast_metadata_url_id, ast_slot_updated",
                semaphore.assets.clone(),
            ),
        ]{
            let cl = self.clone();
            copy_tasks.spawn(async move {
                cl.load_from(file_path.to_str().unwrap_or_default().to_string(), table, columns, semaphore).await
            });
        }

        let file_path = base_path
            .join(metadata_file_name)
            .to_str()
            .unwrap_or_default()
            .to_string();
        let temp_postfix = temp_postfix.clone();
        let cl = self.clone();
        let semaphore = semaphore.metadata.clone();
        copy_tasks.spawn(async move {
            cl.load_through_temp_table(
                file_path,
                "tasks",
                temp_postfix.as_str(),
                "tsk_id, tsk_metadata_url, tsk_status",
                true,
                Some(semaphore),
            )
            .await
        });
        while let Some(task) = copy_tasks.join_next().await {
            task??;
        }
        Ok(())
    }

    async fn load_from_dump_fungibles(
        &self,
        fungible_tokens_path: &str,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Result<(), IndexDbError> {
        self.load_from(
            fungible_tokens_path.to_string(),
            "fungible_tokens",
            "fbt_pubkey, fbt_owner, fbt_asset, fbt_balance, fbt_slot_updated",
            semaphore,
        )
        .await
    }

    async fn destructive_prep_to_batch_nft_load(&self) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self
            .destructive_prep_to_batch_nft_load_tx(&mut transaction)
            .await
        {
            Ok(_) => {
                self.commit_transaction(transaction).await?;
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                return Err(e);
            }
        }
        Ok(())
    }
    async fn finalize_batch_nft_load(&self) -> Result<(), IndexDbError> {
        let mut wg = JoinSet::new();
        let v = self.clone();
        wg.spawn(async move { v.finalize_assets_load().await });
        let v = self.clone();
        wg.spawn(async move { v.finalize_creators_load().await });
        let v = self.clone();
        wg.spawn(async move { v.finalize_authorities_load().await });
        while let Some(task) = wg.join_next().await {
            task??;
        }
        Ok(())
    }

    async fn destructive_prep_to_batch_fungible_load(&self) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self
            .destructive_prep_to_batch_fungible_load_tx(&mut transaction)
            .await
        {
            Ok(_) => {
                self.commit_transaction(transaction).await?;
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                return Err(e);
            }
        }
        Ok(())
    }
    async fn finalize_batch_fungible_load(&self) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self.finalize_batch_fungible_load_tx(&mut transaction).await {
            Ok(_) => {
                self.commit_transaction(transaction).await?;
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn update_last_synced_key(
        &self,
        last_key: &[u8],
        asset_type: AssetType,
    ) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self
            .update_last_synced_key(last_key, &mut transaction, "last_synced_key", asset_type)
            .await
        {
            Ok(_) => {
                self.commit_transaction(transaction).await?;
                Ok(())
            }
            Err(e) => {
                self.rollback_transaction(transaction).await?;
                Err(e)
            }
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
struct TaskIdRawResponse {
    pub(crate) tsk_id: Vec<u8>,
}

#[derive(sqlx::FromRow, Debug)]
struct CreatorRawResponse {
    pub(crate) asc_pubkey: Vec<u8>,
    pub(crate) asc_creator: Vec<u8>,
    pub(crate) asc_verified: bool,
    pub(crate) asc_slot_updated: i64,
}
pub struct CreatorsUpdates {
    pub new_or_updated: Vec<(Pubkey, Creator, i64)>,
    pub to_remove: Vec<(Pubkey, Creator)>,
}

impl PgClient {
    pub async fn get_existing_metadata_keys(&self) -> Result<HashSet<Vec<u8>>, IndexDbError> {
        let operation_start_time = chrono::Utc::now();

        // Start the transaction
        let mut transaction = self.start_transaction().await?;

        // Call the transactional logic method
        let result = self
            .get_existing_metadata_keys_logic(&mut transaction)
            .await;
        // Roll back the transaction (since we only used it for the cursor)
        self.rollback_transaction(transaction).await?;
        self.metrics.observe_request(
            SQL_COMPONENT,
            TRANSACTION_ACTION,
            "transaction_total",
            operation_start_time,
        );
        result
    }

    // The transactional logic is encapsulated here
    async fn get_existing_metadata_keys_logic(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<HashSet<Vec<u8>>, IndexDbError> {
        let mut set = HashSet::new();

        // Declare the cursor
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "DECLARE all_tasks CURSOR FOR SELECT tsk_id FROM tasks WHERE tsk_id IS NOT NULL",
        );
        self.execute_query_with_metrics(transaction, &mut query_builder, CREATE_ACTION, "cursor")
            .await?;

        // Fetch rows in a loop
        loop {
            let mut query_builder: QueryBuilder<'_, Postgres> =
                QueryBuilder::new("FETCH 10000 FROM all_tasks");
            let query = query_builder.build_query_as::<TaskIdRawResponse>();
            let rows = query.fetch_all(&mut *transaction).await.map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "FETCH_CURSOR");
                IndexDbError::QueryExecErr(e)
            })?;

            // If no rows were fetched, we are done
            if rows.is_empty() {
                break;
            }

            for row in rows {
                set.insert(row.tsk_id);
            }
        }

        // Close the cursor
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("CLOSE all_tasks");
        self.execute_query_with_metrics(transaction, &mut query_builder, DROP_ACTION, "cursor")
            .await?;

        Ok(set)
    }

    async fn insert_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        creators: &[(Pubkey, Creator, i64)],
        table: &str,
    ) -> Result<(), IndexDbError> {
        if creators.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("INSERT INTO ");
        query_builder.push(table);
        query_builder.push(" (asc_pubkey, asc_creator, asc_verified, asc_slot_updated) ");
        query_builder.push_values(creators, |mut builder, (pubkey, creator, slot_updated)| {
            builder
                .push_bind(pubkey.to_bytes().to_vec())
                .push_bind(creator.creator.to_bytes().to_vec())
                .push_bind(creator.creator_verified)
                .push_bind(slot_updated);
        });
        query_builder.push(" ON CONFLICT (asc_creator, asc_pubkey) DO UPDATE SET asc_verified = EXCLUDED.asc_verified WHERE ");
        query_builder.push(table);
        query_builder.push(".asc_slot_updated <= EXCLUDED.asc_slot_updated;");

        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            BATCH_UPSERT_ACTION,
            table,
        )
        .await?;
        Ok(())
    }

    async fn delete_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        removed_creators: &[(Pubkey, Creator)],
        table: &str,
    ) -> Result<(), IndexDbError> {
        if removed_creators.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("DELETE FROM ");
        query_builder.push(table);
        query_builder.push(" WHERE (asc_creator, asc_pubkey) IN ");

        query_builder.push_tuples(removed_creators, |mut builder, (pubkey, creator)| {
            builder
                .push_bind(creator.creator.to_bytes())
                .push_bind(pubkey.to_bytes());
        });
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            BATCH_DELETE_ACTION,
            table,
        )
        .await?;
        Ok(())
    }

    async fn insert_assets(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        asset_indexes: &[AssetIndex],
        table: &str,
    ) -> Result<(), IndexDbError> {
        if asset_indexes.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("INSERT INTO ");
        query_builder.push(table);
        query_builder.push(
            " (
            ast_pubkey,
            ast_specification_version,
            ast_specification_asset_class,
            ast_royalty_target_type,
            ast_royalty_amount,
            ast_slot_created,
            ast_owner_type,
            ast_owner,
            ast_delegate,
            ast_authority_fk,
            ast_collection,
            ast_is_collection_verified,
            ast_is_burnt,
            ast_is_compressible,
            ast_is_compressed,
            ast_is_frozen,
            ast_supply,
            ast_metadata_url_id,
            ast_slot_updated) ",
        );
        query_builder.push_values(asset_indexes, |mut builder, asset_index| {
            let metadata_id = asset_index
                .metadata_url
                .as_ref()
                .map(|u| u.get_metadata_id());
            builder
                .push_bind(asset_index.pubkey.to_bytes().to_vec())
                .push_bind(SpecificationVersions::from(
                    asset_index.specification_version,
                ))
                .push_bind(SpecificationAssetClass::from(
                    asset_index.specification_asset_class,
                ))
                .push_bind(RoyaltyTargetType::from(asset_index.royalty_target_type))
                .push_bind(asset_index.royalty_amount)
                .push_bind(asset_index.slot_created)
                .push_bind(asset_index.owner_type.map(OwnerType::from))
                .push_bind(asset_index.owner.map(|owner| owner.to_bytes().to_vec()))
                .push_bind(asset_index.delegate.map(|k| k.to_bytes().to_vec()))
                .push_bind(if let Some(collection) = asset_index.collection {
                    if asset_index.update_authority.is_some() {
                        Some(collection.to_bytes().to_vec())
                    } else if asset_index.authority.is_some() {
                        Some(asset_index.pubkey.to_bytes().to_vec())
                    } else {
                        None
                    }
                } else if asset_index.authority.is_some() {
                    Some(asset_index.pubkey.to_bytes().to_vec())
                } else {
                    None
                })
                .push_bind(asset_index.collection.map(|k| k.to_bytes().to_vec()))
                .push_bind(asset_index.is_collection_verified)
                .push_bind(asset_index.is_burnt)
                .push_bind(asset_index.is_compressible)
                .push_bind(asset_index.is_compressed)
                .push_bind(asset_index.is_frozen)
                .push_bind(asset_index.supply)
                .push_bind(metadata_id)
                .push_bind(asset_index.slot_updated);
        });
        query_builder.push(
            " ON CONFLICT (ast_pubkey)
        DO UPDATE SET
            ast_specification_version = EXCLUDED.ast_specification_version,
            ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
            ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
            ast_royalty_amount = EXCLUDED.ast_royalty_amount,
            ast_slot_created = EXCLUDED.ast_slot_created,
            ast_owner_type = EXCLUDED.ast_owner_type,
            ast_owner = EXCLUDED.ast_owner,
            ast_delegate = EXCLUDED.ast_delegate,
            ast_authority_fk = EXCLUDED.ast_authority_fk,
            ast_collection = EXCLUDED.ast_collection,
            ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
            ast_is_burnt = EXCLUDED.ast_is_burnt,
            ast_is_compressible = EXCLUDED.ast_is_compressible,
            ast_is_compressed = EXCLUDED.ast_is_compressed,
            ast_is_frozen = EXCLUDED.ast_is_frozen,
            ast_supply = EXCLUDED.ast_supply,
            ast_metadata_url_id = EXCLUDED.ast_metadata_url_id,
            ast_slot_updated = EXCLUDED.ast_slot_updated
            WHERE ",
        );
        query_builder.push(table);
        query_builder.push(".ast_slot_updated <= EXCLUDED.ast_slot_updated OR ");
        query_builder.push(table);
        query_builder.push(".ast_slot_updated IS NULL;");

        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            BATCH_UPSERT_ACTION,
            table,
        )
        .await?;
        Ok(())
    }

    async fn insert_authorities(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        authorities: &[Authority],
        table: &str,
    ) -> Result<(), IndexDbError> {
        if authorities.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("INSERT INTO ");
        query_builder.push(table);
        query_builder.push(
            " (
            auth_pubkey,
            auth_authority,
            auth_slot_updated) ",
        );
        query_builder.push_values(authorities, |mut builder, authority| {
            builder
                .push_bind(authority.key.to_bytes().to_vec())
                .push_bind(authority.authority.to_bytes().to_vec())
                .push_bind(authority.slot_updated);
        });
        query_builder.push(
            " ON CONFLICT (auth_pubkey)
        DO UPDATE SET
            auth_authority = EXCLUDED.auth_authority, auth_slot_updated = EXCLUDED.auth_slot_updated
            WHERE ",
        );
        query_builder.push(table);
        query_builder.push(".auth_slot_updated <= EXCLUDED.auth_slot_updated OR ");
        query_builder.push(table);
        query_builder.push(".auth_slot_updated IS NULL;");

        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            BATCH_UPSERT_ACTION,
            table,
        )
        .await?;
        Ok(())
    }

    async fn insert_fungible_tokens(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        fungible_tokens: &[FungibleToken],
        table: &str,
    ) -> Result<(), IndexDbError> {
        if fungible_tokens.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("INSERT INTO ");
        query_builder.push(table);
        query_builder.push(
            " (
            fbt_pubkey,
            fbt_owner,
            fbt_asset,
            fbt_balance,
            fbt_slot_updated
            ) ",
        );
        query_builder.push_values(fungible_tokens, |mut builder, fungible_token| {
            builder
                .push_bind(fungible_token.key.to_bytes().to_vec())
                .push_bind(fungible_token.owner.to_bytes().to_vec())
                .push_bind(fungible_token.asset.to_bytes().to_vec())
                .push_bind(fungible_token.balance)
                .push_bind(fungible_token.slot_updated);
        });
        query_builder.push(
            " ON CONFLICT (fbt_pubkey) DO UPDATE SET
            fbt_balance = EXCLUDED.fbt_balance, fbt_slot_updated = EXCLUDED.fbt_slot_updated, fbt_owner = EXCLUDED.fbt_owner
            WHERE ",
        );
        query_builder.push(table);
        query_builder.push(".fbt_slot_updated <= EXCLUDED.fbt_slot_updated OR ");
        query_builder.push(table);
        query_builder.push(".fbt_slot_updated IS NULL;");

        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            BATCH_UPSERT_ACTION,
            table,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn batch_get_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        pubkeys: &[Vec<u8>],
        table: &str,
    ) -> Result<Vec<(Pubkey, Creator, i64)>, IndexDbError> {
        if pubkeys.is_empty() {
            return Ok(vec![]);
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "SELECT asc_pubkey, asc_creator, asc_verified, asc_slot_updated FROM ",
        );
        query_builder.push(table);
        query_builder.push(" WHERE asc_pubkey in (");
        let pubkey_len = pubkeys.len();
        for (i, k) in pubkeys.iter().enumerate() {
            query_builder.push_bind(k);
            if i < pubkey_len - 1 {
                query_builder.push(",");
            }
        }
        query_builder.push(");");
        let query = query_builder.build_query_as::<CreatorRawResponse>();
        let start_time = chrono::Utc::now();
        let creators_result = query.fetch_all(transaction).await.inspect_err(|_err| {
            self.metrics
                .observe_error(SQL_COMPONENT, BATCH_SELECT_ACTION, table);
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, BATCH_SELECT_ACTION, table, start_time);
        Ok(creators_result
            .iter()
            .map(|c| {
                (
                    Pubkey::try_from(c.asc_pubkey.clone()).unwrap(),
                    Creator {
                        creator: Pubkey::try_from(c.asc_creator.clone()).unwrap(),
                        creator_verified: c.asc_verified,
                        creator_share: 0,
                    },
                    c.asc_slot_updated,
                )
            })
            .collect())
    }

    pub fn diff(
        all_creators: Vec<(Pubkey, Creator, i64)>,
        existing_creators: Vec<(Pubkey, Creator)>,
    ) -> CreatorsUpdates {
        let mut existing_map: HashMap<Pubkey, Vec<Creator>> = HashMap::new();
        for (pubkey, creator) in existing_creators.clone() {
            existing_map.entry(pubkey).or_default().push(creator);
        }

        let mut active_creators_set: HashSet<(Pubkey, Pubkey)> = HashSet::new();
        for (pubkey, creator, _slot) in all_creators.iter() {
            active_creators_set.insert((*pubkey, creator.creator));
        }

        let mut new_or_updated = Vec::new();

        let to_remove = existing_creators
            .into_iter()
            .filter(|(pubkey, creator)| !active_creators_set.contains(&(*pubkey, creator.creator)))
            .to_owned()
            .collect();
        for (pubkey, creator, slot) in all_creators {
            let existing_creators = existing_map.get(&pubkey);

            match existing_creators {
                Some(creators)
                    if !creators.iter().any(|c| {
                        c.creator == creator.creator
                            && c.creator_verified == creator.creator_verified
                    }) =>
                {
                    new_or_updated.push((pubkey, creator, slot))
                }
                None => new_or_updated.push((pubkey, creator, slot)),
                _ => (),
            }
        }
        CreatorsUpdates {
            new_or_updated,
            to_remove,
        }
    }

    pub(crate) async fn update_last_synced_key(
        &self,
        last_key: &[u8],
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
        asset_type: AssetType,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("UPDATE ");
        query_builder.push(table);
        query_builder.push(" SET last_synced_asset_update_key = ");
        query_builder
            .push_bind(last_key)
            .push(format!(" WHERE id = {}", asset_type as i32));
        self.execute_query_with_metrics(transaction, &mut query_builder, UPDATE_ACTION, table)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a creator
    fn create_creator(creator_id: u8, verified: bool) -> Creator {
        Creator {
            creator: create_pubkey(creator_id),
            creator_verified: verified,
            creator_share: 100,
        }
    }

    // Helper function to create a pubkey
    fn create_pubkey(id: u8) -> Pubkey {
        Pubkey::new_from_array([id; 32])
    }

    #[test]
    fn test_all_creators_are_new() {
        let all_creators = vec![
            (create_pubkey(1), create_creator(1, true), 1000),
            (create_pubkey(2), create_creator(2, false), 2000),
        ];
        let existing_creators = vec![];

        let updates = PgClient::diff(all_creators.clone(), existing_creators);

        assert_eq!(updates.new_or_updated, all_creators);
        assert!(updates.to_remove.is_empty());
    }
    #[test]
    fn test_all_existing_creators_are_removed() {
        let all_creators = vec![];
        let existing_creators = vec![
            (create_pubkey(1), create_creator(1, true)),
            (create_pubkey(2), create_creator(2, false)),
        ];

        let updates = PgClient::diff(all_creators, existing_creators.clone());

        assert!(updates.new_or_updated.is_empty());
        assert_eq!(updates.to_remove, existing_creators);
    }
    #[test]
    fn test_all_creators_are_replaced() {
        let all_creators = vec![
            (create_pubkey(3), create_creator(3, true), 3000),
            (create_pubkey(4), create_creator(4, false), 4000),
        ];
        let existing_creators = vec![
            (create_pubkey(1), create_creator(1, true)),
            (create_pubkey(2), create_creator(2, false)),
        ];

        let updates = PgClient::diff(all_creators.clone(), existing_creators);

        assert_eq!(updates.new_or_updated, all_creators);
        assert_eq!(updates.to_remove.len(), 2); // Assuming removed creators are (1, true) and (2, false)
    }
    #[test]
    fn test_some_creators_changed_verification() {
        let all_creators = vec![
            (create_pubkey(1), create_creator(1, false), 5000), // Changed
            (create_pubkey(2), create_creator(2, false), 6000), // Unchanged
        ];
        let existing_creators = vec![
            (create_pubkey(1), create_creator(1, true)),
            (create_pubkey(2), create_creator(2, false)),
        ];

        let updates = PgClient::diff(all_creators.clone(), existing_creators);

        assert_eq!(updates.new_or_updated.len(), 1); // Assuming only creator (1, false) is new/updated
        assert!(updates.to_remove.is_empty()); // No creators are removed
    }

    #[test]
    fn test_no_changes_made() {
        let all_creators = vec![
            (create_pubkey(1), create_creator(1, true), 7000),
            (create_pubkey(2), create_creator(2, false), 8000),
        ];
        let existing_creators = all_creators
            .clone()
            .into_iter()
            .map(|(pk, c, _)| (pk, c))
            .collect();

        let updates = PgClient::diff(all_creators, existing_creators);

        assert!(updates.new_or_updated.is_empty());
        assert!(updates.to_remove.is_empty());
    }
}
