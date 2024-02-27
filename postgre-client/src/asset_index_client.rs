use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::{
    model::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    storage_traits::AssetIndexStorage,
    PgClient, BATCH_DELETE_ACTION, BATCH_SELECT_ACTION, BATCH_UPSERT_ACTION, CREATE_ACTION,
    DROP_ACTION, SELECT_ACTION, SQL_COMPONENT, UPDATE_ACTION,
};
use entities::models::{AssetIndex, Creator, UrlWithStatus};

#[async_trait]
impl AssetIndexStorage for PgClient {
    async fn fetch_last_synced_id(&self) -> Result<Option<Vec<u8>>, String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "SELECT last_synced_asset_update_key FROM last_synced_key WHERE id = 1",
        );
        let start_time = chrono::Utc::now();
        let query = query_builder.build_query_as::<(Option<Vec<u8>>,)>();
        let result = query.fetch_one(&self.pool).await.map_err(|e| {
            self.metrics
                .observe_error(SQL_COMPONENT, SELECT_ACTION, "last_synced_key");
            e.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, "last_synced_key", start_time);
        Ok(result.0)
    }

    async fn update_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
        last_key: &[u8],
    ) -> Result<(), String> {
        let mut transaction = self.start_transaction().await?;

        // First we need to bulk upsert metadata_url into metadata and get back ids for each metadata_url to upsert into assets_v3
        let mut metadata_urls: Vec<_> = asset_indexes
            .iter()
            .filter_map(|asset_index| asset_index.metadata_url.clone())
            .collect::<HashSet<_>>()
            .iter()
            .map(|url| UrlWithStatus::new(url.metadata_url.as_str(), url.is_downloaded))
            .collect();

        if !metadata_urls.is_empty() {
            metadata_urls.sort_by(|a, b| a.metadata_url.cmp(&b.metadata_url));
            tracing::info!("Inserting metadata urls: {:?}", metadata_urls);
            self.insert_tasks(&mut transaction, metadata_urls.clone())
                .await?;
        }

        let mut asset_indexes = asset_indexes.to_vec();
        asset_indexes.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));

        // Collect all creators from all assets
        let mut all_creators: Vec<(Pubkey, Creator, i64)> = asset_indexes
            .iter()
            .flat_map(|asset_index| {
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

        let updated_keys = asset_indexes
            .iter()
            .map(|asset_index| asset_index.pubkey.to_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        tracing::info!("Inserting asset indexes: {:?}", asset_indexes);
        // Bulk insert/update for assets_v3
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "INSERT INTO assets_v3 (
            ast_pubkey,
            ast_specification_version,
            ast_specification_asset_class,
            ast_royalty_target_type,
            ast_royalty_amount,
            ast_slot_created,
            ast_owner_type,
            ast_owner,
            ast_delegate,
            ast_authority,
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
            let metadata_id = asset_index.metadata_url.map(|u| u.get_metadata_id());
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
                .push_bind(asset_index.authority.map(|k| k.to_bytes().to_vec()))
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
        query_builder.push(" ON CONFLICT (ast_pubkey) 
        DO UPDATE SET 
            ast_specification_version = EXCLUDED.ast_specification_version,
            ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
            ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
            ast_royalty_amount = EXCLUDED.ast_royalty_amount,
            ast_slot_created = EXCLUDED.ast_slot_created,
            ast_owner_type = EXCLUDED.ast_owner_type,
            ast_owner = EXCLUDED.ast_owner,
            ast_delegate = EXCLUDED.ast_delegate,
            ast_authority = EXCLUDED.ast_authority,
            ast_collection = EXCLUDED.ast_collection,
            ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
            ast_is_burnt = EXCLUDED.ast_is_burnt,
            ast_is_compressible = EXCLUDED.ast_is_compressible,
            ast_is_compressed = EXCLUDED.ast_is_compressed,
            ast_is_frozen = EXCLUDED.ast_is_frozen,
            ast_supply = EXCLUDED.ast_supply,
            ast_metadata_url_id = EXCLUDED.ast_metadata_url_id,
            ast_slot_updated = EXCLUDED.ast_slot_updated
            WHERE assets_v3.ast_slot_updated <= EXCLUDED.ast_slot_updated OR assets_v3.ast_slot_updated IS NULL;");

        self.execute_query_with_metrics(
            &mut transaction,
            &mut query_builder,
            BATCH_UPSERT_ACTION,
            "assets_v3",
        )
        .await?;

        // Asset creators will be updated in 2 steps:
        // 1. Delete creators for the assets that are being updated and don't exist anymore
        // 2. Upsert creators for the assets
        // Delete creators for the assets that are being updated and don't exist anymore
        let existing_creators = self
            .batch_get_creators(&mut transaction, updated_keys.clone())
            .await?;
        let creator_updates = Self::diff(all_creators.clone(), existing_creators);

        if !creator_updates.to_remove.is_empty() {
            let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                "DELETE FROM asset_creators_v3 WHERE (asc_creator, asc_pubkey) IN ",
            );

            query_builder.push_tuples(
                creator_updates.to_remove,
                |mut builder, (pubkey, creator)| {
                    builder
                        .push_bind(creator.creator.to_bytes())
                        .push_bind(pubkey.to_bytes());
                },
            );
            self.execute_query_with_metrics(
                &mut transaction,
                &mut query_builder,
                BATCH_DELETE_ACTION,
                "asset_creators_v3",
            )
            .await?;
        }

        // Bulk upsert for asset_creators_v3
        if !creator_updates.new_or_updated.is_empty() {
            let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                "INSERT INTO asset_creators_v3 (asc_pubkey, asc_creator, asc_verified, asc_slot_updated) ",
            );
            query_builder.push_values(
                creator_updates.new_or_updated.iter(),
                |mut builder, (pubkey, creator, slot_updated)| {
                    builder
                        .push_bind(pubkey.to_bytes().to_vec())
                        .push_bind(creator.creator.to_bytes().to_vec())
                        .push_bind(creator.creator_verified)
                        .push_bind(slot_updated);
                },
            );
            query_builder.push(" ON CONFLICT (asc_creator, asc_pubkey) DO UPDATE SET asc_verified = EXCLUDED.asc_verified WHERE asset_creators_v3.asc_slot_updated <= EXCLUDED.asc_slot_updated;");

            self.execute_query_with_metrics(
                &mut transaction,
                &mut query_builder,
                BATCH_UPSERT_ACTION,
                "asset_creators_v3",
            )
            .await?;
        }

        self.update_last_synced_key(last_key, &mut transaction)
            .await?;
        // Update last_synced_key
        self.commit_transaction(transaction).await
    }

    async fn load_from_dump(
        &self,
        base_path: &std::path::Path,
        last_key: &[u8],
    ) -> Result<(), String> {
        let Some(metadata_path) = base_path.join("metadata.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };
        let Some(creators_path) = base_path.join("creators.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };
        let Some(assets_path) = base_path.join("assets.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };
        let mut transaction = self.start_transaction().await?;

        self.copy_all(metadata_path, creators_path, assets_path, &mut transaction)
            .await?;
        self.update_last_synced_key(last_key, &mut transaction)
            .await?;
        self.commit_transaction(transaction).await?;
        Ok(())
    }

    async fn get_existing_metadata_keys(&self) -> Result<HashSet<Vec<u8>>, String> {
        let mut set = HashSet::new();
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "DECLARE all_tasks CURSOR FOR SELECT tsk_id FROM tasks WHERE tsk_id IS NOT NULL",
        );
        self.execute_query_with_metrics(&mut tx, &mut query_builder, CREATE_ACTION, "cursor")
            .await?;
        loop {
            let mut query_builder: QueryBuilder<'_, Postgres> =
                QueryBuilder::new("FETCH 10000 FROM all_tasks");
            // Fetch a batch of rows from the cursor
            let query = query_builder.build_query_as::<TaskIdRawResponse>();
            let rows = query.fetch_all(&mut tx).await.map_err(|e| e.to_string())?;

            // If no rows were fetched, we are done
            if rows.is_empty() {
                break;
            }

            for row in rows {
                set.insert(row.tsk_id);
            }
        }
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("CLOSE all_tasks");
        self.execute_query_with_metrics(&mut tx, &mut query_builder, DROP_ACTION, "cursor")
            .await?;
        self.rollback_transaction(tx).await?;
        Ok(set)
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
}
pub struct CreatorsUpdates {
    pub new_or_updated: Vec<(Pubkey, Creator, i64)>,
    pub to_remove: Vec<(Pubkey, Creator)>,
}

impl PgClient {
    async fn batch_get_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        pubkeys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Pubkey, Creator)>, String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("SELECT asc_pubkey, asc_creator, asc_verified FROM asset_creators_v3 WHERE asc_pubkey in (");
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
        let creators_result = query.fetch_all(transaction).await.map_err(|err| {
            self.metrics
                .observe_error(SQL_COMPONENT, BATCH_SELECT_ACTION, "asset_creators_v3");
            err.to_string()
        })?;
        self.metrics.observe_request(
            SQL_COMPONENT,
            BATCH_SELECT_ACTION,
            "asset_creators_v3",
            start_time,
        );
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

    async fn update_last_synced_key(
        &self,
        last_key: &[u8],
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE last_synced_key SET last_synced_asset_update_key = ");
        query_builder.push_bind(last_key).push(" WHERE id = 1");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            UPDATE_ACTION,
            "last_synced_key",
        )
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
