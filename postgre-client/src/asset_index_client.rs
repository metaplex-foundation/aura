use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Postgres, QueryBuilder};

use crate::{
    model::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions},
    storage_traits::AssetIndexStorage,
    PgClient,
};
use entities::models::{AssetIndex, Creator};

#[async_trait]
impl AssetIndexStorage for PgClient {
    async fn fetch_last_synced_id(&self) -> Result<Option<Vec<u8>>, String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "SELECT last_synced_asset_update_key FROM last_synced_key WHERE id = 1",
        );

        let query = query_builder.build_query_as::<(Option<Vec<u8>>,)>();
        let result = query
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(result.0)
    }

    async fn update_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
        last_key: &[u8],
    ) -> Result<(), String> {
        let mut transaction = self.pool.begin().await.map_err(|e| e.to_string())?;

        // First we need to bulk upsert metadata_url into metadata and get back ids for each metadata_url to upsert into assets_v3
        let mut metadata_urls: Vec<String> = asset_indexes
            .iter()
            .filter_map(|asset_index| asset_index.metadata_url.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let mut metadata_url_map: HashMap<String, i64> = HashMap::new();

        if !metadata_urls.is_empty() {
            metadata_urls.sort();
            let mut query_builder: QueryBuilder<'_, Postgres> =
                QueryBuilder::new("INSERT INTO metadata (mtd_url) ");
            query_builder.push_values(metadata_urls.iter(), |mut builder, metadata_url| {
                builder.push_bind(metadata_url);
            });
            query_builder.push(" ON CONFLICT (mtd_url) DO NOTHING RETURNING mtd_id, mtd_url;");
            let query = query_builder.build_query_as::<(i64, String)>();
            let metadata_ids = query
                .fetch_all(&mut transaction)
                .await
                .map_err(|e| e.to_string())?;

            // convert metadata_ids to a map
            metadata_url_map = metadata_ids
                .iter()
                .map(|(id, url)| (url.clone(), *id))
                .collect::<HashMap<String, i64>>();
        }

        let mut asset_indexes = asset_indexes.to_vec();
        asset_indexes.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));

        // Collect all creators from all assets
        let mut all_creators: Vec<(Pubkey, Creator, i64)> = asset_indexes
            .iter()
            .flat_map(|asset_index| {
                asset_index.creators.iter().map(move |creator| {
                    (
                        asset_index.pubkey.clone(),
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
        let valid_creators_key_tupples = all_creators
            .iter()
            .map(|(pubkey, creator, _slot_updated)| {
                (
                    pubkey.to_bytes().to_vec(),
                    creator.creator.to_bytes().to_vec(),
                )
            })
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

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
            let metadata_id = match asset_index.metadata_url {
                Some(ref url) => metadata_url_map.get(url),
                None => None,
            };
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
                .push_bind(
                    asset_index
                        .owner_type
                        .map(|owner_type| OwnerType::from(owner_type)),
                )
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

        let query = query_builder.build();
        query
            .execute(&mut transaction)
            .await
            .map_err(|e| e.to_string())?;

        // Asset creators will be updated in 2 steps:
        // 1. Delete creators for the assets that are being updated and don't exist anymore
        // 2. Upsert creators for the assets
        // Delete creators for the assets that are being updated and don't exist anymore
        if !valid_creators_key_tupples.is_empty() {
            let mut query_builder: QueryBuilder<'_, Postgres> =
                QueryBuilder::new("DELETE FROM asset_creators_v3 WHERE asc_pubkey = ANY(");
            query_builder.push_bind(updated_keys);
            query_builder.push(") AND (asc_creator, asc_pubkey) NOT IN ");

            query_builder.push_tuples(
                valid_creators_key_tupples,
                |mut builder, (pubkey, creator)| {
                    builder.push_bind(creator).push_bind(pubkey);
                },
            );

            let query = query_builder.build();
            query
                .execute(&mut transaction)
                .await
                .map_err(|e| e.to_string())?;
        }

        // Bulk upsert for asset_creators_v3
        if !all_creators.is_empty() {
            let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                "INSERT INTO asset_creators_v3 (asc_pubkey, asc_creator, asc_verified, asc_slot_updated) ",
            );
            query_builder.push_values(
                all_creators.iter(),
                |mut builder, (pubkey, creator, slot_updated)| {
                    builder
                        .push_bind(pubkey.to_bytes().to_vec())
                        .push_bind(creator.creator.to_bytes().to_vec())
                        .push_bind(creator.creator_verified)
                        .push_bind(slot_updated);
                },
            );
            query_builder.push(" ON CONFLICT (asc_creator, asc_pubkey) DO UPDATE SET asc_verified = EXCLUDED.asc_verified WHERE asset_creators_v3.asc_slot_updated <= EXCLUDED.asc_slot_updated;");

            let query = query_builder.build();
            query
                .execute(&mut transaction)
                .await
                .map_err(|e| e.to_string())?;
        }

        // Update last_synced_key
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE last_synced_key SET last_synced_asset_update_key = ");
        query_builder.push_bind(last_key).push(" WHERE id = 1");

        let query = query_builder.build();
        query
            .execute(&mut transaction)
            .await
            .map_err(|e| e.to_string())?;

        transaction.commit().await.map_err(|e| e.to_string())?;

        Ok(())
    }
}
