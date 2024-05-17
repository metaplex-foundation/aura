use std::str::FromStr;

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use entities::api_req_params::Options;
use solana_sdk::{bs58, pubkey::Pubkey};
use sqlx::{Postgres, QueryBuilder};

use crate::{
    error::IndexDbError,
    model::{
        AssetSortBy, AssetSortDirection, AssetSortedIndex, AssetSorting, AssetSupply,
        SearchAssetsFilter,
    },
    storage_traits::AssetPubkeyFilteredFetcher,
    PgClient, SELECT_ACTION, SQL_COMPONENT,
};

#[derive(sqlx::FromRow, Debug)]
struct AssetRawResponse {
    pub pubkey: Vec<u8>,
    pub slot_created: i64,
    pub slot_updated: i64,
}

impl PgClient {
    pub fn build_search_query<'a>(
        filter: &'a SearchAssetsFilter,
        order: &'a AssetSorting,
        limit: u64,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
        options: &'a Options,
    ) -> Result<(QueryBuilder<'a, Postgres>, bool), IndexDbError> {
        // todo: remove the inner join with tasks and only perform it if the metadata_url_id is present in the filter
        let mut query_builder = QueryBuilder::new(
            "SELECT ast_pubkey pubkey, ast_slot_created slot_created, ast_slot_updated slot_updated FROM assets_v3 ",
        );
        let mut group_clause_required = false;

        if filter.creator_address.is_some()
            || filter.creator_verified.is_some()
            || filter.royalty_target.is_some()
        {
            query_builder.push(" INNER JOIN asset_creators_v3 ON ast_pubkey = asc_pubkey ");
            group_clause_required = true;
        }
        if filter.json_uri.is_some() {
            query_builder.push(" INNER JOIN tasks ON ast_metadata_url_id = tsk_id ");
            group_clause_required = true;
        }

        // todo: if we implement the additional params like negata and all/any switch, the true part and the AND prefix should be refactored
        query_builder.push(" WHERE TRUE ");
        if let Some(spec_version) = &filter.specification_version {
            query_builder.push(" AND assets_v3.ast_specification_version = ");
            query_builder.push_bind(spec_version);
        }

        if let Some(asset_class) = &filter.specification_asset_class {
            query_builder.push(" AND assets_v3.ast_specification_asset_class = ");
            query_builder.push_bind(asset_class);
        }

        if let Some(owner_address) = &filter.owner_address {
            query_builder.push(" AND assets_v3.ast_owner = ");
            query_builder.push_bind(owner_address);
        }

        if let Some(owner_type) = &filter.owner_type {
            query_builder.push(" AND assets_v3.ast_owner_type = ");
            query_builder.push_bind(owner_type);
        }

        if let Some(creator_address) = &filter.creator_address {
            query_builder.push(" AND asset_creators_v3.asc_creator = ");
            query_builder.push_bind(creator_address);
        }

        if let Some(creator_verified) = filter.creator_verified {
            query_builder.push(" AND asset_creators_v3.asc_verified = ");
            query_builder.push_bind(creator_verified);
        }

        if let Some(authority) = &filter.authority_address {
            query_builder.push(" AND assets_v3.ast_authority = ");
            query_builder.push_bind(authority);
        }

        if let Some(collection) = &filter.collection {
            query_builder.push(" AND assets_v3.ast_collection = ");
            query_builder.push_bind(collection);
        }

        if !options.show_unverified_collections {
            query_builder.push(" AND assets_v3.ast_is_collection_verified = true");
        }

        if let Some(delegate) = &filter.delegate {
            query_builder.push(" AND assets_v3.ast_delegate = ");
            query_builder.push_bind(delegate);
        }

        if let Some(frozen) = filter.frozen {
            query_builder.push(" AND assets_v3.ast_is_frozen = ");
            query_builder.push_bind(frozen);
        }

        if let Some(supply) = &filter.supply {
            match supply {
                AssetSupply::Equal(s) => {
                    query_builder.push(" AND assets_v3.ast_supply = ");
                    query_builder.push_bind(*s as i64);
                }
                AssetSupply::Greater(s) => {
                    query_builder.push(" AND assets_v3.ast_supply > ");
                    query_builder.push_bind(*s as i64);
                }
            }
        }

        // supply_mint is identical to pubkey
        if let Some(supply_mint) = &filter.supply_mint {
            query_builder.push(" AND assets_v3.ast_pubkey = ");
            query_builder.push_bind(supply_mint);
        }

        if let Some(compressed) = filter.compressed {
            query_builder.push(" AND assets_v3.ast_is_compressed = ");
            query_builder.push_bind(compressed);
        }

        if let Some(compressible) = filter.compressible {
            query_builder.push(" AND assets_v3.ast_is_compressible = ");
            query_builder.push_bind(compressible);
        }

        if let Some(royalty_target_type) = &filter.royalty_target_type {
            query_builder.push(" AND assets_v3.ast_royalty_target_type = ");
            query_builder.push_bind(royalty_target_type);
        }

        if let Some(royalty_target) = &filter.royalty_target {
            query_builder.push(" AND asset_creators_v3.asc_creator = ");
            query_builder.push_bind(royalty_target);
        }

        if let Some(royalty_amount) = filter.royalty_amount {
            query_builder.push(" AND assets_v3.ast_royalty_amount = ");
            query_builder.push_bind(royalty_amount as i64);
        }

        if let Some(burnt) = filter.burnt {
            query_builder.push(" AND assets_v3.ast_is_burnt = ");
            query_builder.push_bind(burnt);
        }

        if let Some(json_uri) = &filter.json_uri {
            query_builder.push(" AND tsk_metadata_url = ");
            query_builder.push_bind(json_uri);
        }

        let order_reversed = before.is_some() && after.is_none();

        match &order.sort_by {
            AssetSortBy::SlotCreated | AssetSortBy::SlotUpdated => {
                if let Some(before) = before {
                    let comparison = match order.sort_direction {
                        AssetSortDirection::Asc => " < ",
                        AssetSortDirection::Desc => " > ",
                    };

                    add_slot_and_key_comparison(
                        before.as_ref(),
                        comparison,
                        &order.sort_by,
                        &mut query_builder,
                    )?;
                }

                if let Some(after) = after {
                    let comparison = match order.sort_direction {
                        AssetSortDirection::Asc => " > ",
                        AssetSortDirection::Desc => " < ",
                    };

                    add_slot_and_key_comparison(
                        after.as_ref(),
                        comparison,
                        &order.sort_by,
                        &mut query_builder,
                    )?;
                }
            }
            AssetSortBy::Key => {
                if let Some(before) = before {
                    let comparison = match order.sort_direction {
                        AssetSortDirection::Asc => " < ",
                        AssetSortDirection::Desc => " > ",
                    };

                    add_key_comparison(
                        before.as_ref(),
                        comparison,
                        &order.sort_by,
                        &mut query_builder,
                    )?;
                }

                if let Some(after) = after {
                    let comparison = match order.sort_direction {
                        AssetSortDirection::Asc => " > ",
                        AssetSortDirection::Desc => " < ",
                    };

                    add_key_comparison(
                        after.as_ref(),
                        comparison,
                        &order.sort_by,
                        &mut query_builder,
                    )?;
                }
            }
        }

        // Add GROUP BY clause if necessary
        if group_clause_required {
            query_builder.push(" GROUP BY assets_v3.ast_pubkey, assets_v3.ast_slot_created, assets_v3.ast_slot_updated ");
        }

        // Add ORDER BY clause
        let direction = match (&order.sort_direction, order_reversed) {
            (AssetSortDirection::Asc, true) | (AssetSortDirection::Desc, false) => " DESC ",
            (AssetSortDirection::Asc, false) | (AssetSortDirection::Desc, true) => " ASC ",
        };

        query_builder.push(" ORDER BY ");
        query_builder.push(order.sort_by.to_string());
        query_builder.push(direction);
        query_builder.push(", ast_pubkey ");
        query_builder.push(direction);
        // Add LIMIT clause
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit as i64);

        // Add OFFSET clause if page is provided
        if let Some(page_num) = page {
            if page_num > 0 {
                let offset = (page_num.saturating_sub(1)) * limit; // Prevent underflow
                query_builder.push(" OFFSET ");
                query_builder.push_bind(offset as i64);
            }
        }
        Ok((query_builder, order_reversed))
    }
}

fn add_slot_and_key_comparison(
    key: &str,
    comparison: &str,
    order_field: &AssetSortBy,
    query_builder: &mut QueryBuilder<'_, Postgres>,
) -> Result<(), IndexDbError> {
    let res = AssetRawResponse::decode_sorting_key(key);
    // TODO-XXX: is that OK that we just ignore the error from the call above?

    if let Ok((slot, pubkey)) = res {
        let order_field = order_field.to_string();

        query_builder.push(format!(" AND ({}{}", order_field, comparison));
        query_builder.push_bind(slot);
        query_builder.push(format!(" OR ({} = ", order_field));
        query_builder.push_bind(slot);
        query_builder.push(format!(" AND ast_pubkey {}", comparison));
        query_builder.push_bind(pubkey);
        query_builder.push("))");
    }

    Ok(())
}

fn add_key_comparison(
    key: &str,
    comparison: &str,
    order_field: &AssetSortBy,
    query_builder: &mut QueryBuilder<'_, Postgres>,
) -> Result<(), IndexDbError> {
    let after =
        Pubkey::from_str(key).map_err(|_| IndexDbError::PubkeyParsingError(key.to_string()))?;

    query_builder.push(format!(" AND ({}{}", order_field, comparison));
    query_builder.push_bind(after.to_bytes());
    query_builder.push(")");

    Ok(())
}

#[async_trait]
impl AssetPubkeyFilteredFetcher for PgClient {
    // TODO: replace String with something that reflexes arguments and Postgre related problems
    async fn get_asset_pubkeys_filtered(
        &self,
        filter: &SearchAssetsFilter,
        order: &AssetSorting,
        limit: u64,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
        options: &Options,
    ) -> Result<Vec<AssetSortedIndex>, IndexDbError> {
        let (mut query_builder, order_reversed) =
            Self::build_search_query(filter, order, limit, page, before, after, options)?;
        let query = query_builder.build_query_as::<AssetRawResponse>();
        let start_time = chrono::Utc::now();
        let result = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e: sqlx::Error| {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "assets_v3");
                e
            })?;
        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, "assets_v3", start_time);
        let r = result
            .into_iter()
            .map(|r| AssetSortedIndex::from((&r, &order.sort_by)));
        if order_reversed {
            Ok(r.rev().collect())
        } else {
            Ok(r.collect())
        }
    }
}

impl AssetRawResponse {
    pub fn encode_sorting_key(&self, sort_by: &AssetSortBy) -> String {
        match sort_by {
            AssetSortBy::SlotCreated => {
                let mut key = self.slot_created.to_be_bytes().to_vec();
                key.extend_from_slice(&self.pubkey);
                general_purpose::STANDARD_NO_PAD.encode(key)
            }
            AssetSortBy::SlotUpdated => {
                let mut key = self.slot_updated.to_be_bytes().to_vec();
                key.extend_from_slice(&self.pubkey);
                general_purpose::STANDARD_NO_PAD.encode(key)
            }
            AssetSortBy::Key => bs58::encode(&self.pubkey).into_string(),
        }
    }

    pub fn decode_sorting_key(encoded_key: &str) -> Result<(i64, Vec<u8>), IndexDbError> {
        let key = match general_purpose::STANDARD_NO_PAD.decode(encoded_key) {
            Ok(k) => k,
            Err(_) => return Err(IndexDbError::Base64DecodingErr),
        };

        if key.len() < 8 {
            return Err(IndexDbError::InvalidSortingKeyErr);
        }

        let slot = i64::from_be_bytes(key[0..8].try_into().unwrap());
        let pubkey = key[8..].to_vec();
        Ok((slot, pubkey))
    }
}

impl From<(&AssetRawResponse, &AssetSortBy)> for AssetSortedIndex {
    fn from(data: (&AssetRawResponse, &AssetSortBy)) -> Self {
        let (asset, sort_by) = data;
        AssetSortedIndex {
            pubkey: asset.pubkey.clone(),
            sorting_id: asset.encode_sorting_key(sort_by),
        }
    }
}
