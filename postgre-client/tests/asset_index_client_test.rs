mod db_setup;

#[cfg(feature = "integration_tests")]
#[cfg(test)]
mod tests {
    use super::db_setup;
    use postgre_client::model::AssetIndex;
    use postgre_client::storage_traits::AssetIndexStorage;
    use postgre_client::PgClient;
    use rand::Rng;
    use sqlx::{Pool, Postgres};
    use testcontainers::clients::Cli;
    use testcontainers::*;
    use tokio;

    async fn count_rows_in_assets(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM assets_v3")
            .fetch_one(pool)
            .await?;

        Ok(count)
    }

    async fn count_rows_in_creators(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM asset_creators_v3")
            .fetch_one(pool)
            .await?;

        Ok(count)
    }

    async fn count_rows_in_metadata(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM metadata")
            .fetch_one(pool)
            .await?;

        Ok(count)
    }

    #[tokio::test]
    async fn test_fetch_last_synced_id_and_update_asset_indexes_batch() {
        let cli = Cli::default();
        let node = cli.run(images::postgres::Postgres::default());
        let (pool, db_name) = db_setup::setup_database(&node).await;

        let asset_index_storage = PgClient::new_with_pool(pool.clone());

        // Verify initial fetch_last_synced_id returns None
        assert!(asset_index_storage
            .fetch_last_synced_id()
            .await
            .unwrap()
            .is_none());

        // Generate random asset indexes
        let asset_indexes = db_setup::generate_asset_index_records(100);
        let last_known_key = db_setup::generate_random_vec(8 + 8 + 32);
        // Insert assets and last key using update_asset_indexes_batch
        asset_index_storage
            .update_asset_indexes_batch(asset_indexes.as_slice(), &last_known_key)
            .await
            .unwrap();

        // Check fetch_last_synced_id again
        let last_synced_key = asset_index_storage.fetch_last_synced_id().await.unwrap();
        assert!(last_synced_key.is_some());
        assert_eq!(last_synced_key.unwrap().as_slice(), &last_known_key[..]);

        // Check the assets are in the db using plain SQL
        let cnt = count_rows_in_assets(&pool).await.unwrap();
        assert_eq!(cnt, 100);
        // ... (Write a SQL query to assert the inserted assets here)

        let new_known_key = db_setup::generate_random_vec(8 + 8 + 32);
        // upsert the existing keys
        let updated_asset_indexes = asset_indexes
            .iter()
            .map(|asset_index| {
                let mut ai = asset_index.clone();
                ai.slot_updated = 200;
                ai.creators[0].creator_verified = !ai.creators[0].creator_verified;
                ai
            })
            .collect::<Vec<AssetIndex>>();
        asset_index_storage
            .update_asset_indexes_batch(updated_asset_indexes.as_slice(), &new_known_key)
            .await
            .unwrap();

        let last_synced_key = asset_index_storage.fetch_last_synced_id().await.unwrap();
        assert!(last_synced_key.is_some());
        assert_eq!(last_synced_key.unwrap().as_slice(), &new_known_key[..]);

        let cnt = count_rows_in_assets(&pool).await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = count_rows_in_metadata(&pool).await.unwrap();
        assert_eq!(cnt, 1);

        pool.close().await;
        db_setup::teardown(&node, &db_name).await;
    }

    #[tokio::test]
    async fn test_update_asset_indexes_batch_w_creators_update() {
        let cli = Cli::default();
        let node = cli.run(images::postgres::Postgres::default());
        let (pool, db_name) = db_setup::setup_database(&node).await;

        // Assuming `MyAssetIndexStorage` implements `AssetIndexStorage`
        let asset_index_storage = PgClient::new_with_pool(pool.clone());

        // Generate random asset indexes
        let mut asset_indexes = db_setup::generate_asset_index_records(100);
        // every asset_index will have 3 creators
        for asset_index in asset_indexes.iter_mut() {
            asset_index.creators.push(postgre_client::model::Creator {
                creator: db_setup::generate_random_vec(32),
                creator_verified: rand::thread_rng().gen_bool(0.5),
            });
            asset_index.creators.push(postgre_client::model::Creator {
                creator: db_setup::generate_random_vec(32),
                creator_verified: rand::thread_rng().gen_bool(0.5),
            });
        }

        let last_known_key = db_setup::generate_random_vec(8 + 8 + 32);

        // Insert assets and last key using update_asset_indexes_batch
        asset_index_storage
            .update_asset_indexes_batch(asset_indexes.as_slice(), &last_known_key)
            .await
            .unwrap();

        // Check the assets are in the db using plain SQL
        let cnt = count_rows_in_assets(&pool).await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = count_rows_in_creators(&pool).await.unwrap();
        assert_eq!(cnt, 300);

        // upsert the existing keys with 2 last creators replaced with a new one
        let updated_asset_indexes = asset_indexes
            .iter()
            .map(|asset_index| {
                let mut ai = asset_index.clone();
                ai.creators.pop();
                ai.creators[1].creator = db_setup::generate_random_vec(32);
                ai.creators[0].creator_verified = !ai.creators[0].creator_verified;
                ai
            })
            .collect::<Vec<AssetIndex>>();

        asset_index_storage
            .update_asset_indexes_batch(updated_asset_indexes.as_slice(), &last_known_key)
            .await
            .unwrap();

        let cnt = count_rows_in_assets(&pool).await.unwrap();
        assert_eq!(cnt, 100);

        let cnt = count_rows_in_creators(&pool).await.unwrap();
        assert_eq!(cnt, 200);

        pool.close().await;
        db_setup::teardown(&node, &db_name).await;
    }
}
