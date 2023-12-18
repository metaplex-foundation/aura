use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};

pub mod asset_filter_client;
pub mod asset_index_client;
pub mod converters;
pub mod model;
pub mod storage_traits;
#[derive(Clone)]
pub struct PgClient {
    pub pool: PgPool,
}

impl PgClient {
    pub async fn new(url: &str, min_connections: u32, max_connections: u32) -> Self {
        let options: PgConnectOptions = url.parse().unwrap();

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .unwrap();

        Self { pool }
    }

    pub fn new_with_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}
