use std::sync::Arc;

use entities::enums::AssetType;
use postgre_client::{storage_traits::AssetIndexStorage, PgClient};
use rocks_db::{
    key_encoders::decode_u64x2_pubkey, storage_traits::AssetUpdateIndexStorage, Storage,
};
use tracing::error;

use crate::{
    api::{
        dapi::response::{Check, HealthCheckResponse, Status, SyncInfo},
        error::DasApiError,
    },
    config::HealthCheckInfo,
};

pub async fn check_health(
    health_check_info: HealthCheckInfo,
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    maximum_healthy_desync: u64,
) -> Result<HealthCheckResponse, DasApiError> {
    let mut overall_status = Status::Ok;
    let mut last_sync_seq_pg_fungible: Option<u64> = None;
    let mut last_sync_seq_pg_nft: Option<u64> = None;
    let mut last_sync_seq_rocksdb_fungible: Option<u64> = None;
    let mut last_sync_seq_rocksdb_nft: Option<u64> = None;

    let mut pg_check = Check::new("PostgresDB".to_string());
    let mut rocks_sync_divergence_check_fungible = Check::new("FungibleSync".to_string());
    let mut rocks_sync_divergence_check_nft = Check::new("NonFungibleSync".to_string());
    if pg_client.check_health().await.is_err() {
        overall_status = Status::Unhealthy;
        pg_check.status = Status::Unhealthy;
        pg_check.description = Some(DasApiError::InternalPostgresError.to_string());
        rocks_sync_divergence_check_fungible.status = Status::Unhealthy;
        rocks_sync_divergence_check_fungible.description =
            Some(DasApiError::InternalPostgresError.to_string());
        rocks_sync_divergence_check_nft.status = Status::Unhealthy;
        rocks_sync_divergence_check_nft.description =
            Some(DasApiError::InternalPostgresError.to_string());
    }

    if overall_status != Status::Unhealthy {
        let last_synced_key_fungibles =
            pg_client.fetch_last_synced_id(AssetType::Fungible).await.map_err(|e| {
                error!(error = %e, "Failed to fetch last synced key for fungibles: {e:?}");
                DasApiError::InternalPostgresError
            })?;

        match last_synced_key_fungibles {
            Some(last_synced_fungibles) => {
                let pg_last_updated_key_fungible = decode_u64x2_pubkey(last_synced_fungibles)
                    .map_err(|_| DasApiError::InternalPostgresError)?;
                let rocks_last_updated_key_fungible =
                    rocks_db.last_known_fungible_asset_updated_key().ok().flatten().ok_or(
                        DasApiError::RocksError(
                            "Unable to fetch last known fungible asset updated key".to_string(),
                        ),
                    )?;
                last_sync_seq_pg_fungible = Some(pg_last_updated_key_fungible.seq);
                last_sync_seq_rocksdb_fungible = Some(rocks_last_updated_key_fungible.seq);
                if rocks_last_updated_key_fungible
                    .seq
                    .saturating_sub(pg_last_updated_key_fungible.seq)
                    > maximum_healthy_desync
                {
                    rocks_sync_divergence_check_fungible.status = Status::Degraded;
                    rocks_sync_divergence_check_fungible.description =
                        Some("Fungible asset desynchronization".to_string());
                }
            },
            None => {
                overall_status = Status::Unhealthy;
                rocks_sync_divergence_check_fungible.status = Status::Unhealthy;
                rocks_sync_divergence_check_fungible.description =
                    Some(DasApiError::InternalPostgresError.to_string());
            },
        }

        let last_synced_key_nfts =
            pg_client.fetch_last_synced_id(AssetType::NonFungible).await.map_err(|e| {
                error!(error = %e, "Failed to fetch last synced key for non-fungibles: {e:?}");
                DasApiError::InternalPostgresError
            })?;

        match last_synced_key_nfts {
            Some(last_synced_nfts) => {
                let pg_last_updated_key_nft = decode_u64x2_pubkey(last_synced_nfts)
                    .map_err(|_| DasApiError::InternalPostgresError)?;
                let rocks_last_updated_key_nft =
                    rocks_db.last_known_nft_asset_updated_key().ok().flatten().ok_or(
                        DasApiError::RocksError(
                            "Unable to fetch last known non-fungible asset updated key".to_string(),
                        ),
                    )?;
                last_sync_seq_pg_nft = Some(pg_last_updated_key_nft.seq);
                last_sync_seq_rocksdb_nft = Some(rocks_last_updated_key_nft.seq);
                if rocks_last_updated_key_nft.seq.saturating_sub(pg_last_updated_key_nft.seq)
                    > maximum_healthy_desync
                {
                    rocks_sync_divergence_check_nft.status = Status::Degraded;
                    rocks_sync_divergence_check_nft.description =
                        Some("Non-fungible asset desynchronization".to_string());
                }
            },
            None => {
                overall_status = Status::Unhealthy;
                rocks_sync_divergence_check_nft.status = Status::Unhealthy;
                rocks_sync_divergence_check_nft.description =
                    Some(DasApiError::InternalPostgresError.to_string());
            },
        }
    }

    Ok(HealthCheckResponse {
        status: overall_status,
        app_version: health_check_info.app_version.clone(),
        node_name: health_check_info.node_name.clone(),
        checks: vec![
            pg_check,
            rocks_sync_divergence_check_fungible,
            rocks_sync_divergence_check_nft,
        ],
        image_info: health_check_info.image_info.clone(),
        sync_info: SyncInfo {
            last_sync_seq_pg_fungible,
            last_sync_seq_pg_nft,
            last_sync_seq_rocksdb_fungible,
            last_sync_seq_rocksdb_nft,
        },
    })
}
