use async_trait::async_trait;
use entities::rollup::Rollup;
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;
use postgre_client::PgClient;
use rocks_db::Storage;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::error;

// pub fn create_leaf_schema(
//     asset: &RolledMintInstruction,
//     tree_id: &Pubkey,
// ) -> Result<LeafSchemaEvent, IngesterError> {
//     // @dev: seller_fee_basis points is encoded twice so that it can be passed to marketplace
//     // instructions, without passing the entire, un-hashed MetadataArgs struct
//     let metadata_args_hash = keccak::hashv(&[asset.mint_args.try_to_vec()?.as_slice()]);
//     let data_hash = keccak::hashv(&[
//         &metadata_args_hash.to_bytes(),
//         &asset.mint_args.seller_fee_basis_points.to_le_bytes(),
//     ]);
//
//     // Use the metadata auth to check whether we can allow `verified` to be set to true in the
//     // creator Vec.
//     let creator_data = asset
//         .mint_args
//         .creators
//         .iter()
//         .map(|c| [c.address.as_ref(), &[c.verified as u8], &[c.share]].concat())
//         .collect::<Vec<_>>();
//
//     // Calculate creator hash.
//     let creator_hash = keccak::hashv(
//         creator_data
//             .iter()
//             .map(|c| c.as_slice())
//             .collect::<Vec<&[u8]>>()
//             .as_ref(),
//     );
//
//     let asset_id = get_asset_id(tree_id, asset.nonce);
//     if asset_id != asset.id {
//         return Err(IngesterError::PDACheckFail(
//             asset_id.to_string(),
//             asset.id.to_string(),
//         ));
//     }
//     let leaf = LeafSchema::V1 {
//         id: asset.id,
//         owner: asset.owner,
//         delegate: asset.delegate,
//         nonce: asset.nonce,
//         data_hash: data_hash.to_bytes(),
//         creator_hash: creator_hash.to_bytes(),
//     };
//     let leaf_hash = leaf.hash();
//
//     Ok(LeafSchemaEvent::new(Version::V1, leaf, leaf_hash))
// }

pub struct RollupDownloaderImpl;
#[async_trait]
impl RollupDownloader for RollupDownloaderImpl {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

pub struct RollupProcessor {
    pg_client: Arc<PgClient>,
    rocks: Arc<Storage>,
}

impl RollupProcessor {
    pub fn new(pg_client: Arc<PgClient>, rocks: Arc<Storage>) -> Self {
        Self { pg_client, rocks }
    }

    pub async fn process_rollups(&self, rx: Receiver<()>) {
        while rx.is_empty() {
            let rollup_to_process = match self.pg_client.fetch_rollup_for_processing().await {
                Ok(rollup) => rollup,
                Err(e) => {
                    error!("Failed fetch rollup for processing: {}", e);
                    continue;
                }
            };
            let json_file = match tokio::fs::read_to_string(&rollup_to_process.file_path).await {
                Ok(json_file) => json_file,
                Err(e) => {
                    error!("Failed read file to string: {}", e);
                    continue;
                }
            };
            let rollup = match serde_json::from_str::<Rollup>(&json_file) {
                Ok(rollup) => rollup,
                Err(e) => {
                    if let Err(e) = self
                        .pg_client
                        .mark_rollup_as_verification_failed(
                            &rollup_to_process.file_path,
                            &e.to_string(),
                        )
                        .await
                    {
                        error!("Failed mark rollup as verification failed: {}", e);
                    }
                    continue;
                }
            };
        }
    }
}
