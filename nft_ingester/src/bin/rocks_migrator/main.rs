use std::sync::Arc;

use bincode::de;
use entities::models::{Creator, Updated};
use nft_ingester::config::init_logger;
use nft_ingester::config::{setup_config, BackfillerConfig, IngesterConfig, INGESTER_BACKUP_NAME};
use nft_ingester::error::IngesterError;
use rocks_db::{backup_service, v0::StorageV0, Storage, key_encoders::decode_u64_pubkey};

use log::{error, info};

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    init_logger();

    info!("Started");
    let config: IngesterConfig = setup_config();

    let storage_v1 = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
    )
    .unwrap();

    let rocks_storage_v1 = Arc::new(storage_v1);

    let storage_v0 = StorageV0::open(
        &config
            .rocks_db_v0_path
            .clone()
            .unwrap_or(DEFAULT_ROCKSDB_PATH.to_string()),
    )
    .unwrap();

    let rocks_storage_v0 = Arc::new(storage_v0);

    let mut asset_static_data_iter = rocks_storage_v0.asset_static_data.iter_start();

    let mut counter = 0;

    while let Some(static_info_result) = asset_static_data_iter.next() {
        match static_info_result {
            Ok(static_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::asset::AssetStaticDetails>(&static_info.1)
                        .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&static_info.0);

                rocks_storage_v1
                    .asset_static_data
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_static_data", counter);

    counter = 0;

    let mut asset_dynamic_data_iter = rocks_storage_v0.asset_dynamic_data.iter_start();

    while let Some(dynamic_info_result) = asset_dynamic_data_iter.next() {
        match dynamic_info_result {
            Ok(dynamic_info) => {
                let deserialized = bincode::deserialize::<
                    rocks_db::v0::columns::asset::AssetDynamicDetails,
                >(&dynamic_info.1)
                .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&dynamic_info.0);

                let slot = deserialized.slot_updated;
                let seq = deserialized.seq;
                let creators = deserialized
                    .creators
                    .iter()
                    .map(|creator| Creator {
                        creator: creator.creator,
                        creator_verified: creator.creator_verified,
                        creator_share: creator.creator_share,
                    })
                    .collect();

                let dynamic_data_v1 = rocks_db::asset::AssetDynamicDetails {
                    pubkey: key,
                    is_compressible: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.is_compressible,
                    },
                    is_compressed: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.is_compressed,
                    },
                    is_frozen: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.is_frozen,
                    },
                    supply: deserialized.supply.map(|supply| Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: supply,
                    }),
                    seq: seq.map(|seq_internal| Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: seq_internal,
                    }),
                    is_burnt: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.is_burnt,
                    },
                    was_decompressed: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.was_decompressed,
                    },
                    onchain_data: deserialized.onchain_data.map(|onchain_data| Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: onchain_data,
                    }),
                    creators: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: creators,
                    },
                    royalty_amount: Updated {
                        slot_updated: deserialized.slot_updated,
                        seq,
                        value: deserialized.royalty_amount,
                    },
                };

                rocks_storage_v1
                    .asset_dynamic_data
                    .put(key, &dynamic_data_v1)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_dynamic_data", counter);
    counter = 0;

    let mut asset_authority_data_iter = rocks_storage_v0.asset_authority_data.iter_start();

    while let Some(authority_info_result) = asset_authority_data_iter.next() {
        match authority_info_result {
            Ok(authority_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::asset::AssetAuthority>(
                        &authority_info.1,
                    )
                    .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&authority_info.0);

                rocks_storage_v1
                    .asset_authority_data
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_authority_data", counter);
    counter = 0;

    let mut asset_owner_data_iter = rocks_storage_v0.asset_owner_data.iter_start();

    while let Some(owner_info_result) = asset_owner_data_iter.next() {
        match owner_info_result {
            Ok(owner_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::v0::columns::asset::AssetOwner>(&owner_info.1).unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&owner_info.0);

                let slot = deserialized.slot_updated;
                let seq = deserialized.owner_delegate_seq;
                let owner_type = {
                    match deserialized.owner_type {
                        rocks_db::v0::columns::asset::OwnerType::Unknown => {
                            entities::enums::OwnerType::Unknown
                        }
                        rocks_db::v0::columns::asset::OwnerType::Token => {
                            entities::enums::OwnerType::Token
                        }
                        rocks_db::v0::columns::asset::OwnerType::Single => {
                            entities::enums::OwnerType::Single
                        }
                    }
                };

                let asset_owner_v1 = rocks_db::asset::AssetOwner {
                    pubkey: key,
                    owner: Updated {
                        slot_updated: slot,
                        seq,
                        value: deserialized.owner,
                    },
                    delegate: deserialized.delegate.map(|delegate| Updated {
                        slot_updated: slot,
                        seq,
                        value: delegate,
                    }),
                    owner_type: Updated {
                        slot_updated: slot,
                        seq,
                        value: owner_type,
                    },
                    owner_delegate_seq: seq.map(|seq_internal| Updated {
                        slot_updated: slot,
                        seq,
                        value: seq_internal,
                    }),
                };

                rocks_storage_v1
                    .asset_owner_data
                    .put(key, &asset_owner_v1)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_owner_data", counter);
    counter = 0;

    let mut asset_leaf_data_iter = rocks_storage_v0.asset_leaf_data.iter_start();

    while let Some(leaf_info_result) = asset_leaf_data_iter.next() {
        match leaf_info_result {
            Ok(leaf_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::asset::AssetLeaf>(&leaf_info.1)
                        .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&leaf_info.0);

                rocks_storage_v1
                    .asset_leaf_data
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_leaf_data", counter);
    counter = 0;

    let mut asset_collection_data_iter = rocks_storage_v0.asset_collection_data.iter_start();

    while let Some(collection_info_result) = asset_collection_data_iter.next() {
        match collection_info_result {
            Ok(collection_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::asset::AssetCollection>(
                        &collection_info.1,
                    )
                    .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&collection_info.0);

                rocks_storage_v1
                    .asset_collection_data
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_collection_data", counter);
    counter = 0;

    let mut asset_offchain_data_iter = rocks_storage_v0.asset_offchain_data.iter_start();

    while let Some(offchain_info_result) = asset_offchain_data_iter.next() {
        match offchain_info_result {
            Ok(offchain_info) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::offchain_data::OffChainData>(&offchain_info.1)
                        .unwrap();

                let key = String::from_utf8(offchain_info.0.to_vec()).unwrap_or_default();

                rocks_storage_v1
                    .asset_offchain_data
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} asset_offchain_data", counter);
    counter = 0;

    let mut cl_items_iter = rocks_storage_v0.cl_items.iter_start();

    while let Some(cl_item_result) = cl_items_iter.next() {
        match cl_item_result {
            Ok(cl_item) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::cl_items::ClItem>(&cl_item.1).unwrap();

                let key = decode_u64_pubkey(cl_item.0.to_vec()).unwrap();

                rocks_storage_v1.cl_items.put(key, &deserialized).unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} cl_items", counter);
    counter = 0;

    let mut cl_leafs_iter = rocks_storage_v0.cl_leafs.iter_start();

    while let Some(cl_leaf_result) = cl_leafs_iter.next() {
        match cl_leaf_result {
            Ok(cl_leaf) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::cl_items::ClLeaf>(&cl_leaf.1).unwrap();

                let key = decode_u64_pubkey(cl_leaf.0.to_vec()).unwrap();

                rocks_storage_v1.cl_leafs.put(key, &deserialized).unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} cl_leafs", counter);
    counter = 0;

    let mut token_accounts_iter = rocks_storage_v0.token_accounts.iter_start();

    while let Some(token_account_result) = token_accounts_iter.next() {
        match token_account_result {
            Ok(token_account) => {
                let deserialized =
                    bincode::deserialize::<rocks_db::column::columns::TokenAccount>(&token_account.1)
                        .unwrap();

                let key = solana_sdk::pubkey::Pubkey::new(&token_account.0);

                rocks_storage_v1
                    .token_accounts
                    .put(key, &deserialized)
                    .unwrap();

                counter += 1;
            }
            Err(e) => {
                error!("Error: {:?}", e);
            }
        }
    }

    info!("Copied {} token_accounts", counter);
    counter = 0;

    Ok(())
}
