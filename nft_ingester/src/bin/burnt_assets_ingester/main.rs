use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use metrics_utils::{red::RequestErrorDurationMetrics, IngesterMetricsConfig};
use rocks_db::{migrator::MigrationState, Storage};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{error, info};

use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::Creator;
use entities::models::Updated;
use nft_ingester::api::dapi::rpc_asset_models::Asset;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::columns::asset::{
    AssetAuthority, AssetCollection, AssetCompleteDetails, AssetDynamicDetails, AssetOwner,
    AssetStaticDetails,
};
use solana_sdk::pubkey::Pubkey;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the target RocksDB instance
    #[arg(short, long, env = "TARGET_MAIN_DB_PATH")]
    target_db_path: PathBuf,

    /// Path to the NDJSON file with burnt assets
    #[arg(short, long)]
    input_file: PathBuf,

    /// Batch size for storing assets (default: 1000)
    #[arg(short = 'b', long, default_value_t = 1000)]
    batch_size: usize,
}

fn convert_asset_to_complete_details(asset: Asset) -> AssetCompleteDetails {
    let pubkey = Pubkey::try_from(asset.id.as_str()).expect("Invalid pubkey in asset id");

    // Convert ownership data
    let owner = AssetOwner {
        pubkey,
        owner: Updated {
            value: Some(
                Pubkey::try_from(asset.ownership.owner.as_str()).expect("Invalid owner pubkey"),
            ),
            update_version: None,
            slot_updated: 0,
        },
        delegate: Updated {
            value: asset
                .ownership
                .delegate
                .as_ref()
                .map(|d| Pubkey::try_from(d.as_str()).expect("Invalid delegate pubkey")),
            update_version: None,
            slot_updated: 0,
        },
        owner_type: Updated {
            value: OwnerType::from(asset.ownership.ownership_model),
            update_version: None,
            slot_updated: 0,
        },
        owner_delegate_seq: Updated { value: None, update_version: None, slot_updated: 0 },
        is_current_owner: Updated { value: true, update_version: None, slot_updated: 0 },
    };

    // Convert static details
    let static_details = Some(AssetStaticDetails {
        pubkey,
        specification_asset_class: SpecificationAssetClass::from(asset.interface),
        royalty_target_type: asset.royalty.as_ref().map_or(RoyaltyTargetType::Unknown, |r| {
            RoyaltyTargetType::from(r.royalty_model.clone())
        }),
        created_at: 0,
        edition_address: None,
    });

    // Convert dynamic details
    let dynamic_details = Some(AssetDynamicDetails {
        pubkey,
        is_compressible: Updated {
            value: asset.compression.as_ref().map(|c| c.eligible).unwrap_or(false),
            update_version: None,
            slot_updated: 0,
        },
        is_compressed: Updated {
            value: asset.compression.as_ref().map(|c| c.compressed).unwrap_or(false),
            update_version: None,
            slot_updated: 0,
        },
        is_frozen: Updated { value: asset.ownership.frozen, update_version: None, slot_updated: 0 },
        supply: asset.supply.as_ref().map(|s| Updated {
            value: s.print_current_supply,
            update_version: None,
            slot_updated: 0,
        }),
        seq: None,
        is_burnt: Updated { value: asset.burnt, update_version: None, slot_updated: 0 },
        was_decompressed: None,
        onchain_data: None,
        creators: Updated {
            value: asset
                .creators
                .as_ref()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|c| Creator {
                    creator: Pubkey::try_from(c.address.as_str()).expect("Invalid creator pubkey"),
                    creator_verified: c.verified,
                    creator_share: c.share as u8,
                })
                .collect(),
            update_version: None,
            slot_updated: 0,
        },
        royalty_amount: Updated {
            value: asset.royalty.as_ref().map(|r| r.basis_points as u16).unwrap_or_default(),
            update_version: None,
            slot_updated: 0,
        },
        url: Updated {
            value: asset.content.as_ref().map(|c| c.json_uri.clone()).unwrap_or_default(),
            update_version: None,
            slot_updated: 0,
        },
        chain_mutability: Some(Updated {
            value: if asset.mutable {
                ChainMutability::Mutable
            } else {
                ChainMutability::Immutable
            },
            update_version: None,
            slot_updated: 0,
        }),
        lamports: asset.lamports.map(|l| Updated {
            value: l,
            update_version: None,
            slot_updated: 0,
        }),
        executable: asset.executable.map(|e| Updated {
            value: e,
            update_version: None,
            slot_updated: 0,
        }),
        metadata_owner: asset.metadata_owner.map(|m| Updated {
            value: m,
            update_version: None,
            slot_updated: 0,
        }),
        raw_name: None,
        mpl_core_plugins: asset.plugins.as_ref().map(|p| Updated {
            value: p.to_string(),
            update_version: None,
            slot_updated: 0,
        }),
        mpl_core_unknown_plugins: asset.unknown_plugins.as_ref().map(|p| Updated {
            value: p.to_string(),
            update_version: None,
            slot_updated: 0,
        }),
        rent_epoch: asset.rent_epoch.map(|r| Updated {
            value: r,
            update_version: None,
            slot_updated: 0,
        }),
        num_minted: asset
            .mpl_core_info
            .as_ref()
            .and_then(|info| info.num_minted)
            .map(|n| Updated { value: n, update_version: None, slot_updated: 0 }),
        current_size: asset
            .mpl_core_info
            .as_ref()
            .and_then(|info| info.current_size)
            .map(|s| Updated { value: s, update_version: None, slot_updated: 0 }),
        plugins_json_version: asset
            .mpl_core_info
            .as_ref()
            .and_then(|info| info.plugins_json_version)
            .map(|v| Updated { value: v, update_version: None, slot_updated: 0 }),
        mpl_core_external_plugins: asset.external_plugins.as_ref().map(|p| Updated {
            value: p.to_string(),
            update_version: None,
            slot_updated: 0,
        }),
        mpl_core_unknown_external_plugins: asset
            .unknown_external_plugins
            .as_ref()
            .map(|p| Updated { value: p.to_string(), update_version: None, slot_updated: 0 }),
        mint_extensions: asset.mint_extensions.as_ref().map(|m| Updated {
            value: m.to_string(),
            update_version: None,
            slot_updated: 0,
        }),
    });

    // Convert collection if present
    let collection = asset.grouping.as_ref().and_then(|groups| {
        groups.iter().find(|g| g.group_key == "collection").map(|g| AssetCollection {
            pubkey,
            collection: Updated {
                value: g
                    .group_value
                    .as_ref()
                    .map(|v| Pubkey::try_from(v.as_str()).expect("Invalid collection pubkey"))
                    .unwrap_or_else(|| {
                        Pubkey::try_from(g.group_key.as_str()).expect("Invalid collection pubkey")
                    }),
                update_version: None,
                slot_updated: 0,
            },
            is_collection_verified: Updated {
                value: g.verified.unwrap_or(false),
                update_version: None,
                slot_updated: 0,
            },
            authority: Updated { value: None, update_version: None, slot_updated: 0 },
        })
    });

    // Convert authority if present
    let authority = asset.authorities.as_ref().and_then(|auths| {
        auths.first().map(|auth| AssetAuthority {
            pubkey,
            authority: Pubkey::try_from(auth.address.as_str()).expect("Invalid authority pubkey"),
            slot_updated: 0,
            write_version: None,
        })
    });

    AssetCompleteDetails {
        pubkey,
        static_details,
        dynamic_details,
        authority,
        owner: Some(owner),
        collection,
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Open target RocksDB
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let target_db = Arc::new(
        Storage::open(
            &args.target_db_path,
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to open target RocksDB"),
    );

    // Initialize metrics
    let ingester_metrics = Arc::new(IngesterMetricsConfig::new());

    // Initialize batch storage
    let mut batch_storage =
        BatchSaveStorage::new(target_db.clone(), args.batch_size, ingester_metrics);

    // Open and read the input file
    let file = File::open(&args.input_file).await.expect("Failed to open input file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut processed_count = 0;
    let mut error_count = 0;

    while let Some(line) = lines.next_line().await.expect("Failed to read line") {
        // Parse the JSON line into Asset
        let asset: Asset = match serde_json::from_str(&line) {
            Ok(asset) => asset,
            Err(e) => {
                error!("Failed to parse JSON line: {}", e);
                error_count += 1;
                continue;
            },
        };

        // Convert Asset to AssetCompleteDetails
        let asset_complete = convert_asset_to_complete_details(asset);

        // Store the asset
        if let Err(e) = batch_storage.store_complete(&asset_complete) {
            error!("Failed to store asset {}: {}", asset_complete.pubkey, e);
            error_count += 1;
            continue;
        }
        batch_storage.asset_updated_with_batch(0, asset_complete.pubkey);

        processed_count += 1;

        // Flush the batch if it's filled
        if batch_storage.batch_filled() {
            if let Err(e) = batch_storage.flush() {
                error!("Failed to flush batch: {}", e);
                error_count += 1;
            }
            info!("Processed {} assets ({} errors)", processed_count, error_count);
        }
    }

    // Flush any remaining assets
    if let Err(e) = batch_storage.flush() {
        error!("Failed to flush final batch: {}", e);
        error_count += 1;
    }

    info!(
        "Processing complete. Total processed: {} assets ({} errors)",
        processed_count, error_count
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_convert_simple_mpl_core_asset() {
        let asset_json = json!({"interface": "MplCoreAsset", "id": "J5MsDg3KqzY9nPiVsY72TD7R9Pb6qMFU4mFESmUW5ezC", "content": {"$schema": "https://schema.metaplex.com/nft1.0.json", "json_uri": "https://curved.pinit.io/6md84A17HUvFnyVL6V8NgnD7hGMfJYmQYuvEXftsB5ac/b645868e-64f1-42fc-b839-07b8576fae39/391.json", "files": [{"uri": "https://curved.pinit.io/6md84A17HUvFnyVL6V8NgnD7hGMfJYmQYuvEXftsB5ac/2dd9be53-3702-4da4-9340-851350cb4b60/391", "mime": "image/jpeg"}], "metadata": {"description": "10000 cute bear 🐻 ", "name": "solana bear maxi #391", "symbol": ""}, "links": {"image": "https://curved.pinit.io/6md84A17HUvFnyVL6V8NgnD7hGMfJYmQYuvEXftsB5ac/2dd9be53-3702-4da4-9340-851350cb4b60/391"}}, "authorities": [{"address": "AjgbTiAdRHtPnzE6t2RkhNHrLTGa6dW6LNW1GvDcq9VX", "scopes": ["full"]}], "compression": {"eligible": false, "compressed": false, "data_hash": "", "creator_hash": "", "asset_hash": "", "tree": "", "seq": 0, "leaf_id": 0}, "grouping": [{"group_key": "collection", "group_value": "6yNdLmoxtL2rEWSTYbUHegkhnsV5b8AT7o2j83qzYyxi", "verified": true}], "royalty": {"royalty_model": "creators", "target": null, "percent": 0.0, "basis_points": 0, "primary_sale_happened": false, "locked": false}, "creators": [], "ownership": {"frozen": false, "delegated": false, "delegate": null, "ownership_model": "single", "owner": "EqcBzFvKGQ2M9n29wgis2s4aqXNzq6unsWiJkCrECeYL"}, "mutable": true, "burnt": true, "plugins": {}, "mpl_core_info": {"plugins_json_version": 1}, "external_plugins": []}
        );

        let asset: Asset = serde_json::from_value(asset_json).unwrap();
        let result = convert_asset_to_complete_details(asset);

        assert_eq!(result.pubkey.to_string(), "J5MsDg3KqzY9nPiVsY72TD7R9Pb6qMFU4mFESmUW5ezC");

        let static_details = result.static_details.as_ref().unwrap();
        assert_eq!(static_details.specification_asset_class, SpecificationAssetClass::MplCoreAsset);
        assert_eq!(static_details.royalty_target_type, RoyaltyTargetType::Creators);

        let dynamic_details = result.dynamic_details.as_ref().unwrap();
        assert_eq!(dynamic_details.is_burnt.value, true);
        assert_eq!(dynamic_details.is_compressible.value, false);
        assert_eq!(dynamic_details.is_compressed.value, false);
        assert_eq!(dynamic_details.plugins_json_version.as_ref().unwrap().value, 1);
        assert!(dynamic_details.creators.value.is_empty());

        let owner = result.owner.as_ref().unwrap();
        assert_eq!(
            owner.owner.value.unwrap().to_string(),
            "EqcBzFvKGQ2M9n29wgis2s4aqXNzq6unsWiJkCrECeYL"
        );
        assert_eq!(owner.delegate.value, None);
        assert_eq!(owner.is_current_owner.value, true);

        let collection = result.collection.as_ref().unwrap();
        assert_eq!(
            collection.collection.value.to_string(),
            "6yNdLmoxtL2rEWSTYbUHegkhnsV5b8AT7o2j83qzYyxi"
        );
        assert_eq!(collection.is_collection_verified.value, true);

        assert_eq!(
            result.authority.as_ref().unwrap().authority.to_string(),
            "AjgbTiAdRHtPnzE6t2RkhNHrLTGa6dW6LNW1GvDcq9VX"
        );
    }

    #[test]
    fn test_convert_asset_with_creators() {
        let asset_json = json!({
            "interface": "MplCoreAsset",
            "id": "D5oeQGgYXmqzsPtaxkMKWPieacwD9o9KZqxT4ic1fuJ",
            "content": {
                "$schema": "https://schema.metaplex.com/nft1.0.json",
                "json_uri": "https://arweave.net/5xsJqmA-QymuBh7eGXi-6B8phTe5TK3fVeblNfX9QQg/25.json",
                "files": [{"uri": "https://arweave.net/zEble7QNypS4-Iirj8fycUo2EU08NaBmGe3cehjwH4E", "mime": "image/png"}],
                "metadata": {"attributes": [{"value": "25%", "trait_type": "Multiplier"}], "description": "Used to boost rewards or get discounts on critters.quest", "name": "Multiplier 25%", "symbol": ""},
                "links": {"image": "https://arweave.net/zEble7QNypS4-Iirj8fycUo2EU08NaBmGe3cehjwH4E", "external_url": "https://critters.quest"}
            },
            "authorities": [{"address": "62YNtwqz9AtuLScnvGoNwLHT3dNHkfnh3wmLHbC5J9rT", "scopes": ["full"]}],
            "compression": {"eligible": false, "compressed": false, "data_hash": "", "creator_hash": "", "asset_hash": "", "tree": "", "seq": 0, "leaf_id": 0}, "grouping": [{"group_key": "collection", "group_value": "BoostaXCxu4nHKwHNSesRiodEC9XBMbKB1YrLqF4WUcQ", "verified": true}], "royalty": {"royalty_model": "creators", "target": null, "percent": 0.05, "basis_points": 500, "primary_sale_happened": false, "locked": false}, "creators": [{"address": "AagX5zJexjYjPpWL4KKZijbMzMQmgqzfXTQFdkcSgY5T", "share": 100, "verified": true}], "ownership": {"frozen": false, "delegated": true, "delegate": "CxmcP9q52DgEfBUvdSU4gNfVfkSe1DzyEQ4QgnD6HaBZ", "ownership_model": "single", "owner": "CxmcP9q52DgEfBUvdSU4gNfVfkSe1DzyEQ4QgnD6HaBZ"}, "mutable": true, "burnt": true, "plugins": {"royalties": {"data": {"creators": [{"address": "AagX5zJexjYjPpWL4KKZijbMzMQmgqzfXTQFdkcSgY5T", "percentage": 100}], "rule_set": "None", "basis_points": 500}, "index": 2, "offset": 231, "authority": {"type": "Address", "address": "62YNtwqz9AtuLScnvGoNwLHT3dNHkfnh3wmLHbC5J9rT"}}, "attributes": {"data": {"attribute_list": [{"key": "Multiplier", "value": "25"}]}, "index": 0, "offset": 169, "authority": {"type": "Address", "address": "62YNtwqz9AtuLScnvGoNwLHT3dNHkfnh3wmLHbC5J9rT"}}, "freeze_delegate": {"data": {"frozen": false}, "index": 3, "offset": 272, "authority": {"type": "Owner", "address": null}}, "update_delegate": {"data": {"additional_delegates": ["qUeStAuzHadtiUn9uZhcudP1seM1pRmwmVonX6kAtfn"]}, "index": 1, "offset": 194, "authority": {"type": "Address", "address": "62YNtwqz9AtuLScnvGoNwLHT3dNHkfnh3wmLHbC5J9rT"}}, "transfer_delegate": {"data": {}, "index": 4, "offset": 274, "authority": {"type": "Owner", "address": null}}}, "mpl_core_info": {"plugins_json_version": 1}, "external_plugins": []
        });

        let asset: Asset = serde_json::from_value(asset_json).unwrap();
        let result = convert_asset_to_complete_details(asset);

        assert_eq!(result.pubkey.to_string(), "D5oeQGgYXmqzsPtaxkMKWPieacwD9o9KZqxT4ic1fuJ");

        let dynamic_details = result.dynamic_details.as_ref().unwrap();
        assert_eq!(dynamic_details.creators.value.len(), 1);
        let creator = &dynamic_details.creators.value[0];
        assert_eq!(creator.creator.to_string(), "AagX5zJexjYjPpWL4KKZijbMzMQmgqzfXTQFdkcSgY5T");
        assert_eq!(creator.creator_share, 100);
        assert_eq!(creator.creator_verified, true);

        assert_eq!(dynamic_details.royalty_amount.value, 500);

        let owner = result.owner.as_ref().unwrap();
        assert_eq!(
            owner.owner.value.unwrap().to_string(),
            "CxmcP9q52DgEfBUvdSU4gNfVfkSe1DzyEQ4QgnD6HaBZ"
        );
        assert_eq!(
            owner.delegate.value.unwrap().to_string(),
            "CxmcP9q52DgEfBUvdSU4gNfVfkSe1DzyEQ4QgnD6HaBZ"
        );
        assert_eq!(owner.is_current_owner.value, true);

        assert!(dynamic_details.plugins_json_version.as_ref().unwrap().value == 1);
    }

    #[test]
    fn test_convert_asset_with_plugins() {
        let asset_json = json!({"interface": "MplCoreAsset", "id": "J1jgbfjxUqx2HwFbZHhumg4D9GDa7jLRXKgKWPak6Cx2", "content": {"$schema": "https://schema.metaplex.com/nft1.0.json", "json_uri": "https://gateway.pinit.io/ipfs/QmRcQdmiginmByyFrpscFoL4wf2qtKfiPXjKSdDQ6LRSxD/1220.json", "files": [{"uri": "https://gateway.pinit.io/ipfs/QmSaXPpchFLaeRgBeShtzfTRXndkBgGS6T36TEn8NLGguB/1220", "mime": "image/png"}], "metadata": {"attributes": [{"value": "Red", "trait_type": "Background"}, {"value": "Galaxy", "trait_type": "Skin"}, {"value": "Mooki", "trait_type": "Outfit"}, {"value": "Smile", "trait_type": "Mouth"}, {"value": "Nerd", "trait_type": "Eyes"}, {"value": "Winter Cold", "trait_type": "Head"}, {"value": "Bobo Plush", "trait_type": "Items"}, {"value": 1748, "max_value": 3333, "trait_type": "Rarity Rank", "display_type": "number"}], "description": "3333 Moopets ready to take the world by storm", "name": "Moopets #12", "symbol": ""}, "links": {"image": "https://gateway.pinit.io/ipfs/QmSaXPpchFLaeRgBeShtzfTRXndkBgGS6T36TEn8NLGguB/1220", "external_url": ""}}, "authorities": [{"address": "2EDPMnKXP2ESRkKCry4tmfMW7Sp8QCnG9nh9b2dJQwX8", "scopes": ["full"]}], "compression": {"eligible": false, "compressed": false, "data_hash": "", "creator_hash": "", "asset_hash": "", "tree": "", "seq": 0, "leaf_id": 0}, "grouping": [{"group_key": "collection", "group_value": "DCufBcekqKYyxM5ZGu77zWGtqCv7Vb9wFzhRvsgXDSPy", "verified": true}], "royalty": {"royalty_model": "creators", "target": null, "percent": 0.0, "basis_points": 0, "primary_sale_happened": false, "locked": false}, "creators": [], "ownership": {"frozen": false, "delegated": false, "delegate": null, "ownership_model": "single", "owner": "7FeBWWYjWNDm7D7QUKbnAYLRmcSwWG4LwHtbS8wt7Fdj"}, "mutable": true, "burnt": true, "plugins": {"freeze_delegate": {"data": {"frozen": false}, "index": 0, "offset": 181, "authority": {"type": "Owner", "address": null}}}, "mpl_core_info": {"plugins_json_version": 1}, "external_plugins": []}
        );

        let asset: Asset = serde_json::from_value(asset_json).unwrap();
        let result = convert_asset_to_complete_details(asset);

        assert_eq!(result.pubkey.to_string(), "J1jgbfjxUqx2HwFbZHhumg4D9GDa7jLRXKgKWPak6Cx2");

        let dynamic_details = result.dynamic_details.as_ref().unwrap();
        assert!(dynamic_details.mpl_core_plugins.is_some());
        assert_eq!(dynamic_details.is_frozen.value, false);
        assert_eq!(dynamic_details.plugins_json_version.as_ref().unwrap().value, 1);

        let owner = result.owner.as_ref().unwrap();
        assert_eq!(
            owner.owner.value.unwrap().to_string(),
            "7FeBWWYjWNDm7D7QUKbnAYLRmcSwWG4LwHtbS8wt7Fdj"
        );
        assert_eq!(owner.delegate.value, None);
        assert_eq!(owner.is_current_owner.value, true);

        let collection = result.collection.as_ref().unwrap();
        assert_eq!(
            collection.collection.value.to_string(),
            "DCufBcekqKYyxM5ZGu77zWGtqCv7Vb9wFzhRvsgXDSPy"
        );
        assert_eq!(collection.is_collection_verified.value, true);
    }
}
