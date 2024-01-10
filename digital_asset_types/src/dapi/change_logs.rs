use std::collections::HashMap;
use std::sync::Arc;

use log::debug;
use solana_sdk::pubkey::Pubkey;

use rocks_db::asset_streaming_client::get_required_nodes_for_proof;
use rocks_db::Storage;
use {
    crate::dao::cl_items,
    crate::rpc::AssetProof,
    sea_orm::{DbErr, FromQueryResult},
    spl_concurrent_merkle_tree::node::empty_node,
};

use crate::fetch_asset_data;

#[derive(FromQueryResult, Debug, Default, Clone, Eq, PartialEq)]
struct SimpleChangeLog {
    cli_hash: Vec<u8>,
    cli_level: i64,
    cli_node_idx: i64,
    cli_seq: i64,
    cli_tree: Vec<u8>,
}

pub async fn get_proof_for_assets(
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
) -> Result<HashMap<String, Option<AssetProof>>, DbErr> {
    let mut results: HashMap<String, Option<AssetProof>> =
        asset_ids.iter().map(|id| (id.to_string(), None)).collect();

    let tree_ids = fetch_asset_data!(rocks_db, asset_leaf_data, asset_ids)
        .values()
        .map(|asset| {
            (
                asset.tree_id,
                (asset.pubkey, asset.nonce.unwrap_or_default()),
            )
        })
        .collect::<HashMap<_, _>>();
    let keys = rocks_db
        .cl_leafs
        .batch_get(
            tree_ids
                .clone()
                .into_iter()
                .map(|(tree, (_, nonce))| (nonce, tree))
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?
        .into_iter()
        .filter_map(|cl_leaf| cl_leaf.map(|leaf| (leaf.cli_node_idx, leaf.cli_tree_key)))
        .collect::<Vec<_>>();
    let cl_items_first_leaf = rocks_db
        .cl_items
        .batch_get(keys.clone())
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?;

    if cl_items_first_leaf.is_empty() {
        return Ok(HashMap::new());
    }

    let leaves: HashMap<_, cl_items::Model> = cl_items_first_leaf
        .into_iter()
        .filter_map(|leaf| {
            leaf.and_then(|leaf| {
                tree_ids.get(&leaf.cli_tree_key).map(|(pubkey, _)| {
                    (
                        pubkey.to_bytes().to_vec(),
                        cl_items::Model {
                            id: 0,
                            tree: leaf.cli_tree_key.to_bytes().to_vec(),
                            node_idx: leaf.cli_node_idx as i64,
                            leaf_idx: leaf.cli_leaf_idx.map(|idx| idx as i64),
                            seq: leaf.cli_seq as i64,
                            level: leaf.cli_level as i64,
                            hash: leaf.cli_hash,
                        },
                    )
                })
            })
        })
        .collect();

    let all_req_keys: Vec<_> = keys
        .into_iter()
        .flat_map(|(node, tree)| {
            get_required_nodes_for_proof(node as i64)
                .into_iter()
                .map(move |node| (node as u64, tree))
        })
        .collect();
    let all_nodes = rocks_db
        .cl_items
        .batch_get(all_req_keys)
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?
        .into_iter()
        .flatten()
        .map(|node| SimpleChangeLog {
            cli_hash: node.cli_hash,
            cli_level: node.cli_level as i64,
            cli_node_idx: node.cli_node_idx as i64,
            cli_seq: node.cli_seq as i64,
            cli_tree: node.cli_tree_key.to_bytes().to_vec(),
        })
        .collect::<Vec<_>>();

    for asset_id in asset_ids.clone().iter() {
        let proof = get_asset_proof(asset_id, &all_nodes, &leaves);
        results.insert(asset_id.to_string(), proof);
    }

    Ok(results)
}

fn get_asset_proof(
    asset_id: &Pubkey,
    nodes: &[SimpleChangeLog],
    leaves: &HashMap<Vec<u8>, cl_items::Model>,
) -> Option<AssetProof> {
    let leaf_key = asset_id.to_bytes().to_vec();
    let leaf = match leaves.get(&leaf_key) {
        Some(leaf) => leaf.clone(),
        None => return None,
    };

    let req_indexes = get_required_nodes_for_proof(leaf.node_idx);
    let mut final_node_list: Vec<SimpleChangeLog> =
        vec![SimpleChangeLog::default(); req_indexes.len()];

    let mut relevant_nodes: Vec<&SimpleChangeLog> = nodes
        .iter()
        .filter(|node| node.cli_tree == leaf.tree && req_indexes.contains(&node.cli_node_idx))
        .collect();

    relevant_nodes.sort_by(|a, b| match b.cli_node_idx.cmp(&a.cli_node_idx) {
        std::cmp::Ordering::Equal => b.cli_seq.cmp(&a.cli_seq),
        other => other,
    }); // ORDER BY cli_node_idx DESC, cli_seq DESC

    for node in relevant_nodes.iter() {
        if (node.cli_level - 1) < final_node_list.len().try_into().unwrap() {
            final_node_list[(node.cli_level - 1) as usize] = node.to_owned().clone();
        }
    }

    for (i, (n, nin)) in final_node_list.iter_mut().zip(req_indexes).enumerate() {
        if *n == SimpleChangeLog::default() {
            *n = make_empty_node(i as i64, nin);
        }
    }

    for n in final_node_list.iter() {
        debug!(
            "level {} index {} seq {} hash {}",
            n.cli_level,
            n.cli_node_idx,
            n.cli_seq,
            bs58::encode(&n.cli_hash).into_string()
        );
    }

    let root = bs58::encode(final_node_list.pop().unwrap().cli_hash).into_string();
    let proof: Vec<String> = final_node_list
        .iter()
        .map(|model| bs58::encode(&model.cli_hash).into_string())
        .collect();

    if proof.is_empty() {
        return None;
    }

    Some(AssetProof {
        root,
        leaf: bs58::encode(&leaf.hash).into_string(),
        proof,
        node_index: leaf.node_idx,
        tree_id: bs58::encode(&leaf.tree).into_string(),
    })
}

fn make_empty_node(lvl: i64, node_index: i64) -> SimpleChangeLog {
    SimpleChangeLog {
        cli_node_idx: node_index,
        cli_level: lvl,
        cli_hash: empty_node(lvl as u32).to_vec(),
        cli_seq: 0,
        cli_tree: vec![],
    }
}
