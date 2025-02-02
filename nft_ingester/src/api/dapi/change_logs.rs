use std::{collections::HashMap, str::FromStr, sync::Arc};

use interface::{
    processing_possibility::ProcessingPossibilityChecker, proofs::ProofChecker,
    slot_getter::LastProcessedSlotGetter,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::{
    clients::asset_streaming_client::get_required_nodes_for_proof,
    columns::cl_items::{ClItemKey, ClLeafKey},
    errors::StorageError,
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use spl_concurrent_merkle_tree::node::empty_node;
use tracing::{debug, warn};

use crate::{
    api::dapi::{model, rpc_asset_models::AssetProof},
    fetch_asset_data,
};

const OFFSET_SLOTS: u64 = 300; // Roughly 2 minutes in Solana time. After this time we consider pending/non-finalized data as invalid and use finalized version instead.

#[derive(Debug, Default, Clone, Eq, PartialEq)]
struct SimpleChangeLog {
    cli_hash: Vec<u8>,
    cli_level: i64,
    cli_node_idx: i64,
    cli_seq: i64,
    cli_tree: Vec<u8>,
}

pub async fn get_proof_for_assets<
    PC: ProofChecker + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
    proof_checker: Option<Arc<PC>>,
    tree_gaps_checker: &Option<Arc<PPC>>,
    metrics: Arc<ApiMetricsConfig>,
) -> Result<HashMap<String, Option<AssetProof>>, StorageError> {
    if let Some(tree_gaps_checker) = tree_gaps_checker {
        if !tree_gaps_checker.can_process_assets(asset_ids.as_slice()).await {
            return Err(StorageError::CannotServiceRequest);
        }
    }

    let mut results: HashMap<String, Option<AssetProof>> =
        asset_ids.iter().map(|id| (id.to_string(), None)).collect();

    // Instead of using a HashMap keyed by tree_id, we keep a Vec of (tree_id, pubkey, nonce).
    let tree_pubkeys: Vec<(Pubkey, Pubkey, u64)> =
        fetch_asset_data!(rocks_db, asset_leaf_data, asset_ids)
            .values()
            .map(|asset| (asset.tree_id, asset.pubkey, asset.nonce.unwrap_or_default()))
            .collect();

    // Construct leaf keys for all requested assets
    let leaf_keys: Vec<ClLeafKey> = tree_pubkeys
        .iter()
        .map(|(tree_id, _pubkey, nonce)| ClLeafKey::new(*nonce, *tree_id))
        .collect();

    // Batch get leaves
    let leaf_entries = rocks_db.cl_leafs.batch_get(leaf_keys.clone()).await?;

    // Create ClItemKeys from the obtained leaves, maintaining the order
    let mut cl_item_keys = Vec::with_capacity(leaf_entries.len());
    for leaf in leaf_entries.iter() {
        if let Some(leaf) = leaf {
            cl_item_keys.push(ClItemKey::new(leaf.cli_node_idx, leaf.cli_tree_key));
        } else {
            // If leaf is not found, push a dummy key. Alternatively, handle it by skipping.
            cl_item_keys.push(ClItemKey::new(0, Pubkey::default()));
        }
    }

    // If no items found at all, return early
    if cl_item_keys.iter().all(|k| k.tree_id == Pubkey::default()) {
        return Ok(HashMap::new());
    }

    // Batch get cl_items
    let cl_items_first_leaf = rocks_db.cl_items.batch_get(cl_item_keys.clone()).await?;

    let slot_for_cutoff = if let Ok(Some(last_seen_slot)) = rocks_db.get_last_ingested_slot().await
    {
        last_seen_slot.wrapping_sub(OFFSET_SLOTS)
    } else {
        0
    };
    // Build the leaves map directly by iterating over tree_pubkeys and the fetched items in parallel
    let leaves: HashMap<Vec<u8>, (model::ClItemsModel, u64)> = tree_pubkeys
        .into_iter()
        .zip(cl_items_first_leaf.into_iter())
        .filter_map(|((tree_id, pubkey, nonce), leaf_opt)| {
            leaf_opt.map(|leaf| {
                let hash = leaf.get_updated_hash(slot_for_cutoff);
                (
                    pubkey.to_bytes().to_vec(),
                    (
                        model::ClItemsModel {
                            id: 0,
                            tree: tree_id.to_bytes().to_vec(),
                            node_idx: leaf.node_idx as i64,
                            leaf_idx: leaf.leaf_idx.map(|idx| idx as i64),
                            level: leaf.level as i64,
                            seq: hash.update_version.and_then(|v| v.get_seq()).unwrap_or_default()
                                as i64,
                            hash: hash.value,
                        },
                        nonce,
                    ),
                )
            })
        })
        .collect();

    // Gather all required nodes for proofs
    let all_req_keys: Vec<_> = cl_item_keys
        .into_iter()
        .flat_map(|ClItemKey { node_id, tree_id }| {
            get_required_nodes_for_proof(node_id as i64)
                .into_iter()
                .map(move |node| ClItemKey::new(node as u64, tree_id))
        })
        .collect();

    let all_nodes = rocks_db
        .cl_items
        .batch_get(all_req_keys)
        .await?
        .into_iter()
        .flatten()
        .map(|node| {
            let hash = node.get_updated_hash(slot_for_cutoff);
            SimpleChangeLog {
                cli_hash: hash.value,
                cli_level: node.level as i64,
                cli_node_idx: node.node_idx as i64,
                cli_seq: hash.update_version.and_then(|v| v.get_seq()).unwrap_or_default() as i64,
                cli_tree: node.tree_key.to_bytes().to_vec(),
            }
        })
        .collect::<Vec<_>>();

    // Compute proofs for each asset
    for asset_id in asset_ids.clone().iter() {
        let proof =
            get_asset_proof(asset_id, &all_nodes, &leaves, proof_checker.clone(), metrics.clone());
        results.insert(asset_id.to_string(), proof);
    }

    Ok(results)
}

fn get_asset_proof(
    asset_id: &Pubkey,
    nodes: &[SimpleChangeLog],
    leaves: &HashMap<Vec<u8>, (model::ClItemsModel, u64)>,
    proof_checker: Option<Arc<impl ProofChecker + Sync + Send + 'static>>,
    metrics: Arc<ApiMetricsConfig>,
) -> Option<AssetProof> {
    let leaf_key = asset_id.to_bytes().to_vec();
    let (leaf, nonce) = match leaves.get(&leaf_key) {
        Some((leaf, nonce)) => (leaf.clone(), *nonce),
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
    let proof: Vec<Vec<u8>> = final_node_list.iter().map(|model| model.cli_hash.clone()).collect();

    if proof.is_empty() {
        return None;
    }

    let tree_id = Pubkey::try_from(leaf.tree.clone()).unwrap_or_default();
    let initial_proofs = proof.iter().filter_map(|k| Pubkey::try_from(k.clone()).ok()).collect();
    let leaf_b58 = bs58::encode(&leaf.hash).into_string();
    if let Some(proof_checker) = proof_checker {
        let lf = Pubkey::from_str(leaf_b58.as_str()).unwrap_or_default();
        let metrics = metrics.clone();
        let cloned_checker = proof_checker.clone();
        let asset_id = *asset_id;
        tokio::spawn(async move {
            match cloned_checker
                .check_proof(tree_id, initial_proofs, nonce as u32, lf.to_bytes())
                .await
            {
                Ok(true) => metrics.inc_proof_checks("proof", metrics_utils::MetricStatus::SUCCESS),
                Ok(false) => {
                    warn!("Proof for asset {:?} of tree {:?} is invalid", asset_id, tree_id);
                    metrics.inc_proof_checks("proof", metrics_utils::MetricStatus::FAILURE)
                },
                Err(e) => {
                    warn!(
                        "Proof check for asset {:?} of tree {:?} failed: {}",
                        asset_id, tree_id, e
                    );
                    metrics.inc_proof_checks("proof", metrics_utils::MetricStatus::FAILURE)
                },
            }
        });
    }
    let proof = proof.iter().map(|model| bs58::encode(model).into_string()).collect();

    Some(AssetProof {
        root,
        leaf: leaf_b58,
        proof,
        node_index: leaf.node_idx,
        tree_id: tree_id.to_string(),
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
