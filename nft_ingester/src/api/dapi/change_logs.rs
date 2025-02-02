use std::{collections::HashMap, str::FromStr, sync::Arc};

use entities::models::Updated;
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
use solana_sdk::{keccak, pubkey::Pubkey};
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
    let leaves: HashMap<Vec<u8>, (model::ClItemsModel, u64, Option<Updated<Vec<u8>>>)> =
        tree_pubkeys
            .into_iter()
            .zip(cl_items_first_leaf.into_iter())
            .filter_map(|((tree_id, pubkey, nonce), leaf_opt)| {
                leaf_opt.map(|leaf| {
                    let (hash, alt_hash) =
                        leaf.get_hash_with_finalized_alternative(slot_for_cutoff);
                    (
                        pubkey.to_bytes().to_vec(),
                        (
                            model::ClItemsModel {
                                id: 0,
                                tree: tree_id.to_bytes().to_vec(),
                                node_idx: leaf.node_idx as i64,
                                leaf_idx: leaf.leaf_idx.map(|idx| idx as i64),
                                level: leaf.level as i64,
                                seq: hash
                                    .update_version
                                    .and_then(|v| v.get_seq())
                                    .unwrap_or_default()
                                    as i64,
                                hash: hash.value,
                            },
                            nonce,
                            alt_hash,
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
            let (hash, alt_hash) = node.get_hash_with_finalized_alternative(slot_for_cutoff);
            (
                SimpleChangeLog {
                    cli_hash: hash.value,
                    cli_level: node.level as i64,
                    cli_node_idx: node.node_idx as i64,
                    cli_seq: hash.update_version.and_then(|v| v.get_seq()).unwrap_or_default()
                        as i64,
                    cli_tree: node.tree_key.to_bytes().to_vec(),
                },
                alt_hash,
            )
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
    nodes: &[(SimpleChangeLog, Option<Updated<Vec<u8>>>)],
    leaves: &HashMap<Vec<u8>, (model::ClItemsModel, u64, Option<Updated<Vec<u8>>>)>,
    proof_checker: Option<Arc<impl ProofChecker + Sync + Send + 'static>>,
    metrics: Arc<ApiMetricsConfig>,
) -> Option<AssetProof> {
    let leaf_key = asset_id.to_bytes().to_vec();
    let (leaf, nonce, alt_leaf_hash) = match leaves.get(&leaf_key) {
        Some((leaf, nonce, alt_leaf_hash)) => (leaf.clone(), *nonce, alt_leaf_hash.clone()),
        None => return None,
    };

    let req_indexes = get_required_nodes_for_proof(leaf.node_idx);
    let mut final_node_list: Vec<(SimpleChangeLog, Option<Updated<Vec<u8>>>)> =
        vec![(SimpleChangeLog::default(), None); req_indexes.len()];

    let mut relevant_nodes: Vec<&(SimpleChangeLog, Option<Updated<Vec<u8>>>)> = nodes
        .iter()
        .filter(|node| node.0.cli_tree == leaf.tree && req_indexes.contains(&node.0.cli_node_idx))
        .collect();

    relevant_nodes.sort_by(|(a, _), (b, _)| match b.cli_node_idx.cmp(&a.cli_node_idx) {
        std::cmp::Ordering::Equal => b.cli_seq.cmp(&a.cli_seq),
        other => other,
    }); // ORDER BY cli_node_idx DESC, cli_seq DESC

    for node in relevant_nodes.iter() {
        if (node.0.cli_level - 1) < final_node_list.len().try_into().unwrap() {
            final_node_list[(node.0.cli_level - 1) as usize] = node.to_owned().clone();
        }
    }

    for (i, (n, nin)) in final_node_list.iter_mut().zip(req_indexes).enumerate() {
        if n.0 == SimpleChangeLog::default() {
            *n = (make_empty_node(i as i64, nin), None);
        }
    }

    for (n, _) in final_node_list.iter() {
        debug!(
            "level {} index {} seq {} hash {}",
            n.cli_level,
            n.cli_node_idx,
            n.cli_seq,
            bs58::encode(&n.cli_hash).into_string()
        );
    }
    // now I have: the leaf with an optional alternative hash in alt_leaf_hash and the final_node_list, which is a list of nodes with their hashes with an optional alternative hash.
    // the first node in final_node_list is the root of the tree (which might as well have an alternative hash)
    // we need to build the proof from the leaf to the root, if there are alternatives anywhere on the path, we need to pick ones that will provide a valid root.
    // The preference always goes to the non-alternative hash.
    let mut leaf_hash = leaf.hash.clone();
    let mut valid_proof = build_proof(final_node_list.clone(), leaf.hash.clone());
    if valid_proof.is_none() {
        if let Some(alt_leaf_hash) = alt_leaf_hash {
            valid_proof = build_proof(final_node_list, alt_leaf_hash.value.clone());
            if valid_proof.is_some() {
                leaf_hash = alt_leaf_hash.value;
            }
        }
    }
    if valid_proof.is_none() || valid_proof.unwrap().is_empty() {
        return None;
    }
    let valid_proof = valid_proof.unwrap();
    let root = valid_proof.pop().unwrap().cli_hash;
    let proof: Vec<Vec<u8>> = valid_proof.iter().map(|model| model.cli_hash.clone()).collect();

    let tree_id = Pubkey::try_from(leaf.tree.clone()).unwrap_or_default();
    let initial_proofs = proof.iter().filter_map(|k| Pubkey::try_from(k.clone()).ok()).collect();
    let leaf_b58 = bs58::encode(&leaf_hash).into_string();
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
        root: bs58::encode(root).into_string(),
        leaf: leaf_b58,
        proof,
        node_index: leaf.node_idx,
        tree_id: tree_id.to_string(),
    })
}

fn build_proof(
    candidates: Vec<(SimpleChangeLog, Option<Vec<u8>>)>,
    leaf: Vec<u8>,
) -> Option<Vec<SimpleChangeLog>> {
    if candidates.len() < 1 {
        return None;
    }
    if candidates.len() == 1 {
        let (root, alt_root) = &candidates[0];
        if root.cli_hash == leaf {
            return Some(vec![root.clone()]);
        }
        if let Some(alt_root) = alt_root {
            if alt_root == &leaf {
                let mut root = root.clone();
                root.cli_hash = alt_root.clone();
                return Some(vec![root]);
            }
        }
        return None;
    }
    let sibling = candidates[candidates.len() - 1].clone();
    let is_sibling_left = sibling.0.cli_node_idx % 2 == 0;
    let main_root_hash = if is_sibling_left {
        keccak::hashv(&[&sibling.0.cli_hash, &leaf])
    } else {
        keccak::hashv(&[&leaf, &sibling.0.cli_hash])
    };
    if let Some(valid_proof) =
        build_proof(candidates[..candidates.len() - 1].to_vec(), main_root_hash.0.to_vec())
    {
        return Some([valid_proof, vec![sibling.0]].concat());
    }
    if let Some(alt_sibling) = sibling.1 {
        let alt_hash = if is_sibling_left {
            keccak::hashv(&[&alt_sibling, &leaf])
        } else {
            keccak::hashv(&[&leaf, &alt_sibling])
        };
        if let Some(valid_proof) =
            build_proof(candidates[..candidates.len() - 1].to_vec(), alt_hash.0.to_vec())
        {
            let mut sibling = sibling.0.clone();
            sibling.cli_hash = alt_sibling;
            return Some([valid_proof, vec![sibling]].concat());
        }
    }
    return None;
}

#[test]
fn test_build_proof_empty_candidates() {
    let candidates = vec![];
    let leaf = vec![1, 2, 3];
    let result = build_proof(candidates, leaf);
    assert_eq!(result, None, "Empty candidate list should yield None");
}

#[test]
fn test_build_proof_single_primary_match() {
    let single_primary = SimpleChangeLog {
        cli_hash: vec![9, 9, 9],
        cli_level: 0,
        cli_node_idx: 0,
        cli_seq: 0,
        cli_tree: vec![],
    };
    let candidates = vec![(single_primary.clone(), None)];
    let leaf = vec![9, 9, 9]; // exactly matches single_primary.cli_hash

    let result = build_proof(candidates, leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    assert_eq!(proof.len(), 1);
    assert_eq!(proof[0], single_primary);
}
#[test]
fn test_build_proof_single_alt_match() {
    let primary_node = SimpleChangeLog {
        cli_hash: vec![10, 10, 10],
        cli_level: 0,
        cli_node_idx: 0,
        cli_seq: 0,
        cli_tree: vec![],
    };
    let alt_hash = vec![99, 99, 99];
    let candidates = vec![(primary_node.clone(), Some(alt_hash.clone()))];
    let leaf = vec![99, 99, 99]; // matches alt_hash.value

    let result = build_proof(candidates, leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    // we expect the single final node to have cli_hash replaced with alt_hash
    assert_eq!(proof.len(), 1);
    assert_eq!(proof[0].cli_hash, alt_hash);
    // other fields should remain the same
    assert_eq!(proof[0].cli_level, primary_node.cli_level);
    assert_eq!(proof[0].cli_node_idx, primary_node.cli_node_idx);
}

#[test]
fn test_build_proof_single_no_match() {
    let node = SimpleChangeLog {
        cli_hash: vec![10, 10, 10],
        cli_level: 0,
        cli_node_idx: 0,
        cli_seq: 0,
        cli_tree: vec![],
    };
    let alt = vec![99, 99, 99];
    let candidates = vec![(node, Some(alt))];
    let leaf = vec![1, 2, 3]; // does not match either primary or alt

    let result = build_proof(candidates, leaf);
    assert_eq!(result, None);
}

#[test]
fn test_generated_proof_with_length_20() {
    use rand::Rng;
    let mut candidates = vec![];
    let random_bytes: [u8; 32] = rand::thread_rng().gen();
    let mut leaf = random_bytes.to_vec();
    let original_leaf = leaf.clone();
    for i in 0..20 {
        let sibling = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let node = SimpleChangeLog {
            cli_hash: sibling.clone(),
            cli_level: 0,
            cli_node_idx: 1 >> (20 - i) as i64, // keeping it simple - always the left-most node
            cli_seq: 0,
            cli_tree: vec![],
        };
        candidates.push((node, None));
        leaf = keccak::hashv(&[&sibling, &leaf]).0.to_vec();
    }
    let root_node = SimpleChangeLog {
        cli_hash: leaf.clone(),
        cli_level: 0,
        cli_node_idx: 1 as i64,
        cli_seq: 0,
        cli_tree: vec![],
    };
    candidates.push((root_node, None));
    candidates.reverse();
    let result = build_proof(candidates.clone(), original_leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    assert_eq!(proof.len(), 21);
    for (i, node) in proof.iter().enumerate() {
        assert_eq!(node.cli_hash, candidates[i].0.cli_hash);
    }
}

#[test]
#[ignore = "runs for over 40 seconds, although is a valid test"]
fn test_generated_proof_with_length_20_all_alts() {
    use rand::Rng;
    let mut candidates = vec![];
    let random_bytes: [u8; 32] = rand::thread_rng().gen();
    let mut leaf = random_bytes.to_vec();
    let original_leaf = leaf.clone();
    for i in 0..20 {
        let sibling = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let random_shit = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let node = SimpleChangeLog {
            cli_hash: random_shit.clone(),
            cli_level: 0,
            cli_node_idx: 1 >> (20 - i) as i64, // keeping it simple - always the left-most node
            cli_seq: 0,
            cli_tree: vec![],
        };
        candidates.push((node, Some(sibling.clone())));
        leaf = keccak::hashv(&[&sibling, &leaf]).0.to_vec();
    }
    let random_shit = rand::thread_rng().gen::<[u8; 32]>().to_vec();
    let root_node = SimpleChangeLog {
        cli_hash: random_shit,
        cli_level: 0,
        cli_node_idx: 1 as i64,
        cli_seq: 0,
        cli_tree: vec![],
    };
    candidates.push((root_node, Some(leaf.clone())));
    candidates.reverse();
    let result = build_proof(candidates.clone(), original_leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    assert_eq!(proof.len(), 21);
    for (i, node) in proof.iter().enumerate() {
        if let Some(expected) = candidates.get(i).unwrap().1.clone() {
            assert_eq!(node.cli_hash, expected);
        }
    }
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
