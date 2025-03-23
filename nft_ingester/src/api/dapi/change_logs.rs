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

#[derive(Clone, Debug, PartialEq, Eq)]
struct CandidateNode {
    node: SimpleChangeLog,
    alt: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct LeafData {
    model: model::ClItemsModel,
    nonce: u64,
    alt: Option<Updated<Vec<u8>>>,
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
    let leaves: HashMap<Vec<u8>, LeafData> = tree_pubkeys
        .into_iter()
        .zip(cl_items_first_leaf.into_iter())
        .filter_map(|((tree_id, pubkey, nonce), leaf_opt)| {
            leaf_opt.map(|leaf| {
                let (hash, alt_hash) = leaf.get_hash_with_finalized_alternative(slot_for_cutoff);
                (
                    pubkey.to_bytes().to_vec(),
                    LeafData {
                        model: model::ClItemsModel {
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
                        alt: alt_hash,
                    },
                )
            })
        })
        .collect();

    // Gather all required nodes and convert them into CandidateNode objects
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
            CandidateNode {
                node: SimpleChangeLog {
                    cli_hash: hash.value,
                    cli_level: node.level as i64,
                    cli_node_idx: node.node_idx as i64,
                    cli_seq: hash.update_version.and_then(|v| v.get_seq()).unwrap_or_default()
                        as i64,
                    cli_tree: node.tree_key.to_bytes().to_vec(),
                },
                alt: alt_hash.map(|a| a.value),
            }
        })
        .collect::<Vec<_>>();

    // Compute proofs for each asset
    for asset_id in asset_ids.iter() {
        let proof =
            get_asset_proof(asset_id, &all_nodes, &leaves, proof_checker.clone(), metrics.clone());
        results.insert(asset_id.to_string(), proof);
    }

    Ok(results)
}

fn get_asset_proof(
    asset_id: &Pubkey,
    nodes: &[CandidateNode],
    leaves: &HashMap<Vec<u8>, LeafData>,
    proof_checker: Option<Arc<impl ProofChecker + Sync + Send + 'static>>,
    metrics: Arc<ApiMetricsConfig>,
) -> Option<AssetProof> {
    let leaf_key = asset_id.to_bytes().to_vec();
    let leaf_data = match leaves.get(&leaf_key) {
        Some(data) => data.clone(),
        None => return None,
    };

    let req_indexes = get_required_nodes_for_proof(leaf_data.model.node_idx);
    let mut final_node_list: Vec<CandidateNode> =
        vec![CandidateNode { node: SimpleChangeLog::default(), alt: None }; req_indexes.len()];

    // Filter nodes to only those that belong to the correct tree and required indexes.
    let mut relevant_nodes: Vec<&CandidateNode> = nodes
        .iter()
        .filter(|candidate| {
            candidate.node.cli_tree == leaf_data.model.tree
                && req_indexes.contains(&candidate.node.cli_node_idx)
        })
        .collect();

    // Sort by cli_node_idx (descending), then by cli_seq (descending)
    relevant_nodes.sort_by(|a, b| match b.node.cli_node_idx.cmp(&a.node.cli_node_idx) {
        std::cmp::Ordering::Equal => b.node.cli_seq.cmp(&a.node.cli_seq),
        other => other,
    });

    // Populate the final candidate list by level.
    for candidate in relevant_nodes.iter() {
        let level_index = (candidate.node.cli_level - 1) as usize;
        if level_index < final_node_list.len() {
            final_node_list[level_index] = (*candidate).clone();
        }
    }

    // Fill any missing nodes with an empty node.
    for (i, nin) in req_indexes.into_iter().enumerate() {
        if final_node_list[i].node == SimpleChangeLog::default() {
            final_node_list[i] = CandidateNode { node: make_empty_node(i as i64, nin), alt: None };
        }
    }

    // Debug logging.
    for candidate in final_node_list.iter() {
        debug!(
            "level {} index {} seq {} hash {}",
            candidate.node.cli_level,
            candidate.node.cli_node_idx,
            candidate.node.cli_seq,
            bs58::encode(&candidate.node.cli_hash).into_string()
        );
    }
    // now I have: the leaf with an optional alternative hash in alt_leaf_hash and the final_node_list, which is a list of nodes with their hashes with an optional alternative hash.
    // the first node in final_node_list is the root of the tree (which might as well have an alternative hash)
    // we need to build the proof from the leaf to the root, if there are alternatives anywhere on the path, we need to pick ones that will provide a valid root.
    // The preference always goes to the non-alternative hash.

    // Choose the preferred leaf hash.
    // Prefer the primary if it is non-empty; otherwise use the alternative.
    let mut leaf_hash = leaf_data.model.hash.clone();
    // Build the proof using the preferred (primary) leaf hash first.
    let mut valid_proof = build_proof(&final_node_list, &leaf_data.model.hash);
    if valid_proof.is_none() {
        if let Some(ref alt_leaf_hash) = leaf_data.alt {
            valid_proof = build_proof(&final_node_list, &alt_leaf_hash.value);
            if valid_proof.is_some() {
                leaf_hash = alt_leaf_hash.value.clone();
            }
        }
    }
    if valid_proof.is_none() {
        warn!(
            ?final_node_list,
            ?leaf_data,
            ?leaf_data.alt,
            "Proof for asset {:?} is invalid",
            asset_id
        );
        return None;
    }
    let mut valid_proof = valid_proof.unwrap();
    if valid_proof.is_empty() {
        return None;
    }
    let root = valid_proof.pop().unwrap().cli_hash;
    let proof: Vec<Vec<u8>> = valid_proof.iter().map(|node| node.cli_hash.clone()).collect();

    let tree_id = Pubkey::try_from(leaf_data.model.tree.clone()).unwrap_or_default();
    let initial_proofs = proof.iter().filter_map(|k| Pubkey::try_from(k.clone()).ok()).collect();
    let leaf_b58 = bs58::encode(&leaf_hash).into_string();

    if let Some(proof_checker) = proof_checker {
        let lf = Pubkey::from_str(leaf_b58.as_str()).unwrap_or_default();
        let metrics = metrics.clone();
        let cloned_checker = proof_checker.clone();
        let asset_id = *asset_id;
        usecase::executor::spawn(async move {
            match cloned_checker
                .check_proof(tree_id, initial_proofs, leaf_data.nonce as u32, lf.to_bytes())
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
    let proof = proof.iter().map(|node| bs58::encode(node).into_string()).collect();

    Some(AssetProof {
        root: bs58::encode(root).into_string(),
        leaf: leaf_b58,
        proof,
        node_index: leaf_data.model.node_idx,
        tree_id: tree_id.to_string(),
    })
}

fn build_proof(candidates: &[CandidateNode], leaf: &[u8]) -> Option<Vec<SimpleChangeLog>> {
    if candidates.is_empty() {
        return None;
    }
    if candidates.len() == 1 {
        let candidate = &candidates[0];
        if candidate.node.cli_hash == leaf {
            return Some(vec![candidate.node.clone()]);
        }
        if let Some(ref alt_val) = candidate.alt {
            if alt_val == leaf {
                let mut node_clone = candidate.node.clone();
                node_clone.cli_hash = alt_val.clone();
                return Some(vec![node_clone]);
            }
        }
        return None;
    }
    // Split off the first candidate and keep the rest as a slice.
    let (first, rest) = candidates.split_first().unwrap();
    let is_left = first.node.cli_node_idx % 2 == 0;

    // Compute the hash with the primary hash.
    let main_root_hash = if is_left {
        keccak::hashv(&[&first.node.cli_hash, leaf]).0.to_vec()
    } else {
        keccak::hashv(&[leaf, &first.node.cli_hash]).0.to_vec()
    };

    if let Some(mut proof) = build_proof(rest, &main_root_hash) {
        proof.insert(0, first.node.clone());
        return Some(proof);
    }

    // If the primary failed, try the alternative (if present).
    if let Some(ref alt_val) = first.alt {
        let alt_hash = if is_left {
            keccak::hashv(&[alt_val, leaf]).0.to_vec()
        } else {
            keccak::hashv(&[leaf, alt_val]).0.to_vec()
        };
        if let Some(mut proof) = build_proof(rest, &alt_hash) {
            let mut node_clone = first.node.clone();
            node_clone.cli_hash = alt_val.clone();
            proof.insert(0, node_clone);
            return Some(proof);
        }
    }
    None
}

#[test]
fn test_build_proof_empty_candidates() {
    let candidates: Vec<CandidateNode> = vec![];
    let leaf = vec![1, 2, 3];
    let result = build_proof(&candidates, &leaf);
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
    let candidates = vec![CandidateNode { node: single_primary.clone(), alt: None }];
    let leaf = vec![9, 9, 9]; // exactly matches single_primary.cli_hash

    let result = build_proof(&candidates, &leaf);
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
    let candidates =
        vec![CandidateNode { node: primary_node.clone(), alt: Some(alt_hash.clone()) }];
    let leaf = vec![99, 99, 99]; // matches alt_hash

    let result = build_proof(&candidates, &leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    // Expect the single final node to have cli_hash replaced with alt_hash
    assert_eq!(proof.len(), 1);
    assert_eq!(proof[0].cli_hash, alt_hash);
    // Other fields should remain the same
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
    let candidates = vec![CandidateNode { node, alt: Some(alt) }];
    let leaf = vec![1, 2, 3]; // does not match either primary or alt

    let result = build_proof(&candidates, &leaf);
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
        candidates.push(CandidateNode { node, alt: None });
        leaf = keccak::hashv(&[&sibling, &leaf]).0.to_vec();
    }
    let root_node = SimpleChangeLog {
        cli_hash: leaf.clone(),
        cli_level: 0,
        cli_node_idx: 1 as i64,
        cli_seq: 0,
        cli_tree: vec![],
    };
    candidates.push(CandidateNode { node: root_node, alt: None });
    let result = build_proof(&candidates, &original_leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    assert_eq!(proof.len(), 21);
    for (i, node) in proof.iter().enumerate() {
        assert_eq!(node.cli_hash, candidates[i].node.cli_hash);
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
            cli_node_idx: 1 << (21 - i) as i64, // keeping it simple - always the left-most node
            cli_seq: 0,
            cli_tree: vec![],
        };
        candidates.push(CandidateNode { node, alt: Some(sibling.clone()) });
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
    candidates.push(CandidateNode { node: root_node, alt: Some(leaf.clone()) });

    let result = build_proof(&candidates, &original_leaf);
    assert!(result.is_some());
    let proof = result.unwrap();
    assert_eq!(proof.len(), 21);
    for (i, node) in proof.iter().enumerate() {
        if let Some(expected) = candidates.get(i).unwrap().alt.clone() {
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

#[test]
pub fn test_valid_mainline_with_invalid_alternatives() {
    let cli_tree = vec![
        51, 110, 42, 4, 221, 82, 221, 8, 14, 42, 227, 168, 2, 155, 0, 127, 46, 104, 120, 38, 189,
        7, 197, 4, 225, 14, 93, 183, 39, 79, 1, 65,
    ];
    let final_node_list = vec![
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    247, 41, 158, 116, 39, 111, 5, 180, 211, 148, 144, 29, 169, 76, 166, 7, 52,
                    220, 20, 16, 232, 138, 18, 246, 190, 179, 5, 109, 143, 136, 187, 50,
                ],
                cli_level: 1,
                cli_node_idx: 16877711,
                cli_seq: 100515,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    73, 34, 61, 36, 227, 245, 69, 236, 116, 10, 14, 227, 226, 213, 91, 63, 39, 71,
                    95, 51, 35, 206, 140, 253, 90, 23, 15, 218, 126, 190, 134, 135,
                ],
                cli_level: 2,
                cli_node_idx: 8438854,
                cli_seq: 100513,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    220, 218, 104, 238, 42, 40, 245, 102, 189, 57, 17, 218, 111, 212, 80, 172, 32,
                    242, 74, 81, 186, 13, 214, 183, 165, 227, 118, 87, 37, 2, 86, 245,
                ],
                cli_level: 3,
                cli_node_idx: 4219426,
                cli_seq: 100511,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    81, 68, 74, 119, 16, 168, 37, 102, 206, 125, 124, 124, 132, 239, 167, 243, 183,
                    57, 196, 135, 27, 4, 43, 157, 239, 49, 12, 255, 168, 227, 205, 107,
                ],
                cli_level: 4,
                cli_node_idx: 2109712,
                cli_seq: 100507,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    93, 214, 21, 70, 101, 14, 91, 94, 114, 226, 78, 55, 233, 141, 155, 205, 21,
                    223, 157, 204, 61, 135, 169, 230, 105, 205, 249, 121, 151, 188, 194, 152,
                ],
                cli_level: 5,
                cli_node_idx: 1054857,
                cli_seq: 100531,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    67, 109, 221, 244, 43, 77, 127, 38, 33, 36, 177, 183, 61, 38, 177, 14, 138, 82,
                    60, 132, 180, 189, 224, 244, 190, 2, 196, 165, 225, 44, 170, 122,
                ],
                cli_level: 6,
                cli_node_idx: 527429,
                cli_seq: 100563,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    220, 198, 188, 149, 105, 14, 24, 37, 143, 71, 156, 25, 8, 197, 74, 133, 169,
                    241, 11, 11, 116, 154, 249, 14, 174, 137, 253, 167, 77, 108, 41, 105,
                ],
                cli_level: 7,
                cli_node_idx: 263715,
                cli_seq: 100627,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    219, 229, 230, 233, 178, 17, 36, 39, 122, 128, 207, 71, 73, 182, 2, 162, 123,
                    235, 43, 71, 242, 207, 235, 220, 175, 56, 134, 114, 230, 249, 102, 157,
                ],
                cli_level: 8,
                cli_node_idx: 131856,
                cli_seq: 16845866,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    203, 106, 187, 169, 161, 204, 84, 238, 16, 79, 156, 93, 181, 49, 134, 60, 232,
                    31, 137, 32, 36, 216, 147, 36, 46, 87, 21, 234, 217, 22, 71, 19,
                ],
                cli_level: 9,
                cli_node_idx: 65929,
                cli_seq: 16852129,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    229, 91, 43, 96, 177, 165, 4, 129, 206, 144, 205, 1, 161, 233, 200, 127, 62,
                    143, 41, 109, 28, 227, 8, 28, 110, 11, 51, 145, 208, 116, 64, 94,
                ],
                cli_level: 10,
                cli_node_idx: 32965,
                cli_seq: 2483344,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    18, 26, 217, 144, 121, 63, 234, 155, 78, 249, 114, 108, 167, 88, 233, 81, 249,
                    203, 104, 160, 165, 135, 26, 25, 186, 170, 119, 5, 63, 122, 56, 4,
                ],
                cli_level: 11,
                cli_node_idx: 16483,
                cli_seq: 16858475,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    8, 45, 237, 33, 93, 211, 227, 199, 41, 91, 20, 194, 98, 93, 44, 228, 135, 243,
                    30, 168, 80, 235, 177, 163, 177, 101, 7, 110, 164, 218, 210, 25,
                ],
                cli_level: 12,
                cli_node_idx: 8240,
                cli_seq: 16853212,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    220, 31, 60, 152, 167, 62, 246, 34, 171, 69, 120, 204, 195, 234, 33, 47, 77,
                    98, 193, 195, 214, 139, 123, 13, 3, 156, 87, 165, 177, 164, 88, 191,
                ],
                cli_level: 13,
                cli_node_idx: 4121,
                cli_seq: 16855791,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    233, 106, 120, 106, 253, 180, 90, 75, 114, 41, 242, 39, 233, 194, 217, 10, 245,
                    218, 12, 11, 21, 43, 77, 178, 145, 165, 250, 22, 99, 254, 116, 188,
                ],
                cli_level: 14,
                cli_node_idx: 2061,
                cli_seq: 16855780,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    195, 143, 3, 113, 213, 80, 219, 63, 6, 144, 169, 243, 36, 106, 165, 2, 78, 246,
                    223, 237, 240, 233, 169, 105, 87, 213, 221, 178, 101, 33, 131, 7,
                ],
                cli_level: 15,
                cli_node_idx: 1031,
                cli_seq: 16859416,
                cli_tree: cli_tree.clone(),
            },
            alt: Some(vec![
                162, 252, 216, 169, 227, 252, 119, 77, 83, 252, 196, 157, 53, 129, 205, 6, 72, 233,
                155, 22, 160, 66, 44, 126, 138, 126, 122, 153, 136, 44, 0, 164,
            ]),
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    58, 156, 228, 251, 242, 206, 130, 55, 219, 45, 103, 71, 57, 203, 107, 17, 78,
                    167, 38, 1, 219, 33, 115, 240, 57, 147, 90, 224, 44, 122, 124, 185,
                ],
                cli_level: 16,
                cli_node_idx: 514,
                cli_seq: 16858734,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    56, 86, 228, 90, 46, 150, 206, 116, 94, 31, 141, 6, 247, 217, 83, 141, 173,
                    233, 201, 140, 114, 8, 95, 230, 81, 167, 144, 122, 231, 82, 95, 146,
                ],
                cli_level: 17,
                cli_node_idx: 256,
                cli_seq: 16861318,
                cli_tree: cli_tree.clone(),
            },
            alt: Some(vec![
                200, 12, 56, 255, 235, 109, 167, 225, 141, 47, 119, 219, 146, 93, 138, 188, 106,
                248, 120, 239, 67, 35, 98, 65, 68, 203, 14, 42, 10, 43, 100, 154,
            ]),
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    135, 212, 16, 109, 254, 152, 133, 104, 178, 220, 47, 34, 177, 45, 46, 44, 87,
                    153, 253, 82, 152, 93, 115, 128, 44, 47, 221, 211, 85, 55, 139, 101,
                ],
                cli_level: 18,
                cli_node_idx: 129,
                cli_seq: 16861764,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    74, 138, 103, 209, 49, 235, 212, 22, 198, 41, 175, 25, 211, 185, 217, 207, 177,
                    176, 119, 96, 202, 46, 131, 102, 4, 220, 160, 238, 16, 104, 230, 110,
                ],
                cli_level: 19,
                cli_node_idx: 65,
                cli_seq: 16861760,
                cli_tree: cli_tree.clone(),
            },
            alt: Some(vec![
                147, 130, 232, 145, 154, 148, 105, 167, 157, 98, 96, 57, 136, 193, 168, 97, 32,
                130, 118, 244, 72, 127, 74, 4, 120, 236, 143, 69, 129, 194, 120, 199,
            ]),
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    106, 76, 162, 225, 172, 41, 172, 5, 172, 189, 81, 103, 219, 159, 145, 219, 59,
                    6, 163, 26, 238, 23, 230, 248, 28, 120, 244, 99, 99, 90, 41, 168,
                ],
                cli_level: 20,
                cli_node_idx: 33,
                cli_seq: 16861767,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    235, 175, 80, 8, 181, 52, 52, 129, 180, 76, 62, 171, 0, 216, 85, 180, 225, 174,
                    210, 255, 31, 107, 48, 177, 247, 240, 131, 251, 130, 2, 13, 221,
                ],
                cli_level: 21,
                cli_node_idx: 17,
                cli_seq: 16861770,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    191, 55, 102, 208, 117, 212, 135, 127, 240, 91, 30, 72, 0, 194, 10, 130, 178,
                    161, 228, 218, 1, 23, 34, 212, 172, 60, 252, 25, 62, 145, 240, 127,
                ],
                cli_level: 22,
                cli_node_idx: 9,
                cli_seq: 16861774,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    30, 72, 161, 219, 233, 200, 175, 221, 197, 29, 140, 155, 123, 7, 149, 243, 41,
                    106, 169, 32, 84, 11, 52, 207, 30, 81, 115, 16, 245, 187, 255, 112,
                ],
                cli_level: 23,
                cli_node_idx: 5,
                cli_seq: 16861779,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    201, 47, 223, 252, 48, 225, 24, 144, 42, 233, 9, 216, 204, 219, 142, 98, 115,
                    118, 174, 81, 31, 100, 121, 122, 213, 134, 255, 231, 0, 186, 168, 152,
                ],
                cli_level: 24,
                cli_node_idx: 3,
                cli_seq: 16861776,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
        CandidateNode {
            node: SimpleChangeLog {
                cli_hash: vec![
                    203, 225, 45, 209, 200, 233, 137, 229, 61, 78, 138, 108, 44, 79, 234, 25, 189,
                    253, 33, 129, 57, 239, 131, 72, 160, 27, 166, 177, 118, 194, 241, 183,
                ],
                cli_level: 25,
                cli_node_idx: 1,
                cli_seq: 16861779,
                cli_tree: cli_tree.clone(),
            },
            alt: None,
        },
    ];
    let leaf = vec![
        228, 119, 100, 82, 32, 252, 105, 143, 204, 55, 100, 0, 137, 216, 104, 138, 227, 10, 217,
        111, 36, 181, 226, 19, 169, 224, 40, 205, 31, 160, 142, 218,
    ];
    let proof = build_proof(&final_node_list, &leaf);
    assert!(proof.is_some());
    for (i, node) in proof.unwrap().iter().enumerate() {
        assert_eq!(node.cli_hash, final_node_list[i].node.cli_hash);
    }
}
