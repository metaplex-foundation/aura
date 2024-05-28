use crate::error::RollupValidationError;
use entities::rollup::Rollup;
use solana_program::keccak::Hash;
use spl_account_compression::state::ConcurrentMerkleTreeHeader;
use spl_account_compression::zero_copy::ZeroCopy;
macro_rules! check_proof {
    ($max_depth:literal, $max_size:literal, &$bytes:ident, $initial_proofs:ident, $leaf:ident, $leaf_index:ident) => {{
        let mut proof: [spl_concurrent_merkle_tree::node::Node; $max_depth] =
            [spl_concurrent_merkle_tree::node::Node::default(); $max_depth];
        spl_concurrent_merkle_tree::hash::fill_in_proof::<$max_depth>(
            $initial_proofs.as_ref(),
            &mut proof,
        );

        let tree = spl_concurrent_merkle_tree::concurrent_merkle_tree::ConcurrentMerkleTree::<
            $max_depth,
            $max_size,
        >::load_bytes($bytes)?;
        Ok(tree.check_valid_proof($leaf, &proof, $leaf_index))
    }};
}

macro_rules! validate_change_logs {
    ($max_depth:literal, $max_size:literal, $leafs:ident, $rollup:ident) => {{
        let mut tree = Box::new(
            spl_concurrent_merkle_tree::concurrent_merkle_tree::ConcurrentMerkleTree::<
                $max_depth,
                $max_size,
            >::new(),
        );
        tree.initialize()?;
        for (i, leaf_hash) in $leafs.iter().enumerate() {
            tree.append(*leaf_hash)?;
            let changelog = tree.change_logs[tree.active_index as usize];
            let path_len = changelog.path.len() as u32;
            let mut path: Vec<spl_account_compression::state::PathNode> = changelog
                .path
                .iter()
                .enumerate()
                .map(|(lvl, n)| {
                    spl_account_compression::state::PathNode::new(
                        *n,
                        (1 << (path_len - lvl as u32)) + (changelog.index >> lvl),
                    )
                })
                .collect();
            path.push(spl_account_compression::state::PathNode::new(
                changelog.root,
                1,
            ));

            match $rollup.rolled_mints.get(i) {
                Some(mint) => {
                    if mint.tree_update.path
                        != path
                            .into_iter()
                            .map(Into::<entities::rollup::PathNode>::into)
                            .collect::<Vec<_>>()
                    {
                        return Err(RollupValidationError::WrongAssetPath(
                            mint.leaf_update.id().to_string(),
                        ));
                    }
                    if mint.tree_update.id != $rollup.tree_id {
                        return Err(RollupValidationError::WrongTreeIdForChangeLog(
                            mint.leaf_update.id().to_string(),
                            $rollup.tree_id.to_string(),
                            mint.tree_update.id.to_string(),
                        ));
                    }
                    if mint.tree_update.index != changelog.index {
                        return Err(RollupValidationError::WrongChangeLogIndex(
                            mint.leaf_update.id().to_string(),
                            changelog.index,
                            mint.tree_update.index,
                        ));
                    }
                }
                None => return Err(RollupValidationError::NoRelevantRolledMint(i as u64)),
            }
        }
        if tree.get_root() != $rollup.merkle_root {
            return Err(RollupValidationError::InvalidRoot(
                Hash::new(tree.get_root().as_slice()).to_string(),
                Hash::new($rollup.merkle_root.as_slice()).to_string(),
            ));
        }
        Ok(())
    }};
}

macro_rules! process_merkle_tree {
    ($macro_name:ident, $max_depth:ident, $max_buffer_size:ident, $($arg:tt)*) => {
        match ($max_depth, $max_buffer_size) {
            (3, 8) => $macro_name!(3, 8, $($arg)*),
            (5, 8) => $macro_name!(5, 8, $($arg)*),
            (6, 16) => $macro_name!(6, 16, $($arg)*),
            (7, 16) => $macro_name!(7, 16, $($arg)*),
            (8, 16) => $macro_name!(8, 16, $($arg)*),
            (9, 16) => $macro_name!(9, 16, $($arg)*),
            (10, 32) => $macro_name!(10, 32, $($arg)*),
            (11, 32) => $macro_name!(11, 32, $($arg)*),
            (12, 32) => $macro_name!(12, 32, $($arg)*),
            (13, 32) => $macro_name!(13, 32, $($arg)*),
            (14, 64) => $macro_name!(14, 64, $($arg)*),
            (14, 256) => $macro_name!(14, 256, $($arg)*),
            (14, 1024) => $macro_name!(14, 1024, $($arg)*),
            (14, 2048) => $macro_name!(14, 2048, $($arg)*),
            (15, 64) => $macro_name!(15, 64, $($arg)*),
            (16, 64) => $macro_name!(16, 64, $($arg)*),
            (17, 64) => $macro_name!(17, 64, $($arg)*),
            (18, 64) => $macro_name!(18, 64, $($arg)*),
            (19, 64) => $macro_name!(19, 64, $($arg)*),
            (20, 64) => $macro_name!(20, 64, $($arg)*),
            (20, 256) => $macro_name!(20, 256, $($arg)*),
            (20, 1024) => $macro_name!(20, 1024, $($arg)*),
            (20, 2048) => $macro_name!(20, 2048, $($arg)*),
            (24, 64) => $macro_name!(24, 64, $($arg)*),
            (24, 256) => $macro_name!(24, 256, $($arg)*),
            (24, 512) => $macro_name!(24, 512, $($arg)*),
            (24, 1024) => $macro_name!(24, 1024, $($arg)*),
            (24, 2048) => $macro_name!(24, 2048, $($arg)*),
            (26, 512) => $macro_name!(26, 512, $($arg)*),
            (26, 1024) => $macro_name!(26, 1024, $($arg)*),
            (26, 2048) => $macro_name!(26, 2048, $($arg)*),
            (30, 512) => $macro_name!(30, 512, $($arg)*),
            (30, 1024) => $macro_name!(30, 1024, $($arg)*),
            (30, 2048) => $macro_name!(30, 2048, $($arg)*),
            _ => Err(RollupValidationError::CannotCreateMerkleTree($max_depth, $max_buffer_size)),
        }
    };
}

pub fn validate_change_logs(
    max_depth: u32,
    max_buffer_size: u32,
    leafs: &[[u8; 32]],
    rollup: &Rollup,
) -> Result<(), RollupValidationError> {
    process_merkle_tree!(
        validate_change_logs,
        max_depth,
        max_buffer_size,
        leafs,
        rollup
    )
}

pub fn check_proof(
    header: &ConcurrentMerkleTreeHeader,
    bytes: &[u8],
    initial_proofs: Vec<[u8; 32]>,
    leaf: [u8; 32],
    leaf_index: u32,
) -> Result<bool, RollupValidationError> {
    let max_depth = header.get_max_depth();
    let max_buffer_size = header.get_max_buffer_size();
    process_merkle_tree!(
        check_proof,
        max_depth,
        max_buffer_size,
        &bytes,
        initial_proofs,
        leaf,
        leaf_index
    )
}
