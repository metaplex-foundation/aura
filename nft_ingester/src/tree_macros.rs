use crate::error::RollupValidationError;
use entities::rollup::Rollup;
use solana_program::keccak::Hash;
macro_rules! _validate_change_logs {
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
                            mint.tree_update.id.to_string(),
                        ));
                    }
                    if mint.tree_update.id != $rollup.tree_id {
                        return Err(RollupValidationError::WrongTreeIdForChangeLog(
                            mint.tree_update.id.to_string(),
                            $rollup.tree_id.to_string(),
                            mint.tree_update.id.to_string(),
                        ));
                    }
                    if mint.tree_update.index != changelog.index {
                        return Err(RollupValidationError::WrongChangeLogIndex(
                            changelog.index,
                            mint.tree_update.index,
                        ));
                    }
                }
                None => return Err(RollupValidationError::NoRelevantRolledMint(i as u64)),
            }
        }
        if tree.get_root() != $rollup.merkle_root {
            return Err(RollupValidationError::InvalidLRoot(
                Hash::new(tree.get_root().as_slice()).to_string(),
                Hash::new($rollup.merkle_root.as_slice()).to_string(),
            ));
        }
        Ok(())
    }};
}

macro_rules! validate_change_logs {
    ($max_depth:ident, $max_buffer_size:ident, $leafs:ident, $rollup:ident) => {{
        // Note: max_buffer_size MUST be a power of 2
        match ($max_depth, $max_buffer_size) {
            (3, 8) => _validate_change_logs!(3, 8, $leafs, $rollup),
            (5, 8) => _validate_change_logs!(5, 8, $leafs, $rollup),
            (6, 16) => _validate_change_logs!(6, 16, $leafs, $rollup),
            (7, 16) => _validate_change_logs!(7, 16, $leafs, $rollup),
            (8, 16) => _validate_change_logs!(8, 16, $leafs, $rollup),
            (9, 16) => _validate_change_logs!(9, 16, $leafs, $rollup),
            (10, 32) => _validate_change_logs!(10, 32, $leafs, $rollup),
            (11, 32) => _validate_change_logs!(11, 32, $leafs, $rollup),
            (12, 32) => _validate_change_logs!(12, 32, $leafs, $rollup),
            (13, 32) => _validate_change_logs!(13, 32, $leafs, $rollup),
            (14, 64) => _validate_change_logs!(14, 64, $leafs, $rollup),
            (14, 256) => _validate_change_logs!(14, 256, $leafs, $rollup),
            (14, 1024) => _validate_change_logs!(14, 1024, $leafs, $rollup),
            (14, 2048) => _validate_change_logs!(14, 2048, $leafs, $rollup),
            (15, 64) => _validate_change_logs!(15, 64, $leafs, $rollup),
            (16, 64) => _validate_change_logs!(16, 64, $leafs, $rollup),
            (17, 64) => _validate_change_logs!(17, 64, $leafs, $rollup),
            (18, 64) => _validate_change_logs!(18, 64, $leafs, $rollup),
            (19, 64) => _validate_change_logs!(19, 64, $leafs, $rollup),
            (20, 64) => _validate_change_logs!(20, 64, $leafs, $rollup),
            (20, 256) => _validate_change_logs!(20, 256, $leafs, $rollup),
            (20, 1024) => _validate_change_logs!(20, 1024, $leafs, $rollup),
            (20, 2048) => _validate_change_logs!(20, 2048, $leafs, $rollup),
            (24, 64) => _validate_change_logs!(24, 64, $leafs, $rollup),
            (24, 256) => _validate_change_logs!(24, 256, $leafs, $rollup),
            (24, 512) => _validate_change_logs!(24, 512, $leafs, $rollup),
            (24, 1024) => _validate_change_logs!(24, 1024, $leafs, $rollup),
            (24, 2048) => _validate_change_logs!(24, 2048, $leafs, $rollup),
            (26, 512) => _validate_change_logs!(26, 512, $leafs, $rollup),
            (26, 1024) => _validate_change_logs!(26, 1024, $leafs, $rollup),
            (26, 2048) => _validate_change_logs!(26, 2048, $leafs, $rollup),
            (30, 512) => _validate_change_logs!(30, 512, $leafs, $rollup),
            (30, 1024) => _validate_change_logs!(30, 1024, $leafs, $rollup),
            (30, 2048) => _validate_change_logs!(30, 2048, $leafs, $rollup),
            _ => Err(RollupValidationError::CannotCreateMerkleTree(
                $max_depth,
                $max_buffer_size,
            )),
        }
    }};
}

pub fn validate_change_logs(
    max_depth: u32,
    max_buffer_size: u32,
    leafs: &[[u8; 32]],
    rollup: &Rollup,
) -> Result<(), RollupValidationError> {
    validate_change_logs!(max_depth, max_buffer_size, leafs, rollup)
}
