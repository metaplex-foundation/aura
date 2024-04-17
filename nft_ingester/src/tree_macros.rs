#[macro_export]
macro_rules! _set_tree_paths {
    ($max_depth:literal, $max_size:literal, $leafs:ident, $rollup:ident) => {{
        let mut tree = Box::new(
            spl_concurrent_merkle_tree::concurrent_merkle_tree::ConcurrentMerkleTree::<
                $max_depth,
                $max_size,
            >::new(),
        );
        tree.initialize()?;
        for (i, leaf_hash) in $leafs.iter().enumerate() {
            tree.append(leaf_hash)?;
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
                    if mint.path != path {
                        return RollupValidationError::WrongAssetPath(mint.id.to_string());
                    }
                }
                None => return RollupValidationError::NoRelevantRolledMint(i as u64),
            }
        }
        Ok(())
    }};
}

#[macro_export]
macro_rules! set_tree_paths {
    ($max_depth:ident, $max_buffer_size:ident, $leafs:ident, $bubblegum_instructions:ident) => {
        // Note: max_buffer_size MUST be a power of 2
        match ($max_depth, $max_buffer_size) {
            (3, 8) => _set_tree_paths!(3, 8, $leafs, $bubblegum_instructions),
            (5, 8) => _set_tree_paths!(5, 8, $leafs, $bubblegum_instructions),
            (6, 16) => _set_tree_paths!(6, 16, $leafs, $bubblegum_instructions),
            (7, 16) => _set_tree_paths!(7, 16, $leafs, $bubblegum_instructions),
            (8, 16) => _set_tree_paths!(8, 16, $leafs, $bubblegum_instructions),
            (9, 16) => _set_tree_paths!(9, 16, $leafs, $bubblegum_instructions),
            (10, 32) => _set_tree_paths!(10, 32, $leafs, $bubblegum_instructions),
            (11, 32) => _set_tree_paths!(11, 32, $leafs, $bubblegum_instructions),
            (12, 32) => _set_tree_paths!(12, 32, $leafs, $bubblegum_instructions),
            (13, 32) => _set_tree_paths!(13, 32, $leafs, $bubblegum_instructions),
            (14, 64) => _set_tree_paths!(14, 64, $leafs, $bubblegum_instructions),
            (14, 256) => _set_tree_paths!(14, 256, $leafs, $bubblegum_instructions),
            (14, 1024) => _set_tree_paths!(14, 1024, $leafs, $bubblegum_instructions),
            (14, 2048) => _set_tree_paths!(14, 2048, $leafs, $bubblegum_instructions),
            (15, 64) => _set_tree_paths!(15, 64, $leafs, $bubblegum_instructions),
            (16, 64) => _set_tree_paths!(16, 64, $leafs, $bubblegum_instructions),
            (17, 64) => _set_tree_paths!(17, 64, $leafs, $bubblegum_instructions),
            (18, 64) => _set_tree_paths!(18, 64, $leafs, $bubblegum_instructions),
            (19, 64) => _set_tree_paths!(19, 64, $leafs, $bubblegum_instructions),
            (20, 64) => _set_tree_paths!(20, 64, $leafs, $bubblegum_instructions),
            (20, 256) => _set_tree_paths!(20, 256, $leafs, $bubblegum_instructions),
            (20, 1024) => _set_tree_paths!(20, 1024, $leafs, $bubblegum_instructions),
            (20, 2048) => _set_tree_paths!(20, 2048, $leafs, $bubblegum_instructions),
            (24, 64) => _set_tree_paths!(24, 64, $leafs, $bubblegum_instructions),
            (24, 256) => _set_tree_paths!(24, 256, $leafs, $bubblegum_instructions),
            (24, 512) => _set_tree_paths!(24, 512, $leafs, $bubblegum_instructions),
            (24, 1024) => _set_tree_paths!(24, 1024, $leafs, $bubblegum_instructions),
            (24, 2048) => _set_tree_paths!(24, 2048, $leafs, $bubblegum_instructions),
            (26, 512) => _set_tree_paths!(26, 512, $leafs, $bubblegum_instructions),
            (26, 1024) => _set_tree_paths!(26, 1024, $leafs, $bubblegum_instructions),
            (26, 2048) => _set_tree_paths!(26, 2048, $leafs, $bubblegum_instructions),
            (30, 512) => _set_tree_paths!(30, 512, $leafs, $bubblegum_instructions),
            (30, 1024) => _set_tree_paths!(30, 1024, $leafs, $bubblegum_instructions),
            (30, 2048) => _set_tree_paths!(30, 2048, $leafs, $bubblegum_instructions),
            _ => Err(RollupValidationError::CannotCreateMerkleTree(
                $max_depth,
                $max_buffer_size,
            )),
        }
    };
}
