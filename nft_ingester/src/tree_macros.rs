#[macro_export]
macro_rules! _set_tree_paths {
    ($max_depth:literal, $max_size:literal, $bubblegum_instructions:ident) => {{
        let mut tree = Box::new(
            spl_concurrent_merkle_tree::concurrent_merkle_tree::ConcurrentMerkleTree::<
                $max_depth,
                $max_size,
            >::new(),
        );
        tree.initialize()?;
        for instruction in $bubblegum_instructions.iter_mut() {
            if let Some(leaf_hash) = instruction.leaf_update.as_ref().map(|leaf| leaf.leaf_hash) {
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

                instruction.tree_update.as_mut().map(|tree_update| {
                    tree_update.path = path;
                    tree_update.index = changelog.index;
                });
            }
        }
        Ok(())
    }};
}

#[macro_export]
macro_rules! set_tree_paths {
    ($max_depth:ident, $max_buffer_size:ident, $bubblegum_instructions:ident) => {
        // Note: max_buffer_size MUST be a power of 2
        match ($max_depth, $max_buffer_size) {
            (3, 8) => _set_tree_paths!(3, 8, $bubblegum_instructions),
            (5, 8) => _set_tree_paths!(5, 8, $bubblegum_instructions),
            (6, 16) => _set_tree_paths!(6, 16, $bubblegum_instructions),
            (7, 16) => _set_tree_paths!(7, 16, $bubblegum_instructions),
            (8, 16) => _set_tree_paths!(8, 16, $bubblegum_instructions),
            (9, 16) => _set_tree_paths!(9, 16, $bubblegum_instructions),
            (10, 32) => _set_tree_paths!(10, 32, $bubblegum_instructions),
            (11, 32) => _set_tree_paths!(11, 32, $bubblegum_instructions),
            (12, 32) => _set_tree_paths!(12, 32, $bubblegum_instructions),
            (13, 32) => _set_tree_paths!(13, 32, $bubblegum_instructions),
            (14, 64) => _set_tree_paths!(14, 64, $bubblegum_instructions),
            (14, 256) => _set_tree_paths!(14, 256, $bubblegum_instructions),
            (14, 1024) => _set_tree_paths!(14, 1024, $bubblegum_instructions),
            (14, 2048) => _set_tree_paths!(14, 2048, $bubblegum_instructions),
            (15, 64) => _set_tree_paths!(15, 64, $bubblegum_instructions),
            (16, 64) => _set_tree_paths!(16, 64, $bubblegum_instructions),
            (17, 64) => _set_tree_paths!(17, 64, $bubblegum_instructions),
            (18, 64) => _set_tree_paths!(18, 64, $bubblegum_instructions),
            (19, 64) => _set_tree_paths!(19, 64, $bubblegum_instructions),
            (20, 64) => _set_tree_paths!(20, 64, $bubblegum_instructions),
            (20, 256) => _set_tree_paths!(20, 256, $bubblegum_instructions),
            (20, 1024) => _set_tree_paths!(20, 1024, $bubblegum_instructions),
            (20, 2048) => _set_tree_paths!(20, 2048, $bubblegum_instructions),
            (24, 64) => _set_tree_paths!(24, 64, $bubblegum_instructions),
            (24, 256) => _set_tree_paths!(24, 256, $bubblegum_instructions),
            (24, 512) => _set_tree_paths!(24, 512, $bubblegum_instructions),
            (24, 1024) => _set_tree_paths!(24, 1024, $bubblegum_instructions),
            (24, 2048) => _set_tree_paths!(24, 2048, $bubblegum_instructions),
            (26, 512) => _set_tree_paths!(26, 512, $bubblegum_instructions),
            (26, 1024) => _set_tree_paths!(26, 1024, $bubblegum_instructions),
            (26, 2048) => _set_tree_paths!(26, 2048, $bubblegum_instructions),
            (30, 512) => _set_tree_paths!(30, 512, $bubblegum_instructions),
            (30, 1024) => _set_tree_paths!(30, 1024, $bubblegum_instructions),
            (30, 2048) => _set_tree_paths!(30, 2048, $bubblegum_instructions),
            _ => Err(IngesterError::CannotCreateMerkleTree(
                $max_depth,
                $max_buffer_size,
            )),
        }
    };
}
