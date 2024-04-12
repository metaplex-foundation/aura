#[macro_export]
macro_rules! _check_proof {
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
        println!("active index: {}", tree.active_index);
        println!("sequence_number: {}", tree.sequence_number);
        println!("rightmost_proof: {:?}", tree.rightmost_proof);
        println!("change_logs: {:?}", tree.change_logs);
        println!("buffer_size: {}", tree.buffer_size);
        Ok(tree.check_valid_proof($leaf, &proof, $leaf_index))
    }};
}

#[macro_export]
macro_rules! check_proof {
    (&$header:ident, $($arg:tt)*) => {
        // Note: max_buffer_size MUST be a power of 2
        match ($header.get_max_depth(), $header.get_max_buffer_size()) {
            (3, 8) => _check_proof!(3, 8, $($arg)*),
            (5, 8) => _check_proof!(5, 8, $($arg)*),
            (6, 16) => _check_proof!(6, 16, $($arg)*),
            (7, 16) => _check_proof!(7, 16, $($arg)*),
            (8, 16) => _check_proof!(8, 16, $($arg)*),
            (9, 16) => _check_proof!(9, 16, $($arg)*),
            (10, 32) => _check_proof!(10, 32, $($arg)*),
            (11, 32) => _check_proof!(11, 32, $($arg)*),
            (12, 32) => _check_proof!(12, 32, $($arg)*),
            (13, 32) => _check_proof!(13, 32, $($arg)*),
            (14, 64) => _check_proof!(14, 64, $($arg)*),
            (14, 256) => _check_proof!(14, 256, $($arg)*),
            (14, 1024) => _check_proof!(14, 1024, $($arg)*),
            (14, 2048) => _check_proof!(14, 2048, $($arg)*),
            (15, 64) => _check_proof!(15, 64, $($arg)*),
            (16, 64) => _check_proof!(16, 64, $($arg)*),
            (17, 64) => _check_proof!(17, 64, $($arg)*),
            (18, 64) => _check_proof!(18, 64, $($arg)*),
            (19, 64) => _check_proof!(19, 64, $($arg)*),
            (20, 64) => _check_proof!(20, 64, $($arg)*),
            (20, 256) => _check_proof!(20, 256, $($arg)*),
            (20, 1024) => _check_proof!(20, 1024, $($arg)*),
            (20, 2048) => _check_proof!(20, 2048, $($arg)*),
            (24, 64) => _check_proof!(24, 64, $($arg)*),
            (24, 256) => _check_proof!(24, 256, $($arg)*),
            (24, 512) => _check_proof!(24, 512, $($arg)*),
            (24, 1024) => _check_proof!(24, 1024, $($arg)*),
            (24, 2048) => _check_proof!(24, 2048, $($arg)*),
            (26, 512) => _check_proof!(26, 512, $($arg)*),
            (26, 1024) => _check_proof!(26, 1024, $($arg)*),
            (26, 2048) => _check_proof!(26, 2048, $($arg)*),
            (30, 512) => _check_proof!(30, 512, $($arg)*),
            (30, 1024) => _check_proof!(30, 1024, $($arg)*),
            (30, 2048) => _check_proof!(30, 2048, $($arg)*),
            _ => Err(IntegrityVerificationError::CannotCreateMerkleTree($header.get_max_depth(), $header.get_max_buffer_size())),
        }
    };
}
