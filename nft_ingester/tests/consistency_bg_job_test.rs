#[cfg(test)]
mod tests {
    use interface::checksums_storage::*;
    use metrics_utils::Peer2PeerConsistencyMetricsConfig;
    use nft_ingester::consistency_bg_job::compare_bbgm_with_peer;
    use nft_ingester::consistency_calculator::calc_bubblegum_checksums;
    use rocks_db::storage_consistency::*;
    use setup::rocks::RocksTestEnvironment;
    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    pub async fn test_checksum_exchange() {
        let my_storage = RocksTestEnvironment::new(&[]).storage;

        let peer_storage = RocksTestEnvironment::new(&[]).storage;

        // prepare
        let tree1 = Pubkey::new_unique();

        let k0_1 = BubblegumChangeKey::new(tree1, 111, 1);
        let v0_1 = BubblegumChange {
            signature: "1".to_string(),
        };
        peer_storage
            .bubblegum_changes
            .put(k0_1.clone(), v0_1.clone())
            .unwrap();

        // Adding bubblegum changes checksum is calculated of
        let k1_1 = BubblegumChangeKey::new(tree1, 10111, 2);
        let v1_1 = BubblegumChange {
            signature: "2".to_string(),
        };
        peer_storage
            .bubblegum_changes
            .put(k1_1.clone(), v1_1.clone())
            .unwrap();

        let k1_2 = BubblegumChangeKey::new(tree1, 10112, 3);
        let v1_2 = BubblegumChange {
            signature: "3".to_string(),
        };
        peer_storage
            .bubblegum_changes
            .put(k1_2.clone(), v1_2.clone())
            .unwrap();

        let k2_1 = BubblegumChangeKey::new(tree1, 20000, 4);
        let v2_1 = BubblegumChange {
            signature: "4".to_string(),
        };
        peer_storage
            .bubblegum_changes
            .put(k2_1.clone(), v2_1.clone())
            .unwrap();

        // Calculate epoch and grand epoch checksum
        calc_bubblegum_checksums(&peer_storage, 0, None).await;
        calc_bubblegum_checksums(&peer_storage, 1, None).await;
        calc_bubblegum_checksums(&peer_storage, 2, None).await;

        let metrics = Peer2PeerConsistencyMetricsConfig::new();
        let result =
            compare_bbgm_with_peer(0, my_storage.as_ref(), peer_storage.as_ref(), &metrics).await;

        assert_eq!(
            result,
            vec![
                BbgmChangeRecord {
                    tree_pubkey: tree1,
                    slot: 111,
                    seq: 1,
                    signature: "1".to_string()
                },
                BbgmChangeRecord {
                    tree_pubkey: tree1,
                    slot: 10111,
                    seq: 2,
                    signature: "2".to_string()
                },
                BbgmChangeRecord {
                    tree_pubkey: tree1,
                    slot: 10112,
                    seq: 3,
                    signature: "3".to_string()
                },
                BbgmChangeRecord {
                    tree_pubkey: tree1,
                    slot: 20000,
                    seq: 4,
                    signature: "4".to_string()
                }
            ]
        );
    }
}
