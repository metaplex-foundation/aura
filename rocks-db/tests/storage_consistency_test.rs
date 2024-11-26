#[cfg(test)]
mod test {

    use interface::checksums_storage::{
        BbgmChangePos, BbgmChangeRecord, BbgmChecksumServiceApi, BbgmEpochCksm, BbgmGrandEpochCksm,
        BbgmGrandEpochCksmWithNumber,
    };
    use rocks_db::storage_consistency::{track_slot_counter, Checksum};
    use rocks_db::storage_consistency::{
        BubblegumChange, BubblegumChangeKey, BubblegumEpoch, BubblegumEpochKey,
        BubblegumGrandEpoch, BubblegumGrandEpochKey,
    };
    use setup::rocks::RocksTestEnvironment;
    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    async fn test_list_bbgm_grand_epochs() {
        let storage = RocksTestEnvironment::new(&[]).storage;

        let tree_1 = Pubkey::new_unique();
        let ge_1_key = BubblegumGrandEpochKey {
            grand_epoch_num: 0,
            tree_pubkey: tree_1,
        };
        let ge_1_val = BubblegumGrandEpoch {
            checksum: Checksum::Value([0u8; 32]),
        };
        storage
            .bubblegum_grand_epochs
            .put(ge_1_key, ge_1_val)
            .unwrap();

        let result = storage
            .list_grand_epoch_checksums(0, None, None)
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![BbgmGrandEpochCksm {
                tree_pubkey: tree_1,
                checksum: Some([0u8; 32])
            }]
        );
    }

    #[tokio::test]
    async fn test_list_bbgm_epoch_checksums() {
        let storage = RocksTestEnvironment::new(&[]).storage;

        let tree_1 = Pubkey::new_unique();

        let epoch_0_key = BubblegumEpochKey {
            tree_pubkey: tree_1,
            epoch_num: 0,
        };
        let epoch_0_val = BubblegumEpoch {
            checksum: Checksum::Value([1u8; 32]),
        };

        let epoch_1_key = BubblegumEpochKey {
            tree_pubkey: tree_1,
            epoch_num: 1,
        };
        let epoch_1_val = BubblegumEpoch {
            checksum: Checksum::Value([2u8; 32]),
        };

        storage
            .bubblegum_epochs
            .put(epoch_0_key, epoch_0_val)
            .unwrap();
        storage
            .bubblegum_epochs
            .put(epoch_1_key, epoch_1_val)
            .unwrap();

        let result = storage.list_epoch_checksums(0, tree_1).await.unwrap();
        assert_eq!(
            result,
            vec![
                BbgmEpochCksm {
                    epoch: 0,
                    tree_pubkey: tree_1,
                    checksum: Some([1u8; 32])
                },
                BbgmEpochCksm {
                    epoch: 1,
                    tree_pubkey: tree_1,
                    checksum: Some([2u8; 32])
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_list_bbgm_epoch_changes() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let tree_1 = Pubkey::new_unique();

        let bbgm_change_1_key = BubblegumChangeKey {
            epoch: 0,
            tree_pubkey: tree_1,
            slot: 5,
            seq: 1,
        };
        let bbgm_change_1_val = BubblegumChange {
            signature: "1".to_string(),
        };

        let bbgm_change_2_key = BubblegumChangeKey {
            epoch: 0,
            tree_pubkey: tree_1,
            slot: 6,
            seq: 2,
        };
        let bbgm_change_2_val = BubblegumChange {
            signature: "2".to_string(),
        };

        let bbgm_change_3_key = BubblegumChangeKey {
            epoch: 0,
            tree_pubkey: tree_1,
            slot: 7,
            seq: 3,
        };
        let bbgm_change_3_val = BubblegumChange {
            signature: "3".to_string(),
        };

        storage
            .bubblegum_changes
            .put(bbgm_change_1_key, bbgm_change_1_val)
            .unwrap();
        storage
            .bubblegum_changes
            .put(bbgm_change_2_key, bbgm_change_2_val)
            .unwrap();
        storage
            .bubblegum_changes
            .put(bbgm_change_3_key, bbgm_change_3_val)
            .unwrap();

        let result_1 = storage
            .list_epoch_changes(0, tree_1, Some(1), None)
            .await
            .unwrap();
        assert_eq!(
            result_1,
            vec![BbgmChangeRecord {
                tree_pubkey: tree_1,
                slot: 5,
                seq: 1,
                signature: "1".to_string()
            }]
        );

        let result_2 = storage
            .list_epoch_changes(0, tree_1, Some(1), Some(BbgmChangePos { slot: 6, seq: 7 }))
            .await
            .unwrap();
        assert_eq!(
            result_2,
            vec![BbgmChangeRecord {
                tree_pubkey: tree_1,
                slot: 7,
                seq: 3,
                signature: "3".to_string()
            }]
        );
    }

    #[tokio::test]
    async fn test_list_bbgm_grand_epoch_for_tree() {
        let storage = RocksTestEnvironment::new(&[]).storage;

        track_slot_counter(900_001); // max grand epoch is 9

        let tree_1 = Pubkey::new_unique();
        let tree_2 = Pubkey::new_unique();

        {
            let k = BubblegumGrandEpochKey {
                grand_epoch_num: 5,
                tree_pubkey: tree_1,
            };
            let v = BubblegumGrandEpoch {
                checksum: Checksum::Value([5u8; 32]),
            };
            storage.bubblegum_grand_epochs.put(k, v).unwrap();
        }
        {
            let k = BubblegumGrandEpochKey {
                grand_epoch_num: 6,
                tree_pubkey: tree_2,
            };
            let v = BubblegumGrandEpoch {
                checksum: Checksum::Value([6u8; 32]),
            };
            storage.bubblegum_grand_epochs.put(k, v).unwrap();
        }
        {
            let k = BubblegumGrandEpochKey {
                grand_epoch_num: 7,
                tree_pubkey: tree_1,
            };
            let v = BubblegumGrandEpoch {
                checksum: Checksum::Value([7u8; 32]),
            };
            storage.bubblegum_grand_epochs.put(k, v).unwrap();
        }
        {
            let k = BubblegumGrandEpochKey {
                grand_epoch_num: 9,
                tree_pubkey: tree_1,
            };
            let v = BubblegumGrandEpoch {
                checksum: Checksum::Value([9u8; 32]),
            };
            storage.bubblegum_grand_epochs.put(k, v).unwrap();
        }

        let result = storage
            .list_bbgm_grand_epoch_for_tree(tree_1)
            .await
            .unwrap();

        assert_eq!(
            result,
            vec![
                BbgmGrandEpochCksmWithNumber {
                    grand_epoch: 5,
                    checksum: Some([5u8; 32])
                },
                BbgmGrandEpochCksmWithNumber {
                    grand_epoch: 7,
                    checksum: Some([7u8; 32])
                },
                BbgmGrandEpochCksmWithNumber {
                    grand_epoch: 9,
                    checksum: Some([9u8; 32])
                },
            ]
        );
    }
}
