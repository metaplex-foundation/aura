#[cfg(test)]
mod tests {
    use nft_ingester::consistency_calculator::calc_bubblegum_checksums;
    use rocks_db::{
        column::TypedColumn,
        storage_consistency::{
            BubblegumChange, BubblegumChangeKey, BubblegumEpoch, BubblegumEpochKey,
            BubblegumGrandEpochKey, Checksum,
        },
    };
    use setup::rocks::RocksTestEnvironment;
    use solana_sdk::{hash::Hasher, pubkey::Pubkey};

    /// This test checks that checksum calculation for bubblegum contract
    /// correctly calculates checksum for the given epoch,
    /// and only for the given epoch.
    #[tokio::test]
    async fn test_calc_epoch() {
        // prepare
        let tree1 = Pubkey::new_unique();

        let storage = RocksTestEnvironment::new(&[]).storage;

        // This change is for epoch we won't calculate in the test,
        // adding it just to verify it is ignored
        let k0_1 = BubblegumChangeKey::new(tree1, 111, 1);
        let v0_1 = BubblegumChange {
            signature: "1".to_string(),
        };
        storage
            .bubblegum_changes
            .put(k0_1.clone(), v0_1.clone())
            .unwrap();

        // Adding bubblegum changes checksum is calculated of
        let k1_1 = BubblegumChangeKey::new(tree1, 10111, 2);
        let v1_1 = BubblegumChange {
            signature: "2".to_string(),
        };
        storage
            .bubblegum_changes
            .put(k1_1.clone(), v1_1.clone())
            .unwrap();

        let k1_2 = BubblegumChangeKey::new(tree1, 10112, 3);
        let v1_2 = BubblegumChange {
            signature: "3".to_string(),
        };
        storage
            .bubblegum_changes
            .put(k1_2.clone(), v1_2.clone())
            .unwrap();

        // This will be also ignored
        let k2_1 = BubblegumChangeKey::new(tree1, 20000, 4);
        let v2_1 = BubblegumChange {
            signature: "4".to_string(),
        };
        storage
            .bubblegum_changes
            .put(k2_1.clone(), v2_1.clone())
            .unwrap();

        // Calculate epoch and grand epoch checksum
        calc_bubblegum_checksums(&storage, 1).await;

        let expected_epoch_checksum = {
            let mut hasher = Hasher::default();
            hasher.hash(&BubblegumChange::encode_key(k1_1));
            hasher.hash(&bincode::serialize(&v1_1).unwrap());
            hasher.hash(&BubblegumChange::encode_key(k1_2));
            hasher.hash(&bincode::serialize(&v1_2).unwrap());
            hasher.result().to_bytes()
        };

        let epoch_key = BubblegumEpochKey::new(tree1, 1);
        let epoch_val = storage
            .bubblegum_epochs
            .get(epoch_key.clone())
            .unwrap()
            .unwrap();

        assert_eq!(Checksum::Value(expected_epoch_checksum), epoch_val.checksum);

        let expected_grand_epoch_checksum = {
            let mut hasher = Hasher::default();
            hasher.hash(&BubblegumEpoch::encode_key(epoch_key));
            hasher.hash(&bincode::serialize(&epoch_val).unwrap());
            hasher.result().to_bytes()
        };

        let grand_epoch_key = BubblegumGrandEpochKey::new(tree1, 0);
        let grand_epoch_val = storage
            .bubblegum_grand_epochs
            .get(grand_epoch_key)
            .unwrap()
            .unwrap();

        assert_eq!(
            Checksum::Value(expected_grand_epoch_checksum),
            grand_epoch_val.checksum
        );
    }
}
