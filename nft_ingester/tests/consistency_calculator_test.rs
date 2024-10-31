#[cfg(test)]
mod tests {
    use nft_ingester::consistency_calculator::ConsistencyCalcMsg;
    use nft_ingester::consistency_calculator::{
        calc_acc_nft_checksums, calc_bubblegum_checksums, NftChangesTracker,
    };
    use rocks_db::{
        column::TypedColumn,
        storage_consistency::{
            AccountNft, AccountNftBucketKey, AccountNftChange, AccountNftChangeKey, AccountNftKey,
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
        calc_bubblegum_checksums(&storage, 1, None).await;

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

    #[tokio::test]
    async fn test_calc_acc_nft_checksums() {
        let storage = RocksTestEnvironment::new(&[]).storage;

        let acc_change_val = AccountNftChange {};

        let acc1_pubkey = make_pubkey_in_bucket(0);
        let acc1_change_key1 = AccountNftChangeKey {
            epoch: 0,
            account_pubkey: acc1_pubkey,
            slot: 11,
            write_version: 1,
        };
        let acc1_change_key2 = AccountNftChangeKey {
            epoch: 0,
            account_pubkey: acc1_pubkey,
            slot: 12,
            write_version: 2,
        };
        // won't take part in calculation
        let acc1_change_key3 = AccountNftChangeKey {
            epoch: 1,
            account_pubkey: acc1_pubkey,
            slot: 10_0001,
            write_version: 10,
        };
        storage
            .acc_nft_changes
            .put(acc1_change_key1.clone(), acc_change_val.clone())
            .unwrap();
        storage
            .acc_nft_changes
            .put(acc1_change_key2.clone(), acc_change_val.clone())
            .unwrap();
        storage
            .acc_nft_changes
            .put(acc1_change_key3.clone(), acc_change_val.clone())
            .unwrap();

        let acc2_pubkey = make_pubkey_in_bucket(1);
        let acc2_change_key1 = AccountNftChangeKey {
            epoch: 0,
            account_pubkey: acc2_pubkey,
            slot: 21,
            write_version: 21,
        };
        storage
            .acc_nft_changes
            .put(acc2_change_key1.clone(), acc_change_val.clone())
            .unwrap();

        // SUT
        calc_acc_nft_checksums(&storage, 0).await;

        // Verify account last state updated
        let latest_acc1_key = AccountNftKey::new(acc1_pubkey);
        let latest_acc1_val = storage
            .acc_nft_last
            .get(latest_acc1_key.clone())
            .unwrap()
            .unwrap();
        assert_eq!(latest_acc1_val.last_slot, acc1_change_key2.slot);
        assert_eq!(
            latest_acc1_val.last_write_version,
            acc1_change_key2.write_version
        );

        let latest_acc2_key = AccountNftKey::new(acc2_pubkey);
        let latest_acc2_val = storage
            .acc_nft_last
            .get(latest_acc2_key.clone())
            .unwrap()
            .unwrap();
        assert_eq!(latest_acc2_val.last_slot, acc2_change_key1.slot);
        assert_eq!(
            latest_acc2_val.last_write_version,
            acc2_change_key1.write_version
        );

        // Verify buckets are updated
        let bucket0_val = storage
            .acc_nft_buckets
            .get(AccountNftBucketKey::new(0))
            .unwrap()
            .unwrap();
        let bucket1_val = storage
            .acc_nft_buckets
            .get(AccountNftBucketKey::new(1))
            .unwrap()
            .unwrap();

        let expected_bucket0_checksum = {
            let mut hasher = Hasher::default();
            hasher.hash(&AccountNft::encode_key(latest_acc1_key.clone()));
            hasher.hash(&bincode::serialize(&latest_acc1_val).unwrap());
            hasher.result().to_bytes()
        };
        let expected_bucket1_checksum = {
            let mut hasher = Hasher::default();
            hasher.hash(&AccountNft::encode_key(latest_acc2_key.clone()));
            hasher.hash(&bincode::serialize(&latest_acc2_val).unwrap());
            hasher.result().to_bytes()
        };
        assert_eq!(
            bucket0_val.checksum,
            Checksum::Value(expected_bucket0_checksum)
        );
        assert_eq!(
            bucket1_val.checksum,
            Checksum::Value(expected_bucket1_checksum)
        );
    }

    #[tokio::test]
    async fn test_notification_on_epoch_change() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1000);
        let sut = NftChangesTracker::new(storage, sender);

        let tree = Pubkey::new_unique();

        sut.watch_bubblegum_change(tree, 90_001).await;
        sut.watch_bubblegum_change(tree, 100_001).await;

        assert_eq!(
            Ok(ConsistencyCalcMsg::EpochChanged { new_epoch: 10 }),
            receiver.try_recv()
        );
        assert_eq!(
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
            receiver.try_recv()
        );
    }

    fn make_pubkey_in_bucket(bucket: u16) -> Pubkey {
        let mut arr = Pubkey::new_unique().to_bytes();
        let bucket_arr = bucket.to_be_bytes();
        arr[0] = bucket_arr[0];
        arr[1] = bucket_arr[1];
        Pubkey::new_from_array(arr)
    }
}
