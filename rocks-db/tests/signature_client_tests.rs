#[cfg(test)]
mod tests {
    use entities::models::SignatureWithSlot;
    use interface::signature_persistence::SignaturePersistence;
    use setup::rocks::*;
    use solana_sdk::{pubkey::Pubkey, signature::Signature};

    #[tokio::test]
    async fn test_first_persisted_signature_for_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let program_id = Pubkey::new_unique();

        let response = storage.first_persisted_signature_for(program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_none());
    }

    #[tokio::test]
    async fn test_persist_and_get_first_for_different_keys() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let first_program_id = Pubkey::new_unique();
        let second_program_id = Pubkey::new_unique();

        let first_signature = SignatureWithSlot { signature: Signature::new_unique(), slot: 100 };
        assert!(storage.persist_signature(first_program_id, first_signature.clone()).await.is_ok());

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), first_signature);

        let second_signature = SignatureWithSlot { signature: Signature::new_unique(), slot: 101 };

        assert!(storage
            .persist_signature(first_program_id, second_signature.clone())
            .await
            .is_ok());

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), first_signature, "first signature should not change");
        // simulate an older signature being persisted, although this should never happen

        let third_signature = SignatureWithSlot { signature: Signature::new_unique(), slot: 99 };
        assert!(storage.persist_signature(first_program_id, third_signature.clone()).await.is_ok());

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), third_signature, "first signature should change");

        let response = storage.first_persisted_signature_for(second_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(
            response.is_none(),
            "first program should not affect signatures for the second program id"
        );
    }

    #[tokio::test]
    async fn test_drop_signatures_before_for_empty_db() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let program_id = Pubkey::new_unique();

        let response = storage.drop_signatures_before(program_id, Default::default()).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_drop_signatures_before_on_generated_records_for_2_accounts() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let first_program_id = Pubkey::new_unique();
        let second_program_id = Pubkey::new_unique();
        // generate 1000 signatures for the first program id each with a 100 + i slot
        let first_signatures: Vec<SignatureWithSlot> = (0..1000)
            .map(|i| SignatureWithSlot { signature: Signature::new_unique(), slot: 100 + i })
            .collect();
        // generate 1000 signatures for the second program id each with a 150 + i slot
        let second_signatures: Vec<SignatureWithSlot> = (0..1000)
            .map(|i| SignatureWithSlot { signature: Signature::new_unique(), slot: 150 + i })
            .collect();
        // persist the signatures
        for signature in first_signatures.iter() {
            assert!(storage.persist_signature(first_program_id, signature.clone()).await.is_ok());
        }
        for signature in second_signatures.iter() {
            assert!(storage.persist_signature(second_program_id, signature.clone()).await.is_ok());
        }

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), first_signatures[0]);

        let response = storage.first_persisted_signature_for(second_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), second_signatures[0]);

        // drop signatures for the second program that are before 200th signature
        let expected_new_first_signature_for_second_account = second_signatures[200].clone();
        let response = storage
            .drop_signatures_before(
                second_program_id,
                expected_new_first_signature_for_second_account.clone(),
            )
            .await;
        assert!(response.is_ok());

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), first_signatures[0], "first signature should not change");

        let response = storage.first_persisted_signature_for(second_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(
            response.unwrap(),
            expected_new_first_signature_for_second_account,
            "first signature should change"
        );
    }

    #[tokio::test]
    async fn test_drop_signatures_before_in_the_same_slot_faking_signature() {
        let storage = RocksTestEnvironment::new(&[]).storage;
        let first_program_id = Pubkey::new_unique();
        // generate 1000 signatures for the first program id each with a 100 slot
        let first_signatures: Vec<SignatureWithSlot> = (0..1000)
            .map(|_| SignatureWithSlot { signature: Signature::new_unique(), slot: 100 })
            .collect();

        // persist the signatures
        for signature in first_signatures.iter() {
            assert!(storage.persist_signature(first_program_id, signature.clone()).await.is_ok());
        }

        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        let first_signature = response.unwrap();

        // droping a signature with a slot before the first signature should not change anything
        let fake_signature = SignatureWithSlot { signature: Default::default(), slot: 100 };

        assert!(storage.drop_signatures_before(first_program_id, fake_signature).await.is_ok());
        let response = storage.first_persisted_signature_for(first_program_id).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert_eq!(response.unwrap(), first_signature, "first signature should not change");
    }
}
