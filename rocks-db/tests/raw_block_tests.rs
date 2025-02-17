#[cfg(test)]
mod tests {
    use entities::models::{RawBlock, RawBlockDeprecated, RawBlockWithTransactions};
    use rocks_db::column::TypedColumn;
    use setup::rocks::RocksTestEnvironment;

    const RAW_BLOCK_DEPRECATED_CF_NAME: &str = "RAW_BLOCK_CBOR_ENCODED";

    #[tokio::test]
    async fn test_raw_block_encoding() {
        // case 1: mock data
        let raw_block_deprecated = RawBlockDeprecated {
            slot: 1,
            block: solana_transaction_status::UiConfirmedBlock {
                previous_blockhash: "prev".to_owned(),
                blockhash: "hash".to_owned(),
                parent_slot: 1,
                transactions: None,
                signatures: None,
                rewards: None,
                block_time: None,
                block_height: None,
            },
        };
        let raw_block: RawBlock = raw_block_deprecated.into();

        assert_eq!(
            raw_block,
            RawBlock {
                slot: 1,
                block: RawBlockWithTransactions {
                    blockhash: "hash".to_owned(),
                    previous_blockhash: "prev".to_owned(),
                    parent_slot: 1,
                    block_time: None,
                    transactions: Default::default(),
                }
            }
        );

        let encoded = RawBlock::encode(&raw_block).expect("encode raw block");
        assert_eq!(raw_block, RawBlock::decode(&encoded).expect("decode raw block"));

        // case 2: real data
        let raw_block_deprecated_cbor_bytes = solana_sdk::bs58::decode("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").into_vec().unwrap();
        let raw_block_deprecated: RawBlockDeprecated =
            serde_cbor::from_slice(&raw_block_deprecated_cbor_bytes).unwrap();

        let raw_block: RawBlock = raw_block_deprecated.into();
        let encoded = RawBlock::encode(&raw_block).unwrap();
        assert_eq!(RawBlock::decode(&encoded).unwrap(), raw_block);
    }

    #[tokio::test]
    async fn test_raw_block_migration() {
        let env1 = RocksTestEnvironment::new(&[]);
        let raw_block_deprecated = RawBlockDeprecated {
            slot: 1,
            block: solana_transaction_status::UiConfirmedBlock {
                previous_blockhash: "prev".to_owned(),
                blockhash: "hash".to_owned(),
                parent_slot: 1,
                transactions: None,
                signatures: None,
                rewards: None,
                block_time: None,
                block_height: None,
            },
        };
        env1.slot_storage
            .db
            .create_cf(RAW_BLOCK_DEPRECATED_CF_NAME, &rocksdb::Options::default())
            .expect("create deprecated cf");
        env1.slot_storage
            .db
            .put_cf(
                &env1.slot_storage.db.cf_handle(RAW_BLOCK_DEPRECATED_CF_NAME).unwrap(),
                RawBlockDeprecated::encode_key(1),
                RawBlockDeprecated::encode(&raw_block_deprecated).expect("encode raw block"),
            )
            .expect("put raw block into the storage");
        assert_eq!(
            RawBlockDeprecated::decode(
                &env1
                    .slot_storage
                    .db
                    .get_cf(
                        &env1.slot_storage.db.cf_handle(RAW_BLOCK_DEPRECATED_CF_NAME).unwrap(),
                        RawBlockDeprecated::encode_key(1)
                    )
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            raw_block_deprecated
        );
        let raw_block: RawBlock = raw_block_deprecated.into();
        env1.slot_storage
            .db
            .put_cf(
                &env1.slot_storage.db.cf_handle(RawBlock::NAME).unwrap(),
                RawBlock::encode_key(1),
                RawBlock::encode(&raw_block).expect("encode raw block"),
            )
            .expect("put raw block into the storage");
        assert_eq!(
            RawBlock::decode(
                &env1
                    .slot_storage
                    .db
                    .get_cf(
                        &env1.slot_storage.db.cf_handle(RawBlock::NAME).unwrap(),
                        RawBlock::encode_key(1)
                    )
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            raw_block
        );
    }
}
