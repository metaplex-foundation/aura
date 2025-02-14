#[cfg(test)]
mod tests {
    use entities::models::{RawBlock, RawBlockDeprecated, RawBlockWithTransactions};
    use rocks_db::column::TypedColumn;
    use setup::rocks::RocksTestEnvironment;

    #[tokio::test]
    async fn test_raw_block_encoding() {
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
            .put_cf(
                &env1.slot_storage.db.cf_handle(RawBlockDeprecated::NAME).unwrap(),
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
                        &env1.slot_storage.db.cf_handle(RawBlockDeprecated::NAME).unwrap(),
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
