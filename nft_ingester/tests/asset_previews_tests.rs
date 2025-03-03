#[cfg(test)]
mod tests {
    use entities::enums::OwnershipModel;
    use itertools::Itertools;
    use nft_ingester::api::dapi::{
        asset_preview::populate_previews_opt,
        rpc_asset_models::{Asset, Content, File, MetadataMap, Ownership},
    };
    use rocks_db::columns::asset_previews::AssetPreviews;
    use setup::rocks::RocksTestEnvironment;
    use solana_sdk::keccak::{self, HASH_BYTES};

    #[tokio::test(flavor = "multi_thread")]
    #[tracing_test::traced_test]
    async fn test_replace_file_urls_with_preview_urls() {
        let rocks_env = RocksTestEnvironment::new(&[]);

        // given
        let files = vec![
            vec!["http://host/url1".to_string(), "http://host/url2".to_string()],
            vec!["http://host/url3".to_string()],
        ];

        rocks_env
            .storage
            .asset_previews
            .put(keccak::hash("http://host/url1".as_bytes()).to_bytes(), AssetPreviews::new(400))
            .unwrap();
        rocks_env
            .storage
            .asset_previews
            .put(keccak::hash("http://host/url3".as_bytes()).to_bytes(), AssetPreviews::new(400))
            .unwrap();

        let mut assets = files.into_iter().map(|v| Some(test_asset_for_content(v))).collect_vec();

        // when
        populate_previews_opt("http://storage/", &rocks_env.storage, &mut assets).await.unwrap();

        // expected
        assert_eq!(
            assets[0].as_ref().unwrap().content.as_ref().unwrap().files.as_ref().unwrap()[0]
                .cdn_uri
                .as_ref()
                .unwrap()
                .clone(),
            format!(
                "http://storage/preview/{}",
                keccak::hash("http://host/url1".as_bytes()).to_string()
            ),
        );

        assert_eq!(
            assets[0].as_ref().unwrap().content.as_ref().unwrap().files.as_ref().unwrap()[1]
                .cdn_uri,
            None,
        );

        assert_eq!(
            assets[1].as_ref().unwrap().content.as_ref().unwrap().files.as_ref().unwrap()[0]
                .cdn_uri
                .as_ref()
                .unwrap()
                .clone(),
            format!(
                "http://storage/preview/{}",
                keccak::hash("http://host/url3".as_bytes()).to_string()
            ),
        );
    }

    fn test_asset_for_content(urls: Vec<String>) -> Asset {
        let files = urls
            .into_iter()
            .map(|url| File {
                uri: Some(url),
                cdn_uri: None,
                mime: None,
                quality: None,
                contexts: None,
            })
            .collect_vec();
        Asset {
            interface: entities::enums::Interface::Nft,
            id: "".to_string(),
            content: Some(Content {
                schema: "".to_string(),
                json_uri: "".to_string(),
                files: Some(files),
                metadata: MetadataMap::new(),
                links: None,
            }),
            authorities: None,
            compression: None,
            grouping: None,
            royalty: None,
            creators: None,
            ownership: Ownership {
                frozen: false,
                delegated: false,
                delegate: None,
                ownership_model: OwnershipModel::Single,
                owner: "".to_string(),
            },
            uses: None,
            supply: None,
            mutable: false,
            burnt: false,
            lamports: None,
            executable: None,
            metadata_owner: None,
            rent_epoch: None,
            plugins: None,
            unknown_plugins: None,
            mpl_core_info: None,
            external_plugins: None,
            unknown_external_plugins: None,
            inscription: None,
            spl20: None,
            mint_extensions: None,
            token_info: None,
        }
    }

    // This test demostrates that elements in the result of batch_get call
    // preserves same order as keys in argument
    #[tokio::test(flavor = "multi_thread")]
    #[tracing_test::traced_test]
    async fn test_rocks_batch_get_order() {
        let rocks_env = RocksTestEnvironment::new(&[]);

        let cf = &rocks_env.storage.asset_previews;

        for i in vec![1, 3, 4, 6, 8] {
            cf.put(tst_url_hash(i), AssetPreviews::new(i as u32)).unwrap();
        }

        // Fetch in not sorted order
        {
            let res = cf
                .batch_get(vec![tst_url_hash(3), tst_url_hash(8), tst_url_hash(6)])
                .await
                .unwrap();

            assert_eq!(
                res,
                vec![
                    Some(AssetPreviews::new(3)),
                    Some(AssetPreviews::new(8)),
                    Some(AssetPreviews::new(6))
                ]
            );
        }

        // inserting more records to cause btree change
        for i in vec![7, 0, 5, 2, 10, 9] {
            cf.put(tst_url_hash(i), AssetPreviews::new(i as u32)).unwrap();
        }

        // Fetch again
        {
            let res = cf
                .batch_get(vec![tst_url_hash(3), tst_url_hash(8), tst_url_hash(6)])
                .await
                .unwrap();

            assert_eq!(
                res,
                vec![
                    Some(AssetPreviews::new(3)),
                    Some(AssetPreviews::new(8)),
                    Some(AssetPreviews::new(6))
                ]
            );
        }

        // Ensure order when requesting missed keys
        {
            let res = cf
                .batch_get(vec![
                    tst_url_hash(3),
                    tst_url_hash(2),
                    tst_url_hash(99),
                    tst_url_hash(1),
                ])
                .await
                .unwrap();

            assert_eq!(
                res,
                vec![
                    Some(AssetPreviews::new(3)),
                    Some(AssetPreviews::new(2)),
                    None,
                    Some(AssetPreviews::new(1))
                ]
            );
        }

        for i in vec![3, 4, 5, 6] {
            cf.delete(tst_url_hash(i)).unwrap();
        }

        // Ensure order after deleted part of keys
        {
            let res = cf
                .batch_get(vec![tst_url_hash(3), tst_url_hash(2), tst_url_hash(1)])
                .await
                .unwrap();

            assert_eq!(res, vec![None, Some(AssetPreviews::new(2)), Some(AssetPreviews::new(1))]);
        }
    }

    fn tst_url(i: u32) -> String {
        format!("http://host/url_{i}")
    }

    fn tst_url_hash(i: u32) -> [u8; HASH_BYTES] {
        keccak::hash(tst_url(i).as_bytes()).to_bytes()
    }
}
