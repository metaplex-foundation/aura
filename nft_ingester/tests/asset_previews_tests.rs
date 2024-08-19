#[cfg(test)]
mod tests {
    use entities::enums::OwnershipModel;
    use itertools::Itertools;
    use nft_ingester::api::dapi::asset_preview::populate_previews_opt;
    use nft_ingester::api::dapi::rpc_asset_models::{Asset, Content, File, MetadataMap, Ownership};
    use rocks_db::asset_previews::AssetPreviews;
    use setup::rocks::RocksTestEnvironment;
    use solana_sdk::keccak;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_replace_file_urls_with_preview_urls() {
        let rocks_env = RocksTestEnvironment::new(&[]);

        // given
        let files = vec![
            vec![
                "http://host/url1".to_string(),
                "http://host/url2".to_string(),
            ],
            vec!["http://host/url3".to_string()],
        ];

        rocks_env
            .storage
            .asset_previews
            .put(
                keccak::hash("http://host/url1".as_bytes()).to_bytes(),
                AssetPreviews::new(400),
            )
            .unwrap();
        rocks_env
            .storage
            .asset_previews
            .put(
                keccak::hash("http://host/url3".as_bytes()).to_bytes(),
                AssetPreviews::new(400),
            )
            .unwrap();

        let mut assets = files
            .into_iter()
            .map(|v| Some(test_asset_for_content(v)))
            .collect_vec();

        // when
        populate_previews_opt("http://storage/", &rocks_env.storage, &mut assets)
            .await
            .unwrap();

        // expected
        assert_eq!(
            assets[0]
                .as_ref()
                .unwrap()
                .content
                .as_ref()
                .unwrap()
                .files
                .as_ref()
                .unwrap()[0]
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
            assets[0]
                .as_ref()
                .unwrap()
                .content
                .as_ref()
                .unwrap()
                .files
                .as_ref()
                .unwrap()[1]
                .cdn_uri,
            None,
        );

        assert_eq!(
            assets[1]
                .as_ref()
                .unwrap()
                .content
                .as_ref()
                .unwrap()
                .files
                .as_ref()
                .unwrap()[0]
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
        }
    }
}
