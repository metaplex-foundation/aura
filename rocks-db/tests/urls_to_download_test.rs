#[cfg(test)]
mod tests {
    use interface::assert_urls::{DownloadOutcome, UrlDownloadNotification, UrlsToDownloadStore};
    use rocks_db::columns::asset_previews::{AssetPreviews, UrlToDownload, DL_MAX_ATTEMPTS};
    use setup::{await_async_for, rocks::*};
    use solana_sdk::keccak;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_get_urls_to_download() {
        // prepare
        let storage = RocksTestEnvironment::new(&[]).storage;

        // given
        storage
            .urls_to_download
            .put("http://good-url.xyz/".to_string(), UrlToDownload::default())
            .unwrap();

        // when
        let urls = storage.get_urls_to_download(1).unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "http://good-url.xyz/".to_string());

        // then
        await_async_for!(
            {
                let UrlToDownload { timestamp, download_attempts } = storage
                    .urls_to_download
                    .get("http://good-url.xyz/".to_string())
                    .unwrap()
                    .unwrap();
                timestamp > 0 && download_attempts == 1
            },
            10,
            std::time::Duration::from_millis(100)
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_successful_download() {
        // prepare
        let storage = RocksTestEnvironment::new(&[]).storage;

        // given
        storage
            .urls_to_download
            .put("http://good-url.xyz/".to_string(), UrlToDownload::default())
            .unwrap();

        // when
        let download_result = UrlDownloadNotification {
            url: "http://good-url.xyz/".to_string(),
            outcome: DownloadOutcome::Success { mime: "image/webp".to_string(), size: 400 },
        };
        storage.submit_download_results(vec![download_result]).unwrap();

        // then
        assert_eq!(None, storage.urls_to_download.get("http://good-url.xyz/".to_string()).unwrap());

        let url_hash = keccak::hash("http://good-url.xyz/".as_bytes());
        let preview_rul = storage.asset_previews.get(url_hash.to_bytes()).unwrap();
        assert_eq!(preview_rul, Some(AssetPreviews { size: 400, failed: None }))
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_submit_absent_url() {
        // prepare
        let storage = RocksTestEnvironment::new(&[]).storage;

        // given: no URLs to download "in the queue"

        // when submitting unknows URL nothing should happen
        let download_result = UrlDownloadNotification {
            url: "http://good-url.xyz/".to_string(),
            outcome: DownloadOutcome::Success { mime: "image/webp".to_string(), size: 400 },
        };
        storage.submit_download_results(vec![download_result]).unwrap();
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_remove_url_after_max_failed_attempts_to_dwonload() {
        // prepare
        let storage = RocksTestEnvironment::new(&[]).storage;

        // given
        storage
            .urls_to_download
            .put(
                "http://good-url.xyz/".to_string(),
                UrlToDownload { timestamp: 1, download_attempts: DL_MAX_ATTEMPTS + 1 },
            )
            .unwrap();

        // when
        let download_result = UrlDownloadNotification {
            url: "http://good-url.xyz/".to_string(),
            outcome: DownloadOutcome::ServerError,
        };
        storage.submit_download_results(vec![download_result.clone()]).unwrap();

        // then
        let url_to_download =
            storage.urls_to_download.get("http://good-url.xyz/".to_string()).unwrap();
        assert_eq!(None, url_to_download);

        let url_hash = keccak::hash("http://good-url.xyz/".as_bytes());
        let preview_rul = storage.asset_previews.get(url_hash.to_bytes()).unwrap();
        assert!(preview_rul.unwrap().failed.is_some());
    }
}
