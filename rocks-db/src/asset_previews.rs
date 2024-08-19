use interface::{
    assert_urls::{UrlDownloadNotification, UrlsToDownloadStore},
    asset_streaming_and_discovery::AsyncError,
};
use serde::{Deserialize, Serialize};
use solana_sdk::keccak;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{column::TypedColumn, errors::StorageError, key_encoders::decode_string, Storage};

const DL_RETRY_INTERVAL_SEC: u64 = 300; // 5 minutes
pub const DL_MAX_ATTEMPTS: u64 = 5;

/// Represents information about asset preview stored on Storage service.
///
/// Also serves a value type for Rocks columns famility that contains pairs:
/// Hash of asset URL -> AssetPreviews
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AssetPreviews {
    /// The size of asset preview stored in Storage service
    pub size: u32,
    pub failed: Option<DownloadFailInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DownloadFailInfo {
    timestamp_epoch_sec: u64,
}

impl AssetPreviews {
    pub fn new(size: u32) -> Self {
        AssetPreviews { size, failed: None }
    }
    pub fn new_failed(timestamp: u64) -> Self {
        AssetPreviews {
            size: 0,
            failed: Some(DownloadFailInfo {
                timestamp_epoch_sec: timestamp,
            }),
        }
    }
}

/// Rocks DB column family for information about asset previews
/// stored on Storage service.
///
/// E.g. given we have image asset, and the keccak hash of the asset URL
/// equals "XXXX".
/// We have sent this URL to Storage service, that has succesfully download the asset,
/// resized it to 400x400px bounding boxm and saved it. And sent a notification back
/// to DAS node about the asset has been downloaded and saved as 400px preview.
/// On receiving this notification, a following asset preview key pair should be persisted.
/// "XXXX" -> AssetPreviews { size = 400 }
impl TypedColumn for AssetPreviews {
    /// keccak256 hash of asset URL
    type KeyType = [u8; 32];

    type ValueType = Self;

    const NAME: &'static str = "ASSET_PREVIEWS";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        index.to_vec()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let mut arr = [0u8; 32];
        if bytes.len() != arr.len() {
            Err(StorageError::InvalidKeyLength)
        } else {
            arr.copy_from_slice(&bytes);
            Ok(arr)
        }
    }
}

/// Entity for column family that contains "URL to download" for storage service.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UrlToDownload {
    pub timestamp: u64, // unix time in seconds
    pub download_attempts: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for UrlToDownload {
    fn default() -> Self {
        Self {
            timestamp: 0,
            download_attempts: 0,
        }
    }
}

impl From<u64> for UrlToDownload {
    fn from(timestamp: u64) -> Self {
        UrlToDownload {
            timestamp,
            download_attempts: 0,
        }
    }
}

/// Rocks DB column family that is used as a queue for asset URLs,
/// to be sent to Storage service, where they are downloaded
/// and saved as previews.
impl TypedColumn for UrlToDownload {
    type KeyType = String;

    type ValueType = Self;

    const NAME: &'static str = "URLS_TO_DOWNLOAD";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        index.into_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        String::from_utf8(bytes).map_err(|e| crate::errors::StorageError::Common(e.to_string()))
    }
}

impl UrlsToDownloadStore for Storage {
    /// Retrieves given number of asset URL, that are waiting to be downloaded.
    /// While fetching, in RocksDB these URLs are marked with the timestamp
    /// that indicates moment when they has been retrieved.
    fn get_urls_to_download(&self, number_of_urls: u32) -> Result<Vec<String>, AsyncError> {
        let now: u64 = current_epoch_time();

        let mut urls = self
            .urls_to_download
            .iter_start()
            .filter_map(|row| {
                row.ok().and_then(|(key_bytes, value_bytes)| {
                    let key = decode_string(key_bytes.to_vec()).ok();
                    let val = bincode::deserialize::<UrlToDownload>(&value_bytes).ok();
                    key.and_then(|k| val.map(|v| (k, v)))
                })
            })
            .filter(|(_key_bytes, value)| {
                value.timestamp == 0
                    || value.timestamp + DL_RETRY_INTERVAL_SEC * value.download_attempts < now
            })
            .take(number_of_urls as usize)
            .collect::<Vec<_>>();

        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (url, info) in urls.iter_mut() {
            info.timestamp = now;
            info.download_attempts += 1;
            self.urls_to_download
                .put_with_batch(&mut batch, url.clone(), info)?;
        }
        self.urls_to_download.backend.write(batch)?;

        Ok(urls.into_iter().map(|(url, _)| url).collect::<Vec<_>>())
    }

    fn submit_download_results(
        &self,
        results: Vec<UrlDownloadNotification>,
    ) -> Result<(), AsyncError> {
        let now = current_epoch_time();
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for UrlDownloadNotification { url, outcome } in results {
            use interface::assert_urls::DownloadOutcome as E;
            match outcome {
                E::Success { mime: _, size } => {
                    // This is the main successful scenario: the asset is downloaded, and we can remove its
                    // URL from the UrlToDownload collection and add to downloaded assets.
                    let url_hash = keccak::hash(url.as_bytes());
                    let prev: AssetPreviews = AssetPreviews::new(size);
                    self.urls_to_download.delete_with_batch(&mut batch, url);
                    self.asset_previews
                        .put_with_batch(&mut batch, url_hash.to_bytes(), &prev)?;
                }
                E::NotFound => {
                    let url_hash = keccak::hash(url.as_bytes());
                    self.urls_to_download.delete_with_batch(&mut batch, url);
                    self.asset_previews.put_with_batch(
                        &mut batch,
                        url_hash.to_bytes(),
                        &AssetPreviews::new_failed(now),
                    )?;
                }
                E::ServerError | E::TooManyRequests => {
                    if let Ok(Some(prev)) = self.urls_to_download.get(url.clone()) {
                        if prev.download_attempts > DL_MAX_ATTEMPTS {
                            let url_hash = keccak::hash(url.as_bytes());
                            self.urls_to_download.delete_with_batch(&mut batch, url);
                            self.asset_previews.put_with_batch(
                                &mut batch,
                                url_hash.to_bytes(),
                                &AssetPreviews::new_failed(now),
                            )?;
                        }
                    };
                }
                E::NotSupportedFormat | E::TooLarge | E::CorruptedAsset | E::Nothing => {
                    // Means in get-asset request, we'll be providing original asset URL
                    self.urls_to_download.delete_with_batch(&mut batch, url)
                }
            };
        }
        self.db.write(batch)?;
        Ok(())
    }
}

fn current_epoch_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
