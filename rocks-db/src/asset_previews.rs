use interface::{assert_urls::{UrlDownloadNotification, UrlsToDownloadStore}, asset_streaming_and_discovery::AsyncError};
use serde::{Deserialize, Serialize};
use solana_sdk::keccak;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{column::TypedColumn, key_encoders::decode_string, Storage};

const DL_RETRY_INTERVAL_SEC: u64 = 300; // 5 minutes
const DL_MAX_ATTEMPTS: u64 = 5;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetPreviews {
    pub size: u32,
}

impl AssetPreviews {
    pub fn new(size: u32) -> Self {
        AssetPreviews { size }
    }
}

impl TypedColumn for AssetPreviews {
    type KeyType = [u8; 32];

    type ValueType = Self;

    const NAME: &'static str = "ASSET_PREVIEWS";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        index.to_vec()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    }
}

/// Entity for column family that contains "URL to download" for media storage.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UrlToDownload {
    pub timestamp: u64, // unix time in seconds
    pub download_attempts: u64,
}

impl From<u64> for UrlToDownload {
    fn from(timestamp: u64) -> Self {
        UrlToDownload { timestamp, download_attempts: 0 }
    }
}

impl TypedColumn for UrlToDownload {
    type KeyType = String;

    type ValueType = Self;

    const NAME: &'static str = "URLS_TO_DOWNLOAD";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        index.into_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        String::from_utf8(bytes)
            .map_err(|e| crate::errors::StorageError::Common(e.to_string()))
    }
}


impl UrlsToDownloadStore for Storage {
    /// Retrieves given number of asset URL, that are waiting to be downloaded.
    /// While fetching, in RocksDB these URLs are marked with the timestamp
    /// that indicates moment when they has been retrieved.
    fn get_urls_to_download(&self, number_of_urls: u32) -> Result<Vec<String>, AsyncError>  {
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut urls = self.urls_to_download.iter_start()
            .filter_map(|row| {
                row.ok().and_then(|(key_bytes, value_bytes)| {
                    let key = decode_string(key_bytes.to_vec()).ok();
                    let val = bincode::deserialize::<UrlToDownload>(&value_bytes).ok();
                    key.and_then(|k| val.map(|v| (k,v)))
                })
            })
            .filter(|(_key_bytes, value)| {
                value.timestamp == 0 ||
                    value.timestamp + DL_RETRY_INTERVAL_SEC * value.download_attempts < now
            })
            .take(number_of_urls as usize)
            .collect::<Vec<_>>();

        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (url, info) in urls.iter_mut() {
            info.timestamp = now;
            info.download_attempts += 1;
            self.urls_to_download.put_with_batch(&mut batch, url.clone(), &info)?;
        }
        self.urls_to_download.backend.write(batch)?;

        Ok(urls.into_iter().map(|(url, _)| url).collect::<Vec<_>>())
    }

    fn submit_download_results(&self, results: Vec<UrlDownloadNotification>) -> Result<(), AsyncError>  {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for UrlDownloadNotification {url, outcome} in results {
            use interface::assert_urls::DownloadOutcome as E;
            match outcome {
                E::Success { mime, size } => {
                    // This is the main successful scenario: the asset is downloaded, and we can remove its
                    // URL from the UrlToDownload collection and add to downloaded assets.
                    let url_hash = keccak::hash(url.as_bytes());
                    let prev: AssetPreviews = AssetPreviews::new(size);
                    self.urls_to_download.delete_with_batch(&mut batch, url);
                    self.asset_previews.put_with_batch(&mut batch, url_hash.to_bytes(), &prev)?;
                },
                E::NotFound => { // TODO-XXX: any additional actions?
                    self.urls_to_download.delete_with_batch(&mut batch, url)
                },
                E::ServerError | E::TooManyRequests  => {
                    if let Ok(Some(prev)) = self.urls_to_download.get(url.clone()) {
                        if prev.download_attempts > DL_MAX_ATTEMPTS {
                            self.urls_to_download.delete_with_batch(&mut batch, url)
                        }
                    };
                },
                E::NotSupportedFormat | E::TooLarge | E::CorruptedAsset | E::Nothing => {
                    self.urls_to_download.delete_with_batch(&mut batch, url)
                },
            };
        };
        self.db.write(batch)?;
        Ok(())
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UrlInDownload;

impl TypedColumn for UrlInDownload {
    type KeyType = String;

    type ValueType = ();

    const NAME: &'static str = "URLS_IN_DOWNLOAD";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        index.into_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        String::from_utf8(bytes)
            .map_err(|e| crate::errors::StorageError::Common(e.to_string()))
    }
}


pub fn init_urls(store: &Storage) {
    // let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
    // store.urls_to_download.put_with_batch(
    //     &mut batch,
    //     "https://sun9-3.userapi.com/impf/c840623/v840623297/21028/ZLHPSops2Rk.jpg?size=1440x2160&quality=96&sign=5693c83276849eb25812b219477f08da&type=album".to_string(), 
    //     &UrlToDownload::from(0)
    // ).unwrap();
    // store.urls_to_download.put_with_batch(
    //     &mut batch, 
    //     "https://sun9-11.userapi.com/impf/mJ1zFrwCdAqKkxAJdsDUlnKV7h_Mvumm89NOug/v_CgKMdStmw.jpg?size=2560x1707&quality=96&sign=0b6d651a9f7438c96f292b27c78e73ee&type=album".to_string(), 
    //     &UrlToDownload::from(0)
    // ).unwrap();
    
    // store.db.write(batch).unwrap();
}

#[test]
fn test_make_hash() {
    println!("{}", keccak::hash("aaa".as_bytes()).to_string());
}