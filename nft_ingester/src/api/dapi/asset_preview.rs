use itertools::Itertools;
use rocks_db::Storage;
use solana_sdk::keccak::{self, Hash};
use url::Url;

use crate::api::{dapi::rpc_asset_models::Asset, error::DasApiError};

/// This is the adaptor for [`replace_file_urls_with_preview_urls_refs`]
pub async fn populate_previews_opt(
    base_url: &str,
    rocks_db: &Storage,
    assets: &mut [Option<Asset>],
) -> Result<(), DasApiError> {
    let mut references = assets.iter_mut().flatten().collect_vec();
    populate_previews_slice(base_url, rocks_db, &mut references).await?;
    Ok(())
}

/// This is the adaptor for [`replace_file_urls_with_preview_urls_refs`]
pub async fn populate_previews(
    base_url: &str,
    rocks_db: &Storage,
    assets: &mut [Asset],
) -> Result<(), DasApiError> {
    let mut references = assets.iter_mut().collect_vec();
    populate_previews_slice(base_url, rocks_db, &mut references).await?;
    Ok(())
}

/// Replaces original assets file URLs with URLs pointing to Storage service,
/// in case if the resource referenced by the URL previously had been donwloaded
/// and stored on Storage service.
///
/// ## Arguments:
/// * `rocks_db` - rocks db client, required since information about
///   assets on storage service is there.
/// * `assets` - collection of assets with file URLs to be repointed
pub async fn populate_previews_slice(
    base_url: &str,
    rocks_db: &Storage,
    assets: &mut [&mut Asset],
) -> Result<(), DasApiError> {
    let hashes = assets
        .iter()
        .filter_map(|a| a.content.as_ref())
        .filter_map(|c| c.files.as_ref())
        .flat_map(|v| v.iter())
        .filter_map(|f| f.uri.clone())
        .map(|url| keccak::hash(url.as_bytes()).to_bytes())
        .collect::<Vec<_>>();

    // order of elements in the result of `batch_get` is same as the order of input keys
    let previews = rocks_db.asset_previews.batch_get(hashes.clone()).await?;

    assets
        .iter_mut()
        .filter_map(|a| a.content.as_mut())
        .filter_map(|c| c.files.as_mut())
        .flat_map(|v| v.iter_mut())
        .filter(|f| f.uri.is_some())
        .enumerate()
        .for_each(|(i, file)| {
            let url = file.uri.as_ref().unwrap(); // always exists because of filter
            let has_preview = previews[i].as_ref().map(|p| p.failed.is_none()).unwrap_or(false);
            if has_preview {
                if let Ok(prevew) = preview_url(url, base_url, keccak::hash(url.as_bytes())) {
                    file.cdn_uri = Some(prevew);
                };
            }
        });
    Ok(())
}

/// Contruct URL that points to the asset preview stored on Storage service
///
/// ## Arguments:
/// * `original_url` - original asset URL to take query params from
/// * `storage_service_base_url` - base URL of Storage service, e.g. http://storage.com/
/// * `url_hash` - bs58 string representation of keccak256 hash of original asset URL
fn preview_url(
    original_url: &str,
    storage_service_base_url: &str,
    url_hash: Hash,
) -> Result<String, url::ParseError> {
    let orig_url = Url::parse(original_url)?;

    let mut result = Url::parse(storage_service_base_url)?
        .join("/preview/")?
        .join(url_hash.to_string().as_str())?;

    result.set_query(orig_url.query());

    Ok(result.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_construct_preview_url_without_query() {
        let original_url = "https://original/nft1";
        let url_hash = keccak::hash(original_url.as_bytes());
        let result = preview_url(original_url, "http://storage", url_hash).unwrap();
        assert_eq!(result, format!("http://storage/preview/{url_hash}"));
    }

    #[test]
    fn test_construct_preview_url_base_path_trailing_slash() {
        let original_url = "https://original/nft1";
        let url_hash = keccak::hash(original_url.as_bytes());
        let result = preview_url(original_url, "http://storage/", url_hash).unwrap();
        assert_eq!(result, format!("http://storage/preview/{url_hash}"));
    }

    #[test]
    fn test_construct_preview_url_with_query() {
        let original_url = "https://original/nft1?car=true";
        let url_hash = keccak::hash(original_url.as_bytes());
        let result = preview_url(original_url, "http://storage", url_hash).unwrap();
        assert_eq!(result, format!("http://storage/preview/{url_hash}?car=true"));
    }
}
