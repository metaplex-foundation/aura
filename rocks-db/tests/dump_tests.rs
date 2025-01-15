use std::{fs::File, sync::Arc};

use metrics_utils::SynchronizerMetricsConfig;
use rocks_db::storage_traits::Dumper;
use setup::rocks::*;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[tracing_test::traced_test]
async fn test_scv_export_from_rocks() {
    let env = RocksTestEnvironment::new(&[]);
    let number_of_assets = 1000;
    let _generated_assets = env.generate_assets(number_of_assets, 25).await;
    let storage = env.storage;
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let assets_path = format!("{}/assets.csv", temp_dir.path().to_str().unwrap());
    let creators_path = format!("{}/creators.csv", temp_dir.path().to_str().unwrap());
    let authority_path = format!("{}/authority.csv", temp_dir.path().to_str().unwrap());
    let fungible_tokens_path = format!("{}/fungible_tokens.csv", temp_dir.path().to_str().unwrap());
    let metadata_path = format!("{}/metadata.csv", temp_dir.path().to_str().unwrap());
    let assets_file: File = File::create(assets_path.to_string()).unwrap();
    let creators_file = File::create(creators_path.to_string()).unwrap();
    let authority_file = File::create(authority_path.to_string()).unwrap();
    let fungible_tokens_file = File::create(fungible_tokens_path.to_string()).unwrap();
    let metadata_file = File::create(metadata_path.to_string()).unwrap();

    storage
        .dump_nft_csv(
            assets_file,
            creators_file,
            authority_file,
            metadata_file,
            155,
            Some(number_of_assets),
            None,
            None,
            &rx,
            Arc::new(SynchronizerMetricsConfig::new()),
        )
        .unwrap();

    storage
        .dump_fungible_csv(
            (fungible_tokens_file, fungible_tokens_path.clone()),
            155,
            None,
            None,
            &rx,
            Arc::new(SynchronizerMetricsConfig::new()),
        )
        .unwrap();
    let mut metadata_reader =
        csv::ReaderBuilder::new().has_headers(false).from_path(metadata_path).unwrap();
    let mut assets_reader =
        csv::ReaderBuilder::new().has_headers(false).from_path(assets_path).unwrap();
    let mut creators_reader =
        csv::ReaderBuilder::new().has_headers(false).from_path(creators_path).unwrap();
    assert_eq!(metadata_reader.records().count(), 1);
    assert_eq!(assets_reader.records().count(), number_of_assets);
    assert_eq!(creators_reader.records().count(), number_of_assets);
}
