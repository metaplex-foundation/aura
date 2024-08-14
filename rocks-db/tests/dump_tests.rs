use std::fs::File;

use setup::rocks::*;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[tracing_test::traced_test]
async fn test_scv_export_from_rocks() {
    let env = RocksTestEnvironment::new(&[]);
    let number_of_assets = 1000;
    let _generated_assets = env.generate_assets(number_of_assets, 25);
    let storage = env.storage;
    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let temp_dir = TempDir::new().expect("Failed to create a temporary directory");
    let tasks_path = format!("{}/tasks.csv", temp_dir.path().to_str().unwrap());
    let assets_path = format!("{}/assets.csv", temp_dir.path().to_str().unwrap());
    let creators_path = format!("{}/creators.csv", temp_dir.path().to_str().unwrap());
    let authority_path = format!("{}/authority.csv", temp_dir.path().to_str().unwrap());
    let tasks_file = File::create(tasks_path.to_string()).unwrap();
    let assets_file: File = File::create(assets_path.to_string()).unwrap();
    let creators_file = File::create(creators_path.to_string()).unwrap();
    let authority_file = File::create(authority_path.to_string()).unwrap();

    storage
        .dump_csv(
            (tasks_file, tasks_path.clone()),
            (assets_file, assets_path.clone()),
            (creators_file, creators_path.clone()),
            (authority_file, authority_path.clone()),
            155,
            &rx,
        )
        .await
        .unwrap();

    let mut tasks_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(tasks_path)
        .unwrap();
    let mut assets_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(assets_path)
        .unwrap();
    let mut creators_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(creators_path)
        .unwrap();
    assert_eq!(tasks_reader.records().count(), 1);
    assert_eq!(assets_reader.records().count(), number_of_assets);
    assert_eq!(creators_reader.records().count(), number_of_assets);
}
