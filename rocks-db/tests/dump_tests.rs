use std::fs::File;

use csv::WriterBuilder;
use setup::rocks::*;
use tempfile::TempDir;

#[tokio::test]
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
    let tasks_file = File::create(tasks_path.to_string()).unwrap();
    let assets_file: File = File::create(assets_path.to_string()).unwrap();
    let creators_file = File::create(creators_path.to_string()).unwrap();
    let mut tasks_writer = WriterBuilder::new()
        .has_headers(false)
        .from_writer(tasks_file);
    let mut assets_writer = WriterBuilder::new()
        .has_headers(false)
        .from_writer(assets_file);
    let mut creators_writer = WriterBuilder::new()
        .has_headers(false)
        .from_writer(creators_file);

    storage
        .dump_csv(
            &mut tasks_writer,
            &mut creators_writer,
            &mut assets_writer,
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
