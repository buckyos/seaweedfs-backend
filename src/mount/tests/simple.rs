use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_simple_io() {
    env_logger::init();
    let tmp_dir = std::env::temp_dir();
    let tmp_dir = tmp_dir.join("weedfs_test");

    log::info!("create file: {}", tmp_dir.join("test.txt").to_string_lossy());
    let file_path = tmp_dir.join("test.txt");
    let mut file = File::create(&file_path).await.unwrap();

    log::info!("write file: {}", tmp_dir.join("test.txt").to_string_lossy());
    file.write_all(b"Hello, world!").await.unwrap();

    log::info!("flush file: {}", tmp_dir.join("test.txt").to_string_lossy());
    file.flush().await.unwrap();

    log::info!("open file: {}", tmp_dir.join("test.txt").to_string_lossy());
    let mut file = File::open(&file_path).await.unwrap();
    let mut buffer = [0; 13];

    log::info!("read file: {}", tmp_dir.join("test.txt").to_string_lossy());
    file.read(&mut buffer).await.unwrap();
    
    assert_eq!(&buffer, b"Hello, world!");
}
