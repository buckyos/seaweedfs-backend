use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_simple_io() {
    let tmp_dir = std::env::temp_dir();
    let tmp_dir = tmp_dir.join("weedfs_test");

    let file_path = tmp_dir.join("test.txt");
    let mut file = File::create(&file_path).await.unwrap();
    file.write_all(b"Hello, world!").await.unwrap();
    file.flush().await.unwrap();

    let mut file = File::open(&file_path).await.unwrap();
    let mut buffer = [0; 13];
    file.read(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"Hello, world!");
}
