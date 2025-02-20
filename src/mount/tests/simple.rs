use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use rand::{rng, Rng};
use std::sync::atomic::{AtomicU32, Ordering};

static FILE_COUNTER: AtomicU32 = AtomicU32::new(1);

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

async fn create_random_content() -> Vec<u8> {
    let len = rng().random_range(10..1000);
    let mut content = vec![0u8; len];
    rng().fill(&mut content[..]);
    content
}

async fn write_file(path: &std::path::Path, content: &[u8]) -> anyhow::Result<()> {
    let mut file = File::create(path).await?;
    file.write_all(content).await?;
    file.flush().await?;
    Ok(())
}

async fn read_file(path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let mut file = File::open(path).await?;
    let mut content = Vec::new();
    file.read_to_end(&mut content).await?;
    Ok(content)
}

async fn create_sequential_content() -> Vec<u8> {
    let seq = FILE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let seq_str = seq.to_string();
    let mut content = String::with_capacity(seq as usize * 100);
    for _ in 0..(seq * 100) {
        content.push_str(&seq_str);
    }
    content.into_bytes()
}

#[tokio::test]
async fn test_simple_dir_tree() -> anyhow::Result<()> {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
    let tmp_dir = std::env::temp_dir().join("weedfs_test");
    let test_dir = tmp_dir.join("testtree");
    if test_dir.exists() {
        tokio::fs::remove_dir_all(&test_dir).await?;
    }
    tokio::fs::create_dir_all(&test_dir).await?;

    // 用于存储所有创建的文件路径及其内容
    let mut file_contents = HashMap::new();

    // 创建三级目录结构和文件
    let dir_structure = vec![
        "dir1",
        "dir1/subdir1",
        "dir1/subdir2",
        "dir2",
        "dir2/subdir1/subsubdir1",
    ];

    // 创建目录结构
    for dir in &dir_structure {
        let dir_path = test_dir.join(dir);
        log::info!("Creating directory: {}", dir_path.display());
        tokio::fs::create_dir_all(&dir_path).await?;

        // 在每个目录中创建1-3个文件
        let file_count = rng().random_range(1..=3);
        for _ in 0..file_count {
            let seq = FILE_COUNTER.load(Ordering::SeqCst);
            let file_name = format!("file_{:03}.dat", seq);
            let file_path = dir_path.join(&file_name);
            let content = create_sequential_content().await;
            
            log::info!("Creating file: {} (seq={}, size={})", file_path.display(), seq, content.len());
            write_file(&file_path, &content).await?;
            file_contents.insert(file_path, content);
        }
    }

    // 验证目录结构和文件内容
    #[async_recursion::async_recursion]
    async fn verify_directory(path: &std::path::Path, contents: &HashMap<std::path::PathBuf, Vec<u8>>) -> anyhow::Result<()> {
        let mut entries = tokio::fs::read_dir(path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;

            if file_type.is_dir() {
                log::info!("Verifying directory: {}", path.display());
                verify_directory(&path, contents).await?;
            } else {
                log::info!("Verifying file: {}", path.display());
                if let Some(expected_content) = contents.get(&path) {
                    let actual_content = read_file(&path).await?;
                    assert_eq!(
                        &actual_content, expected_content,
                        "File content mismatch for {}",
                        path.display()
                    );
                    // 验证文件内容是否正确的序列
                    let seq = (expected_content.len() / 100) as u32;
                    let seq_str = seq.to_string().into_bytes();
                    for chunk in actual_content.chunks(seq_str.len()) {
                        assert_eq!(chunk, &seq_str, "Wrong sequence number in file {}", path.display());
                    }
                    log::info!("File content verified: {} (seq={}, size={})", path.display(), seq, actual_content.len());
                }
            }
        }
        Ok(())
    }

    // 执行验证
    verify_directory(&test_dir, &file_contents).await?;
    
    // 清理测试目录
    tokio::fs::remove_dir_all(&test_dir).await?;
    
    Ok(())
}
