use std::sync::Arc;
pub trait ChunkCache {
    fn read(&self, data: &mut [u8], file_id: &str, offset: usize) -> std::io::Result<usize>;
    fn set(&self, file_id: &str, data: Arc<Vec<u8>>);
    fn exists(&self, file_id: &str) -> bool;
}
