use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use super::cache::ChunkCache;

const MAX_FILE_PART_SIZE: u64 = 8 * 1024 * 1024; // 8MB

#[derive(Clone)]
pub struct ChunkCacheInMem {
    inner: Arc<Mutex<LruCache<String, Arc<Vec<u8>>>>>,
}

impl ChunkCacheInMem {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(max_entries).unwrap()))),
        }
    }
}

impl ChunkCache for ChunkCacheInMem {
    fn read(&self, buffer: &mut [u8], file_id: &str, offset: usize) -> std::io::Result<usize> {
        let mut cache = self.inner.lock().unwrap();
        if let Some(data) = cache.get(file_id) {
            let wanted = buffer.len().min(data.len().saturating_sub(offset));
            buffer[..wanted].copy_from_slice(&data[offset..offset + wanted]);
            Ok(wanted)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"))
        }
    }

    fn exists(&self, file_id: &str) -> bool {
        self.inner.lock().unwrap().contains(file_id)
    }

    fn max_file_part_size(&self) -> u64 {
        MAX_FILE_PART_SIZE
    }

    fn set(&self, file_id: &str, data: Arc<Vec<u8>>) {
        self.inner.lock().unwrap().put(file_id.to_owned(), data);
    }
}
