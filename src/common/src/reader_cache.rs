use anyhow::Result;
use std::cmp::min;
use std::num::NonZero;
use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use lru::LruCache;
use crate::chunk_cache::ChunkCache;
use crate::chunks;

type Downloading = Arc<RwLock<Option<Arc<Vec<u8>>>>>;

struct InnerCache {
    downloading: HashMap<String, Downloading>,
    downloaded: LruCache<String, Arc<Vec<u8>>>,
}


struct CacheInner<T: ChunkCache> {
    outer_cache: T, 
    inner_cache: Mutex<InnerCache>,
}


pub struct ReaderCache<T: ChunkCache> {
    inner: Arc<CacheInner<T>>,
}

impl<T: ChunkCache> Clone for ReaderCache<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ChunkCache> ReaderCache<T> {
    pub fn new(outer_cache: T) -> Self {
        Self {
            inner: Arc::new(CacheInner { 
                outer_cache, 
                inner_cache: Mutex::new(InnerCache { 
                    downloading: HashMap::new(), 
                    downloaded: LruCache::new(NonZero::new(100).unwrap()) 
                }) }),
        }
    }

    pub fn read_at(
        &self, 
        lookup: impl Fn(&str) -> Result<Vec<String>>, 
        buf: &mut [u8], 
        file_id: &str, 
        offset: usize, 
        chunk_size: usize, 
        should_cache: bool
    ) -> std::io::Result<usize> {
        let downloded = {
            let mut inner_cache = self.inner.inner_cache.lock().unwrap();
            if let Some(data) = inner_cache.downloaded.get(file_id) {
                Some(data.clone())
            } else {
                if let Some(downloading) = inner_cache.downloading.get(file_id).cloned() {
                    Some(downloading.read().unwrap().clone().ok_or(std::io::Error::new(std::io::ErrorKind::NotFound, "download failed"))?)
                } else {
                    None
                }  
            } 
        };

        let read = min(chunk_size - offset, buf.len());
        if let Some(data) = downloded {
            buf[..read].copy_from_slice(&data.as_slice()[offset..read]);
            return Ok(read);
        }
        
        if should_cache {
            if let Ok(read) = self.inner.outer_cache.read(buf, file_id, offset) {
                return Ok(read);
            }
        }
        let data = {
            let new_downloading = Arc::new(RwLock::new(None));
            let mut downloading = new_downloading.write().unwrap();
            self.inner.inner_cache.lock().unwrap().downloading.insert(file_id.to_owned(), new_downloading.clone());
            let url_strings = lookup(file_id).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            let mut data = vec![0u8; chunk_size];
            chunks::retried_fetch_chunk_data(&mut data[..], url_strings, true, 0).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            let data = Arc::new(data);
            if should_cache {
                self.inner.outer_cache.set(file_id, data.clone());
            }
            *downloading = Some(data.clone());
            data
        };
        buf[..read].copy_from_slice(&data.as_slice()[offset..read]);
        Ok(read)
    }
}
