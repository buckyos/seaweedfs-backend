use std::cmp::min;
use std::num::NonZero;
use std::ops::Range;
use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use lru::LruCache;
use crate::chunk_cache::ChunkCache;
use crate::chunks::{self, LookupFileId};
use crate::interval_list::IntervalValue;

#[derive(Clone, Debug)]
pub struct ChunkView { 
    pub file_id: String,
    // view's start offset in the file
    pub offset_in_chunk: i64,
    // chunk's total size
    pub chunk_size: u64,
    // view's size
    pub view_size: u64,
    // view's offset in the file
    pub view_offset: i64,
}

impl IntervalValue for ChunkView {
    fn set_range(&mut self, old_range: Range<i64>, new_range: Range<i64>) {
        self.offset_in_chunk += new_range.start - old_range.start;
        self.view_offset = new_range.start;
        self.view_size = (new_range.end - new_range.start) as u64;
    }
}

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

    pub fn outer_cache(&self) -> &T {
        &self.inner.outer_cache
    }

    pub fn uncache(&self, file_id: &str) {
        let mut inner_cache = self.inner.inner_cache.lock().unwrap();
        inner_cache.downloading.remove(file_id);
        inner_cache.downloaded.pop(file_id);
    }

    pub fn maybe_cache<'a>(
        &self, 
        _lookup: &impl LookupFileId, 
        _chunk_views: impl Iterator<Item = &'a ChunkView>
    ) {
        // TODO: implement
        // if rc.lookupFileIdFn == nil {
        //     return
        // }
    
        // rc.Lock()
        // defer rc.Unlock()
    
        // if len(rc.downloaders) >= rc.limit {
        //     return
        // }
    
        // for x := chunkViews; x != nil; x = x.Next {
        //     chunkView := x.Value
        //     if _, found := rc.downloaders[chunkView.FileId]; found {
        //         continue
        //     }
        //     if rc.chunkCache.IsInCache(chunkView.FileId, true) {
        //         glog.V(4).Infof("%s is in cache", chunkView.FileId)
        //         continue
        //     }
    
        //     if len(rc.downloaders) >= rc.limit {
        //         // abort when slots are filled
        //         return
        //     }
    
        //     // glog.V(4).Infof("prefetch %s offset %d", chunkView.FileId, chunkView.ViewOffset)
        //     // cache this chunk if not yet
        //     shouldCache := (uint64(chunkView.ViewOffset) + chunkView.ChunkSize) <= rc.chunkCache.GetMaxFilePartSizeInCache()
        //     cacher := newSingleChunkCacher(rc, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int(chunkView.ChunkSize), shouldCache)
        //     go cacher.startCaching()
        //     <-cacher.cacheStartedCh
        //     rc.downloaders[chunkView.FileId] = cacher
    
        // }
    
        // return
    }

    pub fn read_at(
        &self, 
        lookup: &impl LookupFileId, 
        buf: &mut [u8], 
        file_id: &str, 
        offset: usize, 
        chunk_size: usize, 
        should_cache: bool
    ) -> std::io::Result<usize> {
        log::trace!("read_at: file_id: {}, offset: {}, chunk_size: {}", file_id, offset, chunk_size);
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
            log::trace!("read_at: downloaded: file_id: {}, offset: {}, chunk_size: {}", file_id, offset, chunk_size);
            buf[..read].copy_from_slice(&data.as_slice()[offset..offset + read]);
            return Ok(read);
        }
        
        if should_cache {
            if let Ok(read) = self.inner.outer_cache.read(buf, file_id, offset) {
                log::trace!("read_at: outer_cache: file_id: {}, offset: {}, chunk_size: {}", file_id, offset, chunk_size);
                return Ok(read);
            }
        }
        let data = {
            let new_downloading = Arc::new(RwLock::new(None));
            let mut downloading = new_downloading.write().unwrap();
            self.inner.inner_cache.lock().unwrap().downloading.insert(file_id.to_owned(), new_downloading.clone());
           
            // TODO: use a buffer pool
            let mut data = vec![0u8; chunk_size];
            log::trace!("read_at: fetch_whole_chunk: file_id: {}, offset: {}, chunk_size: {}", file_id, offset, chunk_size);    
            chunks::fetch_whole_chunk(lookup, &mut data[..], file_id)
                .map_err(|e| {
                    log::trace!("read_at: fetch_whole_chunk: file_id: {}, offset: {}, chunk_size: {}, error: {}", file_id, offset, chunk_size, e);
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                })?;
            let data = Arc::new(data);
            if should_cache {
                self.inner.outer_cache.set(file_id, data.clone());
            }
            *downloading = Some(data.clone());
            data
        };
        buf[..read].copy_from_slice(&data.as_slice()[offset..offset + read]);
        Ok(read)
    }
}
