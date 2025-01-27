use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use dfs_common::ChunkCache;
use dfs_common::chunks::LookupFileId;
use crate::file_handle::{FileHandleId, FileHandle};
struct FileHandleInodeMap<T: ChunkCache> {
    inode_to_fh: BTreeMap<u64, FileHandle<T>>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}

struct WfsInner<T: ChunkCache> {
    fh_map: RwLock<FileHandleInodeMap<T>>, 
    chunk_cache: T
}

pub struct Wfs<T: ChunkCache> {
    inner: Arc<WfsInner<T>>, 
}


impl<T: ChunkCache> Clone for Wfs<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}


impl<T: ChunkCache + Clone> Wfs<T> {
    pub fn new(chunk_cache: T) -> Self {
        Self {
            inner: Arc::new(WfsInner {
                fh_map: RwLock::new(FileHandleInodeMap {
                    inode_to_fh: BTreeMap::new(),
                    fh_to_inode: BTreeMap::new(),
                }),
                chunk_cache,
            }),
        }
    }

    pub(crate) fn chunk_cache(&self) -> &T {
        &self.inner.chunk_cache
    }

    fn get_handle_by_id(&self, id: FileHandleId) -> Option<FileHandle<T>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.fh_to_inode.get(&id).and_then(|inode| fh_map.inode_to_fh.get(&inode)).cloned()
    }

    fn get_handle_by_inode(&self, inode: u64) -> Option<FileHandle<T>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.inode_to_fh.get(&inode).cloned()
    }
}

impl<T: ChunkCache + Clone> LookupFileId for Wfs<T> {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>> {
        unimplemented!()
    }
}


impl<T: ChunkCache + Clone> Filesystem for Wfs<T> {
    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let fh = self.get_handle_by_id(fh);
        if fh.is_none() {
            reply.error(ENOENT);
            return;
        }

        // TODO: optimize buffer allocation
        let mut buffer = vec![0u8; size as usize];
        
        let file_handle = fh.unwrap();
        match file_handle.read_from_chunks(self,&mut buffer, offset) {
            Ok((n, _ts_ns)) => {
                // 只返回实际读取的数据
                reply.data(&buffer[..n]);
            },
            Err(e) => {
                reply.error(ENOENT);  // 或其他适当的错误码
            }
        }
    }
}
