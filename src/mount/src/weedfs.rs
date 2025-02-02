use anyhow::Result;
use dfs_common::pb::filer_pb::FileChunk;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite, Request
};
use libc::ENOENT;
use dfs_common::ChunkCache;
use dfs_common::chunks::{LookupFileId, UploadChunk};
use dfs_common::ChunkCacheInMem;
use crate::file_handle::{FileHandle, FileHandleId, FileHandleOwner};
use crate::path::InodeToPath;

pub struct WfsOption {
    pub chunk_size_limit: usize,
    pub concurrent_writers: usize,
    pub filer_mount_root_path: PathBuf,
}


struct FileHandleInodeMap {
    inode_to_fh: BTreeMap<u64, FileHandle<Wfs>>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}

struct WfsInner {
    option: WfsOption,
    inode_to_path: RwLock<InodeToPath>,
    fh_map: RwLock<FileHandleInodeMap>, 
    chunk_cache: ChunkCacheInMem
}

pub struct Wfs {
    inner: Arc<WfsInner>, 
}


impl Clone for Wfs {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}


impl Wfs {
    pub fn new(option: WfsOption) -> Self {
        Self {
            inner: Arc::new(WfsInner {
                inode_to_path: RwLock::new(InodeToPath::new(option.filer_mount_root_path.clone())),
                fh_map: RwLock::new(FileHandleInodeMap {
                    inode_to_fh: BTreeMap::new(),
                    fh_to_inode: BTreeMap::new(),
                }),
                chunk_cache: ChunkCacheInMem::new(1024),
                option,
            }),
        }
    }

    fn option(&self) -> &WfsOption {
        &self.inner.option
    }

    fn chunk_cache(&self) -> &ChunkCacheInMem {
        &self.inner.chunk_cache
    }

    fn get_handle_by_id(&self, id: FileHandleId) -> Option<FileHandle<Self>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.fh_to_inode.get(&id).and_then(|inode| fh_map.inode_to_fh.get(&inode)).cloned()
    }

    fn get_handle_by_inode(&self, inode: u64) -> Option<FileHandle<Self>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.inode_to_fh.get(&inode).cloned()
    }
}

impl LookupFileId for Wfs {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>> {
        unimplemented!()
    }
}

impl UploadChunk for Wfs {
    fn upload(&self, content: impl Read, file_id: &str, offset: i64, ts_ns: u64) -> Result<FileChunk> {
        unimplemented!()
    }
}

impl ChunkCache for Wfs {
    fn read(&self, data: &mut [u8], file_id: &str, offset: usize) -> std::io::Result<usize> {
        self.chunk_cache().read(data, file_id, offset)
    }
    fn set(&self, file_id: &str, data: Arc<Vec<u8>>) {
        self.chunk_cache().set(file_id, data);
    }
    fn exists(&self, file_id: &str) -> bool {
        self.chunk_cache().exists(file_id)
    }
    fn max_file_part_size(&self) -> u64 {
        self.chunk_cache().max_file_part_size()
    }
}

impl FileHandleOwner for Wfs {
    fn chunk_size_limit(&self) -> usize {
        self.option().chunk_size_limit
    }

    fn concurrent_writers(&self) -> usize {
        self.option().concurrent_writers
    }

    fn get_path(&self, inode: u64) -> Option<PathBuf> {
        self.inner.inode_to_path.read().unwrap().get_path(inode).map(|p| p.to_path_buf())
    }
}

impl Filesystem for Wfs {
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
        match file_handle.read(&mut buffer, offset) {
            Ok((n, _ts_ns)) => {
                // 只返回实际读取的数据
                reply.data(&buffer[..n]);
            },
            Err(e) => {
                reply.error(ENOENT);  // 或其他适当的错误码
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyWrite,
    ) {
        let fh = self.get_handle_by_id(fh);
        if fh.is_none() {
            reply.error(ENOENT);
            return;
        }

        let file_handle = fh.unwrap();
        match file_handle.write(data, offset) {
            Ok(n) => {
                reply.written(n as u32);
            }
            Err(_) => {
                reply.error(ENOENT);
            }
        }
    }
}
