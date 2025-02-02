use anyhow::{Result, Error};
use dfs_common::pb::{*, filer_pb::{Entry, FileChunk, FuseAttributes}};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow
};
use libc::ENOENT;
use dfs_common::chunks::{self, LookupFileId, UploadChunk};
use dfs_common::{ChunkCache, ChunkCacheInMem};
use crate::file_handle::{FileHandle, FileHandleId, FileHandleOwner};
use crate::path::{InodeToPath, ROOT_INODE};

pub struct WfsOption {
    pub filer_addresses: Vec<String>,
    pub chunk_size_limit: usize,
    pub concurrent_writers: usize,
    pub filer_mount_root_path: PathBuf,

    pub mount_mtime: SystemTime,
    pub mount_ctime: SystemTime,
    pub mount_mode: u32,
    pub mount_uid: u32,
    pub mount_gid: u32,
}

struct FileHandleInodeMap {
    ref_count: BTreeMap<FileHandleId, usize>,
    inode_to_fh: BTreeMap<u64, FileHandle<Wfs>>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}

struct WfsInner {
    option: WfsOption,
    filer_client: FilerClient,
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

const BLOCK_SIZE: u32 = 512;


impl Wfs {
    pub fn new(option: WfsOption) -> Result<Self> {
        let filer_client = FilerClient::connect(option.filer_addresses.clone())?;
        Ok(Self {
            inner: Arc::new(WfsInner {
                filer_client,
                inode_to_path: RwLock::new(InodeToPath::new(option.filer_mount_root_path.clone())),
                fh_map: RwLock::new(FileHandleInodeMap {
                    ref_count: BTreeMap::new(),
                    inode_to_fh: BTreeMap::new(),
                    fh_to_inode: BTreeMap::new(),
                }),
                chunk_cache: ChunkCacheInMem::new(1024),
                option,
            }),
        })
    }

    fn option(&self) -> &WfsOption {
        &self.inner.option
    }

    fn chunk_cache(&self) -> &ChunkCacheInMem {
        &self.inner.chunk_cache
    }

    fn filer_client(&self) -> &FilerClient {
        &self.inner.filer_client
    }

    fn get_handle_by_id(&self, id: FileHandleId) -> Option<FileHandle<Self>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.fh_to_inode.get(&id).and_then(|inode| fh_map.inode_to_fh.get(&inode)).cloned()
    }

    fn get_handle_by_inode(&self, inode: u64) -> Option<FileHandle<Self>> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.inode_to_fh.get(&inode).cloned()
    }

    fn get_entry_by_inode(&self, inode: u64) -> Option<(Entry, Option<FileHandle<Self>>)> {
        if inode == ROOT_INODE {
            return Some((Entry {
                name: "".to_string(),
                is_directory: true,
                attributes: Some(FuseAttributes {
                    mtime: self.option().mount_mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    file_mode: self.option().mount_mode,
                    uid: self.option().mount_uid,
                    gid: self.option().mount_gid,
                    crtime: self.option().mount_ctime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    ..Default::default()
                }),
                ..Default::default()
            }, None));
        }
        if let Some(fh) = self.get_handle_by_inode(inode) {
            Some((fh.entry(), Some(fh)))
        } else {
            if let Some(path) = self.get_path(inode) {
                self.get_entry_by_path(path.as_path()).map(|entry| (entry, None))
            } else {
                None
            }
        }
    }

    fn get_entry_by_path(&self, path: &Path) -> Option<Entry> {
        if path == self.option().filer_mount_root_path {
            return Some(Entry {
               name: "".to_string(),
               is_directory: true,
               attributes: Some(FuseAttributes {
                    mtime: self.option().mount_mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    file_mode: self.option().mount_mode,
                    uid: self.option().mount_uid,
                    gid: self.option().mount_gid,
                    crtime: self.option().mount_ctime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    ..Default::default()
               }),
               ..Default::default()
            });
        } 
        self.filer_client().lookup_directory_entry(path).ok().and_then(|entry| entry)
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

struct EntryAttr(Entry, u64, bool);

impl From<(Entry, u64, bool)> for EntryAttr {
    fn from((entry, inode, calculate_size): (Entry, u64, bool)) -> Self {
        Self(entry, inode, calculate_size)
    }
}

impl Into<FileAttr> for EntryAttr {
    fn into(self) -> FileAttr {
        unimplemented!()
    }
}

fn chmod(mode: &mut u32, new_mode: u32) {
    *mode = *mode & 0o777 | new_mode & 0o777
}

fn to_unix_time(time: TimeOrNow) -> i64 {
    match time {
        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    }
}

impl Filesystem for Wfs {
    // fn mknod(
    //     &mut self,
    //     _req: &Request,
    //     _ino: u64,
    //     _mode: u32,
    //     _uid: u32,
    //     _gid: u32,
    //     reply: ReplyEntry,
    // ) {
        
    // }

    fn open(
        &mut self,
        _req: &Request,
        ino: u64,
        flags: i32,
        reply: ReplyOpen,
    ) {
        if let Some((entry, _)) = self.get_entry_by_inode(ino) {
            let mut fh_map = self.inner.fh_map.write().unwrap();
            let id = if let Some(id) = fh_map.inode_to_fh.get(&ino).map(|fh| fh.id()) {
                *fh_map.ref_count.get_mut(&id).unwrap() += 1;
                id
            } else {
                let id = rand::random::<u64>();
                let fh = FileHandle::new(self.clone(), id, ino, entry).unwrap();
                fh_map.fh_to_inode.insert(fh.id(), ino);
                fh_map.ref_count.insert(fh.id(), 1);
                fh_map.inode_to_fh.insert(ino, fh);
                id
            };
            reply.opened(id, flags as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut fh_map = self.inner.fh_map.write().unwrap();
        let release = if let Some(id) = fh_map.ref_count.get_mut(&fh) {
            *id -= 1;
            *id == 0
        } else {
            false
        };

        if release {
            fh_map.fh_to_inode.remove(&fh);
            fh_map.inode_to_fh.remove(&ino);
            fh_map.ref_count.remove(&fh);
        }
        reply.ok();
    }

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

    fn getattr(
        &mut self, 
        _req: &Request, 
        ino: u64, 
        _fh: Option<u64>, 
        reply: ReplyAttr
    ) {
        if let Some((entry, _)) = self.get_entry_by_inode(ino) {
            reply.attr(
                &Duration::from_secs(119),
                &EntryAttr::from((entry, ino, true)).into()
            );
        } else {
            reply.error(ENOENT);
        }

    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Some((mut entry, fh)) = self.get_entry_by_inode(ino) {
            // TODO: wormEnforced
            let mut update_chunks = false;
            if let Some(size) = size {
                update_chunks = entry.truncate(size);
            }

            if let Some(mode) = mode {
                chmod(&mut entry.attributes.as_mut().unwrap().file_mode, mode);
            }

            if let Some(uid) = uid {
                entry.attributes.as_mut().unwrap().uid = uid;
            }

            if let Some(gid) = gid {
                entry.attributes.as_mut().unwrap().gid = gid;
            }

            if let Some(mtime) = mtime {
                entry.attributes.as_mut().unwrap().mtime = to_unix_time(mtime);
            }

            if let Some(atime) = atime {
                entry.attributes.as_mut().unwrap().mtime = to_unix_time(atime);
            }

            if let Some(fh) = fh {
                match fh.update_entry(entry.clone(), update_chunks) {
                    Ok(_) => (),
                    Err(e) => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            }

            match self.filer_client().update_entry(entry.clone()) {
                Ok(_) => (),
                Err(e) => {
                    reply.error(ENOENT);
                    return;
                }
            }

            reply.attr(
                &Duration::from_secs(119),
                &EntryAttr::from((entry, ino, true)).into()
            );
        } else {
            reply.error(ENOENT);
        }
    }
}
