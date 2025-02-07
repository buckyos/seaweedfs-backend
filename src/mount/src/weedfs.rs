use anyhow::Result;
use dfs_common::{pb::{filer_pb::{Entry, FileChunk, FileId, FuseAttributes}, *}, SplitChunkPage};
use std::{collections::{BTreeMap, HashMap}, ffi::OsStr, str::FromStr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow
};
use libc::{ENOENT, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFSOCK};
use dfs_common::pb::filer_pb::AssignVolumeRequest;
use dfs_common::chunks::{self, LookupFileId, UploadChunk};
use dfs_common::{ChunkCache, ChunkCacheInMem};
use crate::file_handle::{FileHandle, FileHandleId, FileHandleOwner};
use crate::path::{InodeToPath, ROOT_INODE};
use rand::seq::SliceRandom;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeServerAccess {
    FilerProxy,
    PublicUrl,
    Url,
}

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

    pub replication: String,
    pub collection: String,
    pub ttl: Duration,
    pub disk_type: String,
    pub data_center: String,

    pub volume_server_access: VolumeServerAccess,
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
    chunk_cache: ChunkCacheInMem, 
    volume_location_cache: RwLock<HashMap<String, (Vec<String>, Vec<String>)>>,
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
                volume_location_cache: RwLock::new(HashMap::new()),
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
        let pulic_url = match self.option().volume_server_access {
            VolumeServerAccess::FilerProxy => {
                return Ok(vec![format!("http://{}/?proxyChunkId={}", self.option().filer_addresses[self.filer_client().prefer_address_index()], file_id)]);
            }, 
            VolumeServerAccess::PublicUrl => {
                true
            }, 
            VolumeServerAccess::Url => {
                false
            },
        };
        
        let file_id = FileId::from_str(file_id)?;
        let vid = file_id.volume_id.to_string();
        let cached_locations = {
            let vid_cache = self.inner.volume_location_cache.read().unwrap();
            if let Some(locations) = vid_cache.get(&vid) {
                Some(locations.clone())
            } else {
                None
            }
        };

        let (mut same_dc_locations, mut other_locations) = if let Some(locations) = cached_locations {
            locations
        } else {
            let mut locations = self.filer_client().lookup_volume(vec![vid.clone()])?;
            if locations.is_empty() {
                return Err(anyhow::anyhow!("no locations found for volume {}", vid));
            }
            let locations = locations.remove(&vid).unwrap();
            let mut same_dc_locations = Vec::new();
            let mut other_locations = Vec::new();
            for location in locations.locations {
                if location.data_center == self.option().data_center {
                    if pulic_url {
                        same_dc_locations.push(location.public_url);
                    } else {
                        same_dc_locations.push(location.url);
                    }
                } else {
                    if pulic_url {
                        other_locations.push(location.public_url);
                    } else {
                        other_locations.push(location.url);
                    }
                }
            }
            self.inner.volume_location_cache.write().unwrap().insert(vid, (same_dc_locations.clone(), other_locations.clone()));
            (same_dc_locations, other_locations)
        };
       
        
        same_dc_locations.shuffle(&mut rand::rng());
        other_locations.shuffle(&mut rand::rng());
        same_dc_locations.append(&mut other_locations);
        Ok(same_dc_locations)
    }
}

#[async_trait::async_trait]
impl UploadChunk for Wfs {
    async fn upload<'a>(
        &self,
        content: SplitChunkPage<'a>,
        full_path: &str,
        file_id: &str
    ) -> Result<FileChunk> {
        let assign_volume_req = AssignVolumeRequest {
            count: 1,
            replication: self.option().replication.clone(),
            collection: self.option().collection.clone(),
            ttl_sec: self.option().ttl.as_secs() as i32,
            disk_type: self.option().disk_type.clone(),
            data_center: self.option().data_center.clone(),
            rack: "".to_string(),
            data_node: "".to_string(),
            path: full_path.to_string(),
        };
        let assign_volume_resp = self.filer_client().async_assign_volume(assign_volume_req).await?;
        let location = assign_volume_resp.location.ok_or_else(|| anyhow::anyhow!("no location found for volume {}", file_id))?;
        let upload_url = match self.option().volume_server_access {
            VolumeServerAccess::FilerProxy => {
                format!("http://{}/?proxyChunkId={}", location.url, file_id)
            },
            VolumeServerAccess::PublicUrl => {
                format!("http://{}/{}", location.public_url, file_id)
            },
            VolumeServerAccess::Url => {
                format!("http://{}/{}", location.url, file_id)
            },
        };

        // FIXME: auth, gzip and cipher
        let result = chunks::async_retried_upload_chunk(&content, &upload_url, file_id).await?;
        // if content.offset == 0 {
        //     self.chunk_cache().set(&assign_volume_resp.file_id, Arc::new());
        // }
        Ok(FileChunk {
            offset: content.offset,
            size: result.size.unwrap() as u64,
            modified_ts_ns: content.ts_ns as i64,
            e_tag: result.content_md5.unwrap_or_default(),
            source_file_id: "".to_string(),
            fid: FileId::from_str(&assign_volume_resp.file_id).ok(),
            source_fid: None,
            cipher_key: result.cipher_key.unwrap_or_default(),
            is_compressed: result.gzip.unwrap_or(0) > 0,
            is_chunk_manifest: false,
            file_id: assign_volume_resp.file_id,
        })

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
        let (entry, inode, calculate_size) = (self.0, self.1, self.2);
        let size = if calculate_size {
            entry.file_size()
        } else if entry.attributes.as_ref().unwrap().file_mode & OS_MODE_SYMLINK != 0 {
            entry.attributes.as_ref().unwrap().symlink_target.len() as u64
        } else {
            0
        };
        
        FileAttr {
            ino: inode,
            blksize: BLOCK_SIZE as u32,
            size,
            blocks: (size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64,
            atime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.attributes.as_ref().unwrap().mtime as u64),
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.attributes.as_ref().unwrap().mtime as u64),
            ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.attributes.as_ref().unwrap().mtime as u64),
            crtime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.attributes.as_ref().unwrap().mtime as u64),
            kind: to_syscall_type(entry.attributes.as_ref().unwrap().file_mode),
            nlink: if entry.hard_link_counter > 0 {
                entry.hard_link_counter as u32
            } else {
                1
            },
            uid: entry.attributes.as_ref().unwrap().uid,
            gid: entry.attributes.as_ref().unwrap().gid,
            rdev: entry.attributes.as_ref().unwrap().rdev,
            perm: 0,
            flags: 0,
        }
    }
}

fn chmod(mode: &mut u32, new_mode: u32) {
    *mode = *mode & 0o777 | new_mode & 0o777
}

const OS_MODE_REGULAR: u32 = 0;
const OS_MODE_DIR: u32 = 1 << (32 - 1); 
const OS_MODE_APPEND: u32 = 1 << (32 - 2); 
const OS_MODE_EXCLUSIVE: u32 = 1 << (32 - 3); 
const OS_MODE_TEMPORARY: u32 = 1 << (32 - 4); 
const OS_MODE_SYMLINK: u32 = 1 << (32 - 5); 
const OS_MODE_DEVICE: u32 = 1 << (32 - 6); 
const OS_MODE_NAMED_PIPE: u32 = 1 << (32 - 7); 
const OS_MODE_SOCKET: u32 = 1 << (32 - 8); 
const OS_MODE_SETUID: u32 = 1 << (32 - 9);
const OS_MODE_SETGID: u32 = 1 << (32 - 10);
const OS_MODE_CHAR_DEVICE: u32 = 1 << (32 - 11);
const OS_MODE_STICKY: u32 = 1 << (32 - 12);
const OS_MODE_IRREGULAR: u32 = 1 << (32 - 13);

const OS_MODE_TYPE: u32 = OS_MODE_DIR
                | OS_MODE_SYMLINK
                | OS_MODE_NAMED_PIPE
                | OS_MODE_SOCKET
                | OS_MODE_DEVICE 
                | OS_MODE_CHAR_DEVICE
                | OS_MODE_IRREGULAR;
const OS_MODE_PERM: u32 = 0o777;


fn to_syscall_type(mode: u32) -> FileType {
    match mode & OS_MODE_TYPE {
        OS_MODE_DIR => FileType::Directory,
        OS_MODE_SYMLINK => FileType::Symlink,
        OS_MODE_NAMED_PIPE => FileType::NamedPipe,
        OS_MODE_SOCKET => FileType::Socket,
        OS_MODE_DEVICE => FileType::BlockDevice,
        OS_MODE_CHAR_DEVICE => FileType::CharDevice,
        _ => FileType::RegularFile,
    }
}

fn to_os_file_type(mode: u32) -> u32 {
    match mode & (S_IFMT & 0xffff) {
        S_IFDIR => OS_MODE_DIR as u32,
        S_IFLNK => OS_MODE_SYMLINK as u32,
        S_IFIFO => OS_MODE_NAMED_PIPE as u32,
        S_IFSOCK => OS_MODE_SOCKET as u32,
        S_IFBLK => OS_MODE_DEVICE as u32,
        S_IFCHR => OS_MODE_CHAR_DEVICE as u32,
        _ => OS_MODE_REGULAR as u32,
    }
}

fn to_os_file_mode(mode: u32) -> u32 {
    to_os_file_type(mode) | (mode & 0o7777)
}

fn to_unix_time(time: TimeOrNow) -> i64 {
    match time {
        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    }
}

impl Filesystem for Wfs {
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let dir_full_path = self.get_path(parent);
        if dir_full_path.is_none() {
            reply.error(ENOENT);
            return;
        }
        let dir_full_path = dir_full_path.unwrap();
        let entry_full_path = dir_full_path.join(name);
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        // FIXME: 有一致性问题
        let inode = self.inner.inode_to_path.read().unwrap().allocate_inode(entry_full_path.as_path(), now);
        let entry = Entry {
            name: name.to_string_lossy().to_string(),
            is_directory: false,
            attributes: Some(FuseAttributes {
                mtime: now as i64,
                crtime: now as i64,
                file_mode: to_os_file_mode(mode),
                uid: 0,
                gid: 0,
                ttl_sec: self.option().ttl.as_secs() as i32,
                rdev: rdev,
                inode: inode,
                ..Default::default()
            }),
            ..Default::default()
        };
        match self.filer_client().create_entry(entry_full_path.as_path(), &entry) {
            Ok(_) => (),
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        }
        let inode = self.inner.inode_to_path.write().unwrap().lookup(
            entry_full_path.as_path(), 
            now, 
            false, 
            false, 
            inode, 
            true
        );
        reply.entry(
            &self.option().ttl, 
            &EntryAttr::from((entry, inode, true)).into(), 
            1
        );
    }

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
            Err(_) => {
                reply.error(ENOENT);
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
                    Err(_) => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            }

            match self.filer_client().update_entry(entry.clone()) {
                Ok(_) => (),
                Err(_) => {
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
