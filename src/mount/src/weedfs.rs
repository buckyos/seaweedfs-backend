use anyhow::Result;
use dfs_common::pb::{filer_pb::{Entry, FileChunk, FileId, FuseAttributes}, *};
use std::{collections::{BTreeMap, HashMap}, ffi::OsStr, future::Future, str::FromStr};
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
use crate::file::{FileHandle, FileHandleId, FileHandleOwner};
use crate::path::{InodeToPath, ROOT_INODE};
use rand::seq::SliceRandom;
use std::thread_local;
use std::cell::RefCell;
use tokio::runtime::Runtime;
use std::os::unix::fs::MetadataExt;

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(None);
}


pub fn with_thread_local_runtime<F: Future>(f: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // log::trace!("using existing tokio runtime");
        handle.block_on(f)
    } else {
        RUNTIME.with(|rt| {
            if rt.borrow().is_none() {
                // log::trace!("creating tokio runtime");
                *rt.borrow_mut() = Some(Runtime::new().expect("Failed to create runtime"));
            } else {
                // log::trace!("using existing created tokio runtime");
            }

            let rt_ref = rt.borrow();
            let runtime = rt_ref.as_ref().unwrap();
            runtime.block_on(f)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeServerAccess {
    FilerProxy,
    PublicUrl,
    Url,
}

#[derive(Debug)]
pub struct WfsOption {
    pub filer_addresses: Vec<String>,
    pub chunk_size_limit: usize,
    pub concurrent_writers: usize,
    pub dir: PathBuf,
    pub dir_auto_create: bool,
    pub filer_mount_root_path: PathBuf,

    pub replication: String,
    pub collection: String,
    pub ttl: Duration,
    pub disk_type: String,
    pub data_center: String,

    pub volume_server_access: VolumeServerAccess,
}

impl Default for WfsOption {
    fn default() -> Self {
        Self {
            filer_addresses: vec!["localhost:8888".to_string()],
            filer_mount_root_path: PathBuf::from("/"),
            dir: PathBuf::from("/"),
            dir_auto_create: true,
            chunk_size_limit: 2 * 1024 * 1024,
            concurrent_writers: 32,
            ttl: Duration::from_secs(0),
            replication: "".to_string(),
            collection: "".to_string(),
            disk_type: "".to_string(),
            data_center: "".to_string(),
            volume_server_access: VolumeServerAccess::Url,
        }
    }
}

struct FileHandleInodeMap {
    ref_count: BTreeMap<FileHandleId, usize>,
    inode_to_fh: BTreeMap<u64, FileHandle<Wfs>>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}

struct WfsInner {
    option: WfsOption, 
    mount_mtime: SystemTime,
    mount_ctime: SystemTime,
    mount_mode: u32,
    mount_uid: u32,
    mount_gid: u32,
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
    pub fn mount(option: WfsOption) -> Result<()> {
        log::info!("Mounting Wfs with option: {:?}", option);
        let path = option.dir.clone(); 
        let wfs = with_thread_local_runtime(Self::new(option))
            .map_err(|e| {
                log::error!("Failed to mount Wfs: {}", e);
                e
            })?;
        fuser::mount2(wfs, &path, &[])
            .map_err(|e| {
                log::error!("Failed to mount Wfs: {}", e);
                e
            })?;
        Ok(())
    }
    
    pub async fn new(option: WfsOption) -> Result<Self> {
        if option.dir_auto_create && !option.dir.exists() {
            log::info!("Creating directory: {:?}", option.dir);
            std::fs::create_dir_all(&option.dir)
                .map_err(|e| {
                    log::error!("Failed to create directory: {}", e);
                    e
                })?;
        }
        let dir_meta = option.dir.metadata()
            .map_err(|e| {
                log::error!("Failed to get directory metadata: {}", e);
                e
            })?;
        
        let filer_gprc_addresses = option.filer_addresses.clone().into_iter().map(|addr| {
            let mut parts = addr.split(':');
            let host = parts.next().unwrap_or("localhost");
            let port = parts.next().unwrap_or("8888").parse::<u16>().unwrap_or(8888);
            format!("http://{}:{}", host, port + 10000)
        }).collect();
        let filer_client = FilerClient::connect(filer_gprc_addresses).await
            .map_err(|e| {
                log::error!("Failed to connect to filer: {}", e);
                e
            })?;

        Ok(Self {
            inner: Arc::new(WfsInner {
                mount_mode: OS_MODE_DIR | 0o777, 
                mount_uid: dir_meta.uid(),
                mount_gid: dir_meta.gid(),
                mount_mtime: SystemTime::now(), 
                mount_ctime: dir_meta.modified()?, 
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

    async fn get_entry_by_inode(&self, inode: u64) -> Option<(Entry, Option<FileHandle<Self>>)> {
        if inode == ROOT_INODE {
            return Some((Entry {
                name: "".to_string(),
                is_directory: true,
                attributes: Some(FuseAttributes {
                    mtime: self.inner.mount_mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    file_mode: self.inner.mount_mode,
                    uid: self.inner.mount_uid,
                    gid: self.inner.mount_gid,
                    crtime: self.inner.mount_ctime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    ..Default::default()
                }),
                ..Default::default()
            }, None));
        }
        if let Some(fh) = self.get_handle_by_inode(inode) {
            Some((fh.entry(), Some(fh)))
        } else {
            if let Some(path) = self.get_path(inode) {
                self.get_entry_by_path(path.as_path()).await.map(|entry| (entry, None))
            } else {
                None
            }
        }
    }

    async fn get_entry_by_path(&self, path: &Path) -> Option<Entry> {
        if path == self.option().filer_mount_root_path {
            return Some(Entry {
               name: "".to_string(),
               is_directory: true,
               attributes: Some(FuseAttributes {
                    mtime: self.inner.mount_mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    file_mode: self.inner.mount_mode,
                    uid: self.inner.mount_uid,
                    gid: self.inner.mount_gid,
                    crtime: self.inner.mount_ctime.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    ..Default::default()
               }),
               ..Default::default()
            });
        } 
        self.filer_client().lookup_directory_entry(path).await.ok().and_then(|entry| entry)
    }

    async fn lookup_volume(&self, file_id: &str) -> Result<Vec<String>> {
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
            let mut locations = self.filer_client().lookup_volume(vec![vid.clone()]).await?;
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

impl LookupFileId for Wfs {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>> {
        with_thread_local_runtime(self.lookup_volume(file_id))
    }
}

#[async_trait::async_trait]
impl UploadChunk for Wfs {
    async fn upload(
        &self,
        full_path: &Path,
        content: impl Send + Clone + Into<reqwest::Body>, 
        offset: i64,
        ts_ns: i64
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
            path: full_path.to_string_lossy().to_string(),
        };
        let assign_volume_resp = self.filer_client().assign_volume(assign_volume_req).await
            .map_err(|e| {
                anyhow::anyhow!("assign volume failed: {}", e)
            })?;
        let file_id = &assign_volume_resp.file_id;
        let location = assign_volume_resp.location
            .ok_or_else(|| anyhow::anyhow!("no location found"))?;
        
        log::trace!("path: {}, offset {}, ts: {} assign volume: {}", full_path.to_string_lossy(), offset, ts_ns, file_id);
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
        let result = chunks::retried_upload_chunk(
            &upload_url, 
            full_path.file_name().unwrap().to_string_lossy().to_string().as_str(), 
            content
        ).await
            .map_err(|e| {
                anyhow::anyhow!("upload chunk {} failed: {}", file_id, e)
            })?;
        // TODO: insert to chunk cache
        // if content.offset == 0 {
        //     self.chunk_cache().set(&assign_volume_resp.file_id, Arc::new());
        // }
        Ok(FileChunk {
            offset: offset,
            size: result.size.unwrap() as u64,
            modified_ts_ns: ts_ns,
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

    fn filer_client(&self) -> &FilerClient {
        &self.inner.filer_client
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
    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        log::trace!("lookup: parent: {}, name: {}", parent, name.to_string_lossy());
        let full_path = {
            let inode_to_path = self.inner.inode_to_path.read().unwrap();
            let dir_path = inode_to_path.get_path(parent);
            if dir_path.is_none() {
                log::trace!("lookup: parent not found, parent: {}", parent);
                reply.error(ENOENT);
                return;
            }
            let dir_path = dir_path.unwrap();
            dir_path.join(name)
        };

        let entry = with_thread_local_runtime(self.get_entry_by_path(full_path.as_path()));
        
        if entry.is_none() {
            log::trace!("lookup: entry not found, path: {}", full_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let mut entry = entry.unwrap();
        let inode= self.inner.inode_to_path.write().unwrap()
            .lookup(
                full_path.as_path(), 
                entry.attributes.as_ref().unwrap().crtime as u64, 
                entry.is_directory, 
                entry.hard_link_counter > 0, 
                entry.attributes.as_ref().unwrap().inode, 
                true
            );
        log::trace!("lookup: parent: {}, name: {}, inode: {}", parent, name.to_string_lossy(), inode);

        if let Some(file_handle) = self.get_handle_by_inode(inode) {
            log::trace!("lookup: parent: {}, name: {}, inode: {}, found file handle", parent, name.to_string_lossy(), inode);
            entry = file_handle.entry();
        }

        reply.entry(
            &self.option().ttl, 
            &EntryAttr::from((entry, inode, true)).into(), 
            1
        );

    }


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
        log::trace!("mknod: parent: {}, name: {}, mode: {}, umask: {}, rdev: {}", parent, name.to_string_lossy(), mode, _umask, rdev);
        let dir_full_path = self.get_path(parent);
        if dir_full_path.is_none() {
            log::trace!("mknod: parent not found, parent: {}", parent);
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


        

        match with_thread_local_runtime(
            self.filer_client().create_entry(dir_full_path.as_path(), &entry)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("mknod: parent: {}, name: {}, create entry failed, error: {}", parent, name.to_string_lossy(), e);
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
        log::trace!("open: ino: {}, flags: {}", ino, flags);
        match with_thread_local_runtime(
            self.get_entry_by_inode(ino)
        ) {
            Some((entry, _)) => {
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
            }
            None => {
                log::trace!("open: ino: {}, entry not found", ino);
                reply.error(ENOENT);
            }
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
        log::trace!("read: fh: {}, offset: {}, size: {}", fh, offset, size);
        let file_handle = self.get_handle_by_id(fh);
        if file_handle.is_none() {
            log::trace!("read: fh: {} not found", fh);
            reply.error(ENOENT);
            return;
        }

        // TODO: optimize buffer allocation
        let mut buffer = vec![0u8; size as usize];
        
        let file_handle = file_handle.unwrap();
        match file_handle.read(&mut buffer, offset) {
            Ok((n, _ts_ns)) => {
                log::trace!("read: fh: {}, offset: {}, size: {}, read: {}", fh, offset, size, n);
                // 只返回实际读取的数据
                reply.data(&buffer[..n]);
            },
            Err(e) => {
                log::trace!("read: fh: {}, offset: {}, size: {}, error: {}", fh, offset, size, e);
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
        log::trace!("write: fh: {}, offset: {}, len: {:?}", fh, offset, data.len());
        let file_handle = self.get_handle_by_id(fh);
        if file_handle.is_none() {
            log::trace!("write: fh: {} not found", fh);
            reply.error(ENOENT);
            return;
        }

        let file_handle = file_handle.unwrap();
        match file_handle.write(data, offset) {
            Ok(n) => {
                log::trace!("write: fh: {}, offset: {}, len: {:?}, written: {}", fh, offset, data.len(), n);
                reply.written(n as u32);
            }
            Err(e) => {
                log::trace!("write: fh: {}, offset: {}, len: {:?}, error: {}", fh, offset, data.len(), e);
                reply.error(ENOENT);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        log::trace!("flush: fh: {}", fh);
        let file_handle = self.get_handle_by_id(fh);
        if file_handle.is_none() {
            log::trace!("flush: fh: {} not found", fh);
            reply.error(ENOENT);
            return;
        }

        let file_handle = file_handle.unwrap();
        match with_thread_local_runtime(file_handle.flush()) {
            Ok(_) => {
                log::trace!("flush: fh: {} ok", fh);
                reply.ok();
            }
            Err(e) => {
                log::trace!("flush: fh: {}, error: {}", fh, e);
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
        log::trace!("getattr: ino: {}", ino);
        match with_thread_local_runtime(
            self.get_entry_by_inode(ino)
        ) {
            Some((entry, _)) => {
                reply.attr(
                    &self.option().ttl,
                    &EntryAttr::from((entry, ino, true)).into()
                );
            }
            None => {
                log::trace!("getattr: ino: {}, entry not found", ino);
                reply.error(ENOENT);
            }
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
        log::trace!("setattr: ino: {}, mode: {:?}, uid: {:?}, gid: {:?}, size: {:?}, atime: {:?}, mtime: {:?}", ino, mode, uid, gid, size, atime, mtime);
        match with_thread_local_runtime(
            self.get_entry_by_inode(ino)
        ) {
            Some((mut entry, fh)) => {
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
                            log::trace!("setattr: ino: {}, update entry failed, error: {}", ino, e);
                            reply.error(ENOENT);
                            return;
                        }
                    }
                }

                match with_thread_local_runtime(
                    self.filer_client().update_entry(entry.clone())
                ) {
                    Ok(_) => (),
                    Err(e) => {
                        log::trace!("setattr: ino: {}, update entry failed, error: {}", ino, e);
                        reply.error(ENOENT);
                        return;
                    }
                }

                reply.attr(
                    &self.option().ttl,
                    &EntryAttr::from((entry, ino, true)).into()
                );
            }
            None => {
                log::trace!("setattr: ino: {}, entry not found", ino);
                reply.error(ENOENT);
            }
        }
    }
}
