use anyhow::Result;
use dfs_common::pb::{filer_pb::{Entry, FileChunk, FileId, FuseAttributes}, *};
use std::{collections::{BTreeMap, HashMap}, ffi::OsStr, future::Future, str::FromStr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow
};
use libc::{c_int, ENAMETOOLONG, ENOENT, EIO, EINVAL, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFSOCK};
use dfs_common::pb::filer_pb::AssignVolumeRequest;
use dfs_common::chunks::{self, LookupFileId, UploadChunk};
use dfs_common::{ChunkCache, ChunkCacheInMem};
use crate::file::{FileHandle, FileHandleId, FileHandleOwner};
use crate::path::{InodeToPath, ROOT_INODE};
use rand::{RngCore, rng, prelude::SliceRandom};
use std::thread_local;
use std::cell::RefCell;
use tokio::runtime::Runtime;
use std::os::unix::fs::MetadataExt;
use futures::stream::StreamExt;

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(None);
}

fn check_name(name: &OsStr) -> Option<c_int> {
    if name.len() >= 256 {
        return Some(ENAMETOOLONG);
    }
    None
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
    pub umask: u32,
    pub allow_others: bool,

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
            umask: 0o022,
            allow_others: false,
            volume_server_access: VolumeServerAccess::Url,
        }
    }
}

struct FileHandleInodeMap {
    ref_count: BTreeMap<FileHandleId, usize>,
    inode_to_fh: BTreeMap<u64, FileHandle<Wfs>>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}


pub type DirectoryHandleId = u64;

pub enum DirectoryHandle {
    Init, 
    Reading {
        is_finished: bool,
        last_name: String,
        last_offset: i64,
    },
    Finished {
        last_offset: i64,
    },
    Error(String),
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
    dh_map: RwLock<BTreeMap<DirectoryHandleId, Arc<Mutex<DirectoryHandle>>>>,
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
       
        let mut options = vec![MountOption::AutoUnmount, MountOption::DefaultPermissions];
        if option.allow_others {
            options.push(MountOption::AllowOther);
        }
        let wfs = with_thread_local_runtime(Self::new(option))
        .map_err(|e| {
            log::error!("Failed to mount Wfs: {}", e);
            e
        })?;

        fuser::mount2(wfs, &path, &options)
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
                dh_map: RwLock::new(BTreeMap::new()),
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

    fn has_inode(&self, inode: u64) -> bool {
        self.inner.inode_to_path.read().unwrap().has_inode(inode)
    }        

    async fn get_entry_by_inode(&self, inode: u64) -> Option<(PathBuf, Entry, Option<FileHandle<Self>>)> {
        if inode == ROOT_INODE {
            return Some((PathBuf::from("/"), Entry {
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
        let path = self.get_path(inode);
        if path.is_none() {
            return None;
        }
        let path = path.unwrap();
        if let Some(fh) = self.get_handle_by_inode(inode) {
            Some((path.parent().unwrap().to_path_buf(), fh.entry(), Some(fh)))
        } else {
            self.get_entry_by_path(path.as_path()).await.map(|entry| (path.parent().unwrap().to_path_buf(), entry, None))
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
        
        let file_id = FileId::from_str(file_id)
            .map_err(|e| {
                anyhow::anyhow!("invalid file id: {}", e)
            })?;
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
            let mut locations = self.filer_client().lookup_volume(vec![vid.clone()]).await
                .map_err(|e| {
                    anyhow::anyhow!("lookup volume on filer failed: {}", e)
                })?;
            if locations.is_empty() {
                return Err(anyhow::anyhow!("no locations found for volume {}", vid));
            }
            let locations = locations.remove(&vid).unwrap();
            let mut same_dc_locations = Vec::new();
            let mut other_locations = Vec::new();
            for location in locations.locations {
                if location.data_center == self.option().data_center {
                    if pulic_url {
                        same_dc_locations.push(format!("http://{}/{}", location.public_url, file_id.to_string()));
                    } else {
                        same_dc_locations.push(format!("http://{}/{}", location.url, file_id.to_string()));
                    }
                } else {
                    if pulic_url {
                        other_locations.push(format!("http://{}/{}", location.public_url, file_id.to_string()));
                    } else {
                        other_locations.push(format!("http://{}/{}", location.url, file_id.to_string()));
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

    async fn list_entries(&self, mut reply: ReplyDirectory, path: PathBuf, fh: DirectoryHandleId, handle: Arc<Mutex<DirectoryHandle>>, start_from: String, is_plus: bool) {
        let mut dh = handle.lock().unwrap();
        let mut last_offset = match &*dh {
            DirectoryHandle::Init => {
                2
            },
            DirectoryHandle::Reading { last_offset, .. } => {
                *last_offset
            }, 
            _ => {
                unreachable!()
            }
        };
        match self.filer_client().list_entries(path.as_path(), &start_from, None).await {
            Err(e) => {
                log::trace!("list_entries: fh {} error", fh);
                *dh = DirectoryHandle::Error(e.to_string());
                reply.error(EIO);
            }
            Ok(mut entries) => {
                while let Some(entry) = entries.next().await {
                    last_offset += 1;
                    match entry {
                        Ok(Some(entry)) => {
                            log::trace!("list_entries: fh {} entry: {}", fh, entry.name);
                            let inode = self.inner.inode_to_path.write().unwrap().lookup(
                                &path.join(&entry.name), 
                                entry.attributes.as_ref().unwrap().mtime as u64, 
                                entry.is_directory, 
                                entry.hard_link_counter > 0, 
                                entry.attributes.as_ref().unwrap().inode,
                                 is_plus
                            );
                            let full = reply.add(
                                inode, 
                                last_offset, 
                                to_syscall_type(entry.attributes.as_ref().unwrap().file_mode), 
                                &entry.name
                            );
                            if full {
                                *dh = DirectoryHandle::Reading {
                                    is_finished: false,
                                    last_name: entry.name,
                                    last_offset,
                                };
                                reply.ok();
                                return;
                            }
                        }
                        Ok(None) => {
                            log::trace!("list_entries: fh {} finished", fh);
                            *dh = DirectoryHandle::Finished { last_offset };
                            reply.ok();
                            return;
                        }
                        Err(e) => {
                            *dh = DirectoryHandle::Error(e.to_string());
                            reply.error(EIO);
                            return; 
                        }
                    }
                }

                log::trace!("list_entries: fh {} finished", fh);
                *dh = DirectoryHandle::Finished { last_offset };
                reply.ok();
            }
        }
    }

    async fn rename_entry(&self, old_path: &Path, new_path: &Path) -> Result<()> {
        let mut stream = self.filer_client().rename_entry(old_path, new_path).await?;
        while let Some(resp) = stream.next().await {
            if let Err(e) = resp {
                return Err(e);
            }
            let resp = resp.unwrap();
            if let Some(new_entry) = resp.event_notification.as_ref().and_then(|e| e.new_entry.as_ref()) {
                let notification = resp.event_notification.as_ref().unwrap();
                let old_child = Path::new(&resp.directory).join(&notification.old_entry.as_ref().unwrap().name);
                let new_child = Path::new(&notification.new_parent_path).join(&new_entry.name);
                let (source_inode, _) = self.inner.inode_to_path.write().unwrap().move_path(&old_child, &new_child);
                if let Some(source_inode) = source_inode {
                    if let Some(fh) = self.get_handle_by_inode(source_inode) {
                        fh.rename(new_entry.name.clone());
                    }
                }
            }
        }
        Ok(())
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
            perm: (entry.attributes.as_ref().unwrap().file_mode & OS_MODE_PERM) as u16,
            flags: 0,
        }
    }
}

fn chmod(mode: &mut u32, new_mode: u32) {
    *mode = *mode & !OS_MODE_PERM | new_mode & OS_MODE_PERM
}

const RENAME_FLAG_EMPTY: u32 = 0;
const RENAME_FLAG_NO_REPLACE: u32 = 1;
const RENAME_FLAG_EXCHANGE: u32 = 2;
const RENAME_FLAG_WHITEOUT: u32 = 3;

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
const OS_MODE_PERM: u32 = 0o7777;


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
    to_os_file_type(mode) | (mode & OS_MODE_PERM)
}

fn to_unix_time(time: TimeOrNow) -> i64 {
    match time {
        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    }
}

fn new_hard_link_id() -> Vec<u8> {
    let mut id = vec![0; 17];
    id[0] = 0x01;
    rng().fill_bytes(&mut id[1..17]);
    id
}

impl Filesystem for Wfs {
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Some(err) = check_name(newname) {
            log::trace!("link: name too long, name: {}", newname.to_string_lossy());
            reply.error(err);
            return;
        }
        
        let old_entry_path = self.get_path(ino);
        if old_entry_path.is_none() {
            log::trace!("link: old entry not found, ino: {}", ino);
            reply.error(ENOENT);
            return;
        }
        let old_entry_path = old_entry_path.unwrap();
        
        let new_parent_path = self.get_path(newparent);
        if new_parent_path.is_none() {
            log::trace!("link: new parent not found, newparent: {}", newparent);
            reply.error(ENOENT);
            return;
        }
        let new_parent_path = new_parent_path.unwrap();

        let old_entry = with_thread_local_runtime(self.get_entry_by_path(old_entry_path.as_path()));
        if old_entry.is_none() {
            log::trace!("link: old entry not found, path: {}", old_entry_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let mut old_entry = old_entry.unwrap();

        // hardlink is not allowed in WORM mode
        // if wormEnforced, _ := wfs.wormEnforcedForEntry(oldEntryPath, oldEntry); wormEnforced {
        //     return fuse.EPERM
        // }

        // update old file to hardlink mode
        if old_entry.hard_link_id.is_empty() {
            old_entry.hard_link_id = new_hard_link_id();
            old_entry.hard_link_counter = 1;
        }
        old_entry.hard_link_counter += 1;
        old_entry.attributes.as_mut().unwrap().mtime = to_unix_time(TimeOrNow::Now);
       
        match with_thread_local_runtime(
            self.filer_client().update_entry(
                old_entry_path.parent().unwrap(),
                old_entry.clone()
            )
        ) {
            Ok(_) => {
            },
            Err(e) => {
                log::trace!("link: update entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }

        let mut new_entry = old_entry;
        new_entry.name = newname.to_string_lossy().to_string();

        match with_thread_local_runtime(
            self.filer_client().create_entry(
                &new_parent_path,
                &new_entry
            )
        ) {
            Ok(_) => {
            },
            Err(e) => {
                log::trace!("link: create entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }

        let new_entry_path = new_parent_path.join(newname);
        self.inner.inode_to_path.write().unwrap().add_path(
            ino,
            new_entry_path
        );

        reply.entry(    
            &self.option().ttl, 
            &EntryAttr::from((new_entry, ino, true)).into(), 
            1
        );
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if let Some(err) = check_name(link_name) {
            log::trace!("symlink: name too long, name: {}", link_name.to_string_lossy());
            reply.error(err);
            return;
        }
        let parent_path = self.get_path(parent);
        if parent_path.is_none() {
            log::trace!("symlink: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let entry = Entry {
            name: link_name.to_string_lossy().to_string(),
            is_directory: false,
            attributes: Some(FuseAttributes {
                mtime: now as i64,
                crtime: now as i64,
                file_mode: OS_MODE_SYMLINK | OS_MODE_PERM,
                symlink_target: target.to_string_lossy().to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        
        match with_thread_local_runtime(
            self.filer_client().create_entry(
                parent_path.as_path(), 
                &entry
            )
        ) {
            Ok(_) => {
            },
            Err(e) => {
                log::trace!("symlink: create entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }
        let entry_full_path = parent_path.join(link_name);
        let inode = self.inner.inode_to_path.write().unwrap().lookup(
            entry_full_path.as_path(), 
            now, 
            false, 
            false, 
            0, 
            true
        );

        reply.entry(
            &self.option().ttl, 
            &EntryAttr::from((entry, inode, true)).into(), 
            1
        );
    }


    fn readlink(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        reply: ReplyData,
    ) {
        let full_path = self.get_path(ino);
        if full_path.is_none() {
            log::trace!("readlink: ino not found, ino: {}", ino);
            reply.error(ENOENT);
            return;
        }
        let full_path = full_path.unwrap();
        let entry = with_thread_local_runtime(self.get_entry_by_path(full_path.as_path()));
        if entry.is_none() {
            log::trace!("readlink: entry not found, path: {}", full_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let entry = entry.unwrap();
        if entry.attributes.as_ref().unwrap().file_mode & OS_MODE_SYMLINK == 0 {
            log::trace!("readlink: entry is not a symlink, path: {}", full_path.to_string_lossy());
            reply.error(EINVAL);
            return;
        }
        reply.data(entry.attributes.as_ref().unwrap().symlink_target.as_bytes());
    }


    fn unlink(
        &mut self, 
        _req: &Request<'_>, 
        parent: u64, 
        name: &OsStr, 
        reply: ReplyEmpty
    ) {
        let parent_path = self.get_path(parent);
        if parent_path.is_none() {
            log::trace!("unlink: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        let entry_full_path = parent_path.join(name);
        let entry = with_thread_local_runtime(self.get_entry_by_path(entry_full_path.as_path()));
        if entry.is_none() {
            log::trace!("unlink: entry not found, path: {}", entry_full_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let is_delete_data = entry.as_ref().unwrap().hard_link_counter <= 1;
        match with_thread_local_runtime(
            self.filer_client().delete_entry(entry_full_path.as_path(), is_delete_data)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("unlink: delete entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }

        self.inner.inode_to_path.write().unwrap().remove_path(&entry_full_path.as_path());
        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        old_parent: u64,
        old_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if let Some(err) = check_name(new_name) {
            log::trace!("rename: new name too long, new_name: {}", new_name.to_string_lossy());
            reply.error(err);
            return;
        }
        let old_parent_path = self.get_path(old_parent);
        if old_parent_path.is_none() {
            log::trace!("rename: old parent not found, old_parent: {}", old_parent);
            reply.error(ENOENT);
            return;
        }
        let old_parent_path = old_parent_path.unwrap();
        let old_full_path = old_parent_path.join(old_name);

        let new_parent_path = self.get_path(new_parent);
        if new_parent_path.is_none() {
            log::trace!("rename: new parent not found, new_parent: {}", new_parent);
            reply.error(ENOENT);
            return;
        }
        let new_parent_path = new_parent_path.unwrap();
        let new_full_path = new_parent_path.join(new_name);

        if flags >= RENAME_FLAG_WHITEOUT {
            reply.error(EINVAL);
            return;
        }
      
        // FIXME: worm 
        // if wormEnforced, _ := wfs.wormEnforcedForEntry(oldPath, oldEntry); wormEnforced {
        //     return fuse.EPERM
        // }

        match with_thread_local_runtime(
            self.rename_entry(
                old_full_path.as_path(),
                new_full_path.as_path()
            )
        ) {
            Ok(_) => {
                reply.ok();
            },
            Err(e) => {
                log::trace!("rename: rename entry failed, error: {}", e);
                reply.error(EIO);
                return; 
            }
        }
    }


    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if let Some(err) = check_name(name) {
            log::trace!("mkdir: name too long, name: {}", name.to_string_lossy());
            reply.error(err);
            return;
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let entry = Entry {
            name: name.to_string_lossy().to_string(),
            is_directory: true,
            attributes: Some(FuseAttributes {
                mtime: now as i64,
                crtime: now as i64,
                file_mode: OS_MODE_DIR | (mode & !umask),
                uid: req.uid(),
                gid: req.gid(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let parent_path = self.get_path(parent);
        if parent_path.is_none() {
            log::trace!("mkdir: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        

        match with_thread_local_runtime(
            self.filer_client().create_entry(
                    parent_path.as_path(), 
                    &entry
                )
            ) {
            Ok(_) => {
            },
            Err(e) => {
                log::trace!("mkdir: create entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }
        

        let entry_full_path = parent_path.join(name);
        let inode = self.inner.inode_to_path.write().unwrap().lookup(
            entry_full_path.as_path(), 
            now, 
            true, 
            false, 
            0, 
            true
        );

        reply.entry(
            &self.option().ttl, 
            &EntryAttr::from((entry, inode, true)).into(), 
            1
        );
    }

    fn rmdir(
        &mut self, 
        _req: &Request<'_>, 
        parent: u64, 
        name: &OsStr, 
        reply: ReplyEmpty
    ) {
        let parent_path = self.get_path(parent);
        if parent_path.is_none() {
            log::trace!("rmdir: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        match with_thread_local_runtime(
            self.filer_client().delete_entry(&parent_path.join(name), true)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("rmdir: delete entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }
        let entry_full_path = parent_path.join(name);
        self.inner.inode_to_path.write().unwrap().remove_path(&entry_full_path.as_path());
        reply.ok();
    }

    fn opendir(
        &mut self, 
        _req: &Request<'_>, 
        ino: u64, 
        flags: i32, 
        reply: ReplyOpen
    ) {
        if !self.has_inode(ino) {
            reply.error(ENOENT);
            return;
        }
        let mut dh_map = self.inner.dh_map.write().unwrap();
        let id = rand::random::<u64>();
        dh_map.insert(id, Arc::new(Mutex::new(DirectoryHandle::Init)));
        reply.opened(id, flags as u32);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        let mut dh_map = self.inner.dh_map.write().unwrap();
        dh_map.remove(&fh);
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,  // 改为 bool 类型
        reply: ReplyEmpty,
    ) { 
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        mut offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let dh = self.inner.dh_map.read().unwrap().get(&fh).cloned();
        if dh.is_none() {
            log::trace!("readdir: fh not found, fh: {}", fh);
            reply.error(ENOENT);
            return;
        }

        if offset == 0 {
            offset = 1;
            if reply.add(
                ino,
                offset,
                FileType::Directory,
                "."
            ) {
                reply.ok();
                return;
            }
        }

        if offset == 1 {
            offset = 2;
            if reply.add(
                ino,
                offset,
                FileType::Directory,
                ".."
            ) {
                reply.ok();
                return;
            }
        }

        let path = self.get_path(ino).unwrap();
        let dh = dh.unwrap();
        let last_name = {
            let dh = dh.lock().unwrap();
            match &*dh {
                DirectoryHandle::Init => {
                   "".to_string()
                }
                DirectoryHandle::Reading { 
                    is_finished, 
                    last_name, 
                    last_offset 
                } => {
                    if offset <= *last_offset {
                        log::trace!("readdir: fh {} offset <= last_offset, offset: {}, last_offset: {}", fh, offset, *last_offset);
                        reply.error(EINVAL);
                        return; 
                    } 
                    if *is_finished {
                        log::trace!("readdir: fh {} is_finished, offset: {}, last_offset: {}", fh, offset, *last_offset);
                        reply.ok();
                        return;
                    }
                    last_name.clone()
                }
                DirectoryHandle::Finished { last_offset } => {
                    if offset >= *last_offset {
                        log::trace!("readdir: fh {} offset > last_offset, offset: {}, last_offset: {}", fh, offset, *last_offset);
                        reply.ok();
                    } else {
                        log::trace!("readdir: fh {} offset <= last_offset, offset: {}, last_offset: {}", fh, offset, *last_offset);
                        reply.error(EINVAL);
                    }
                    return; 
                }
                DirectoryHandle::Error(_) => {
                    log::trace!("readdir: fh {} exsiting error", fh);
                    reply.error(EIO);
                    return;
                }
            }
        };

        with_thread_local_runtime(self.list_entries(reply, path, fh, dh, last_name, false));
    }

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Some(err) = check_name(name) {
            log::trace!("lookup: name too long, name: {}", name.to_string_lossy());
            reply.error(err);
            return;
        }
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
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        log::trace!("mknod: parent: {}, name: {}, mode: {}, umask: {}, rdev: {}", parent, name.to_string_lossy(), mode, _umask, rdev);
        if let Some(err) = check_name(name) {
            log::trace!("mknod: name too long, name: {}", name.to_string_lossy());
            reply.error(err);
            return;
        }
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
                uid: req.uid(),
                gid: req.gid(),
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
            Some((_, entry, _)) => {
                let mut fh_map = self.inner.fh_map.write().unwrap();
                let id = if let Some(id) = fh_map.inode_to_fh.get(&ino).map(|fh| fh.id()) {
                    *fh_map.ref_count.get_mut(&id).unwrap() += 1;
                    id
                } else {
                    let id = rand::random::<u64>();
                    log::trace!("open: ino: {}, id: {}, chunks: {}", ino, id, entry.chunks.len());
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
            Some((_, entry, _)) => {
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
            Some((parent, mut entry, fh)) => {
                log::trace!("setattr: ino: {}, entry: {{ name: {}, is_directory: {} }}", ino, entry.name, entry.is_directory);
                // TODO: wormEnforced
                let mut update_chunks = false;
                if let Some(size) = size {
                    update_chunks = entry.truncate(size);
                    entry.attributes.as_mut().unwrap().mtime = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                    entry.attributes.as_mut().unwrap().file_size = size;
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
                    self.filer_client().update_entry(&parent, entry.clone())
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
