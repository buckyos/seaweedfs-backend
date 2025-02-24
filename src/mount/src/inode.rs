use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Mutex};
use dfs_common::pb::filer_pb::Entry;
use dfs_common::{ChunkCacheInMem, FileHandle, FileHandleOwner, Weedfs};
use md5::{Md5, Digest};

#[derive(Debug)]
pub struct FuseId {
    pub inode: u64,
    pub fh: u64,
}

pub type FuseFileHandle = FileHandle<FuseIdMap, ChunkCacheInMem>;

struct FileHandleInodeMap {
    ref_count: BTreeMap<u64, usize>,
    inode_to_fh: BTreeMap<u64, FuseFileHandle>,
    fh_to_inode: BTreeMap<u64, u64>,
}

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



const ROOT_INODE: u64 = 1;

trait PathExt {
    fn as_inode(&self, ts_ns: u64) -> u64;
}

impl PathExt for &Path {
    fn as_inode(&self, ts_secs: u64) -> u64 {
        let mut hasher = Md5::new();
        hasher.update(self.to_string_lossy().as_bytes());
        let hash = hasher.finalize();
        let hash_bytes: [u8; 8] = hash[..8].try_into().unwrap();
        u64::from_le_bytes(hash_bytes) + ts_secs * 37
    }
}

struct InodeEntry {
    path: Vec<PathBuf>,
    nlookup: u64,
    is_directory: bool,
}

struct InodeToPath {
    inode_to_path: BTreeMap<u64, InodeEntry>,
    path_to_inode: HashMap<PathBuf, u64>,
}

impl InodeToPath {
    fn new(root: PathBuf) -> Self {
        Self {
            inode_to_path: BTreeMap::from([(ROOT_INODE, InodeEntry { path: vec![root.clone()], nlookup: 1, is_directory: true })]),
            path_to_inode: HashMap::from([(root, ROOT_INODE)]),
        }
    }

    fn has_inode(&self, inode: u64) -> bool {
        self.inode_to_path.contains_key(&inode)
    }

    fn get_path(&self, inode: u64) -> Option<&Path> {
        self.inode_to_path.get(&inode).and_then(|entry| entry.path.first().map(|p| p.as_path()))
    }

    fn move_path(&mut self, old_path: &Path, new_path: &Path) -> (Option<u64>, Option<u64>) {
        let target_inode = self.path_to_inode.remove(new_path);
        if let Some(target_inode) = target_inode {
            self.remove_inode(target_inode);
        }
        let source_inode = self.path_to_inode.remove(old_path);
        if let Some(source_inode) = source_inode {
            self.path_to_inode.insert(new_path.to_path_buf(), source_inode);
        } else {
            return (source_inode, target_inode);
        }
        let source_inode = source_inode.unwrap();
        if let Some(entry) = self.inode_to_path.get_mut(&source_inode) {
            entry.path.iter_mut().for_each(|p| {
                if p == old_path {
                    *p = new_path.to_path_buf();
                }
            });
            if target_inode.is_some() {
                entry.nlookup += 1;
            }
        }
        (Some(source_inode), target_inode)
    }

    fn allocate_inode(&self, path: &Path, ts_ns: u64) -> u64 {
        if path.to_string_lossy() == "/" {
            return ROOT_INODE;
        }
        let mut inode = path.as_inode(ts_ns);
        while self.inode_to_path.contains_key(&inode) {
            inode += 1;
        }
        inode
    }

    fn remove_path(&mut self, path: &Path) {
        if let Some(inode) = self.path_to_inode.remove(path) {
            self.remove_inode(inode);
        }
    }

    fn remove_inode(&mut self, inode: u64) {
        if let Some(entry) = self.inode_to_path.get_mut(&inode) {
            entry.path.clear();
            if entry.path.is_empty() {
                self.inode_to_path.remove(&inode);
            }
        }
    }

    fn add_path(&mut self, inode: u64, path: PathBuf) {
        self.path_to_inode.insert(path.clone(), inode);
        self.inode_to_path.entry(inode).or_insert(InodeEntry { path: vec![], nlookup: 1, is_directory: false })
            .path.push(path);
    }

    fn lookup(
        &mut self, 
        path: &Path, 
        ts_secs: u64, 
        is_directory: bool, 
        is_hardlink: bool, 
        possible_inode: u64, 
        is_lookup: bool
    ) -> u64 {
        let exists_inode = if let Some(exists_inode) = self.path_to_inode.get(path).cloned() {
            exists_inode
        } else {
            let mut inode = if possible_inode == 0 {
                path.as_inode(ts_secs)
            } else {
                possible_inode
            };

            if !is_hardlink {
                while self.inode_to_path.contains_key(&inode) {
                    inode += 1;
                }
            }
            log::trace!("lookup: insert inode: {}, path: {}", inode, path.to_string_lossy());
            self.path_to_inode.insert(path.to_path_buf(), inode);
            inode
        };

        if let Some(entry) = self.inode_to_path.get_mut(&exists_inode) {
            if is_lookup {
                entry.nlookup += 1;
            }
        } else {
            if !is_lookup {
                self.inode_to_path.insert(exists_inode, InodeEntry { path: vec![path.to_path_buf()], nlookup: 0, is_directory });
            } else {
                self.inode_to_path.insert(exists_inode, InodeEntry { path: vec![path.to_path_buf()], nlookup: 1, is_directory });
            }
        }

        exists_inode
    }
}

struct IdMapInner {
    inode_to_path: RwLock<InodeToPath>,
    fh_map: RwLock<FileHandleInodeMap>, 
    dh_map: RwLock<BTreeMap<u64, Arc<Mutex<DirectoryHandle>>>>,
}

#[derive(Clone)]
pub struct FuseIdMap {
    inner: Arc<IdMapInner>,
}

impl FuseIdMap {
    pub fn new(filer_root_path: PathBuf) -> Self {
        Self {
            inner: Arc::new(IdMapInner {
                inode_to_path: RwLock::new(InodeToPath::new(filer_root_path)),
                fh_map: RwLock::new(FileHandleInodeMap {
                    ref_count: BTreeMap::new(),
                    inode_to_fh: BTreeMap::new(),
                    fh_to_inode: BTreeMap::new(),
                }),
                dh_map: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    pub fn get_file_by_id(&self, id: u64) -> Option<FuseFileHandle> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.fh_to_inode.get(&id).and_then(|inode| fh_map.inode_to_fh.get(&inode)).cloned()
    }

    pub fn get_file_by_inode(&self, inode: u64) -> Option<FuseFileHandle> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.inode_to_fh.get(&inode).cloned()
    }

    pub fn has_inode(&self, inode: u64) -> bool {
        self.inner.inode_to_path.read().unwrap().has_inode(inode)
    } 

    pub fn get_path_by_inode(&self, inode: u64) -> Option<PathBuf> {
        self.inner.inode_to_path.read().unwrap().get_path(inode).map(|path| path.to_path_buf())
    }

    pub fn lookup(&self, path: &Path, ts_secs: u64, is_directory: bool, is_hardlink: bool, possible_inode: u64, is_lookup: bool) -> u64 {
        self.inner.inode_to_path.write().unwrap().lookup(path, ts_secs, is_directory, is_hardlink, possible_inode, is_lookup)
    }

    pub fn move_path(&self, old_path: &Path, new_path: &Path) -> (Option<u64>, Option<u64>) {
        self.inner.inode_to_path.write().unwrap().move_path(old_path, new_path)
    }

    pub fn add_path(&self, inode: u64, path: PathBuf) {
        self.inner.inode_to_path.write().unwrap().add_path(inode, path);
    }

    pub fn remove_path(&self, path: &Path) {
        self.inner.inode_to_path.write().unwrap().remove_path(path);
    }

    pub fn open_directory(&self) -> (u64, Arc<Mutex<DirectoryHandle>>) {
        let mut dh_map = self.inner.dh_map.write().unwrap();
        let id = rand::random::<u64>();
        let dh = Arc::new(Mutex::new(DirectoryHandle::Init));
        dh_map.insert(id, dh.clone());
        (id, dh)
    }

    pub fn remove_directory(&self, id: u64) {
        let mut dh_map = self.inner.dh_map.write().unwrap();
        dh_map.remove(&id);
    }

    pub fn get_directory_handle(&self, id: u64) -> Option<Arc<Mutex<DirectoryHandle>>> {
        let dh_map = self.inner.dh_map.read().unwrap();
        dh_map.get(&id).cloned()
    }

    pub fn allocate_inode(&self, path: &Path, ts_secs: u64) -> u64 {
        self.inner.inode_to_path.read().unwrap().allocate_inode(path, ts_secs)
    }

    pub fn open_file(&self, fs: Weedfs<Self, ChunkCacheInMem>, inode: u64, entry: Entry) -> u64 {
        let mut fh_map = self.inner.fh_map.write().unwrap();
        if let Some(fh) = fh_map.inode_to_fh.get(&inode).map(|fh| fh.id().fh) {
            *fh_map.ref_count.get_mut(&fh).unwrap() += 1;
            fh
        } else {
            let fh = rand::random::<u64>();
            log::trace!("open: ino: {}, fh: {}, chunks: {}", inode, fh, entry.chunks.len());
            let file = FileHandle::new(fs, entry, FuseId { fh, inode }).unwrap();
            fh_map.fh_to_inode.insert(fh, inode);
            fh_map.ref_count.insert(fh, 1);
            fh_map.inode_to_fh.insert(inode, file);
            fh
        }
    }

    pub fn release_file(&self, fh: u64, inode: u64) {
        let mut fh_map = self.inner.fh_map.write().unwrap();
        let release = if let Some(id) = fh_map.ref_count.get_mut(&fh) {
            *id -= 1;
            *id == 0
        } else {
            false
        };

        if release {
            fh_map.fh_to_inode.remove(&fh);
            fh_map.inode_to_fh.remove(&inode);
            fh_map.ref_count.remove(&fh);
        }
    }
}



impl FileHandleOwner for FuseIdMap {
    type FileHandleId = FuseId;
    fn get_path(&self, id: &FuseId) -> Option<PathBuf> {
        self.get_path_by_inode(id.inode)
    }
}