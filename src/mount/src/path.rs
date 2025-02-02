use std::collections::{BTreeMap, HashMap};
use std::path::{PathBuf, Path};
use md5::{Md5, Digest};

pub const ROOT_INODE: u64 = 1;

pub trait PathExt {
    fn as_inode(&self, ts_ns: u64) -> u64;
}

impl PathExt for &Path {
    fn as_inode(&self, ts_ns: u64) -> u64 {
        let mut hasher = Md5::new();
        hasher.update(self.to_string_lossy().as_bytes());
        let hash = hasher.finalize();
        let hash_bytes: [u8; 8] = hash[..8].try_into().unwrap();
        u64::from_le_bytes(hash_bytes) + ts_ns * 37
    }
}

struct InodeEntry {
    path: Vec<PathBuf>,
    nlookup: u64,
    is_directory: bool,
}

pub struct InodeToPath {
    inode_to_path: BTreeMap<u64, InodeEntry>,
    path_to_inode: HashMap<PathBuf, u64>,
}

impl InodeToPath {
    pub fn new(root: PathBuf) -> Self {
        Self {
            inode_to_path: BTreeMap::from([(ROOT_INODE, InodeEntry { path: vec![root.clone()], nlookup: 1, is_directory: true })]),
            path_to_inode: HashMap::from([(root, ROOT_INODE)]),
        }
    }

    pub fn get_path(&self, inode: u64) -> Option<&Path> {
        self.inode_to_path.get(&inode).and_then(|entry| entry.path.first().map(|p| p.as_path()))
    }
}
