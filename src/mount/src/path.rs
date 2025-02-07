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

    pub fn allocate_inode(&self, path: &Path, ts_ns: u64) -> u64 {
        if path.to_string_lossy() == "/" {
            return ROOT_INODE;
        }
        let mut inode = path.as_inode(ts_ns);
        while self.inode_to_path.contains_key(&inode) {
            inode += 1;
        }
        inode
    }

    pub fn lookup(
        &mut self, 
        path: &Path, 
        ts_ns: u64, 
        is_directory: bool, 
        is_hardlink: bool, 
        possible_inode: u64, 
        is_lookup: bool
    ) -> u64 {
        let exists_inode = if let Some(exists_inode) = self.path_to_inode.get(path).cloned() {
            exists_inode
        } else {
            let mut inode = if possible_inode == 0 {
                path.as_inode(ts_ns)
            } else {
                possible_inode
            };

            if !is_hardlink {
                while self.inode_to_path.contains_key(&inode) {
                    inode += 1;
                }
            }
            inode
        };

        self.path_to_inode.insert(path.to_path_buf(), exists_inode);

        if let Some(entry) = self.inode_to_path.get_mut(&exists_inode) {
            if is_lookup {
                entry.nlookup += 1;
            }
        } else {
            if !is_lookup {
                self.inode_to_path.insert(exists_inode, InodeEntry { path: vec![path.to_path_buf()], nlookup: 0, is_directory: is_directory });
            } else {
                self.inode_to_path.insert(exists_inode, InodeEntry { path: vec![path.to_path_buf()], nlookup: 1, is_directory: is_directory });
            }
        }

        exists_inode
    }
}
