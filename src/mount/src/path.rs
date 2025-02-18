use std::collections::{BTreeMap, HashMap};
use std::path::{PathBuf, Path};
use md5::{Md5, Digest};

pub const ROOT_INODE: u64 = 1;

pub trait PathExt {
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

    pub fn has_inode(&self, inode: u64) -> bool {
        self.inode_to_path.contains_key(&inode)
    }

    pub fn get_path(&self, inode: u64) -> Option<&Path> {
        self.inode_to_path.get(&inode).and_then(|entry| entry.path.first().map(|p| p.as_path()))
    }

    pub fn move_path(&mut self, old_path: &Path, new_path: &Path) -> (Option<u64>, Option<u64>) {
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

    pub fn remove_path(&mut self, path: &Path) {
        if let Some(inode) = self.path_to_inode.remove(path) {
            self.remove_inode(inode);
        }
    }

    pub fn remove_inode(&mut self, inode: u64) {
        if let Some(entry) = self.inode_to_path.get_mut(&inode) {
            entry.path.clear();
            if entry.path.is_empty() {
                self.inode_to_path.remove(&inode);
            }
        }
    }

    pub fn add_path(&mut self, inode: u64, path: PathBuf) {
        self.path_to_inode.insert(path.clone(), inode);
        self.inode_to_path.entry(inode).or_insert(InodeEntry { path: vec![], nlookup: 1, is_directory: false })
            .path.push(path);
    }

    pub fn lookup(
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
            inode
        };

        log::trace!("lookup: insert inode: {}, path: {}", exists_inode, path.to_string_lossy());
        self.path_to_inode.insert(path.to_path_buf(), exists_inode);

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
