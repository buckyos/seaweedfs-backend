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
use crate::file_handle::{FileHandleId, FileHandle};
struct FileHandleInodeMap {
    inode_to_fh: BTreeMap<u64, FileHandle>,
    fh_to_inode: BTreeMap<FileHandleId, u64>,
}

struct WfsInner {
    fh_map: RwLock<FileHandleInodeMap>,
}

#[derive(Clone)]
pub struct Wfs {
    inner: Arc<WfsInner>, 
}



impl Wfs {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WfsInner {
                fh_map: RwLock::new(FileHandleInodeMap {
                    inode_to_fh: BTreeMap::new(),
                    fh_to_inode: BTreeMap::new(),
                }),
            }),
        }
    }

    fn get_handle_by_id(&self, id: FileHandleId) -> Option<FileHandle> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.fh_to_inode.get(&id).and_then(|inode| fh_map.inode_to_fh.get(&inode)).cloned()
    }

    fn get_handle_by_inode(&self, inode: u64) -> Option<FileHandle> {
        let fh_map = self.inner.fh_map.read().unwrap();
        fh_map.inode_to_fh.get(&inode).cloned()
    }

    pub fn lookup(&self, file_id: &str) -> Result<Vec<String>> {
        unimplemented!()
    }
}


impl Filesystem for Wfs {
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
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
        match file_handle.read_from_chunks(&mut buffer, offset) {
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
