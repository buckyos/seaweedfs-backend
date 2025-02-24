use anyhow::Result;
use dfs_common::pb::filer_pb::FuseAttributes;
use dfs_common::pb::{filer_pb::Entry, *};
use std::ffi::OsStr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::{
    FileAttr, FileType, TimeOrNow
};
use libc::{c_int, ENAMETOOLONG, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFSOCK};
use rand::{RngCore, rng};
use std::os::unix::fs::MetadataExt;

const BLOCK_SIZE: u64 = 512;

pub fn check_name(name: &OsStr) -> Option<c_int> {
    if name.len() >= 256 {
        return Some(ENAMETOOLONG);
    }
    None
}

pub struct EntryAttr(Entry, u64, bool);

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


pub fn chmod(mode: &mut u32, new_mode: u32) {
    *mode = *mode & !OS_MODE_PERM | new_mode & OS_MODE_PERM
}

pub const RENAME_FLAG_EMPTY: u32 = 0;
pub const RENAME_FLAG_NO_REPLACE: u32 = 1;
pub const RENAME_FLAG_EXCHANGE: u32 = 2;
pub const RENAME_FLAG_WHITEOUT: u32 = 3;

pub const OS_MODE_REGULAR: u32 = 0;
pub const OS_MODE_DIR: u32 = 1 << (32 - 1); 
pub const OS_MODE_APPEND: u32 = 1 << (32 - 2); 
pub const OS_MODE_EXCLUSIVE: u32 = 1 << (32 - 3); 
pub const OS_MODE_TEMPORARY: u32 = 1 << (32 - 4); 
pub const OS_MODE_SYMLINK: u32 = 1 << (32 - 5); 
pub const OS_MODE_DEVICE: u32 = 1 << (32 - 6); 
pub const OS_MODE_NAMED_PIPE: u32 = 1 << (32 - 7); 
pub const OS_MODE_SOCKET: u32 = 1 << (32 - 8); 
pub const OS_MODE_SETUID: u32 = 1 << (32 - 9);
pub const OS_MODE_SETGID: u32 = 1 << (32 - 10);
pub const OS_MODE_CHAR_DEVICE: u32 = 1 << (32 - 11);
pub const OS_MODE_STICKY: u32 = 1 << (32 - 12);
pub const OS_MODE_IRREGULAR: u32 = 1 << (32 - 13);

pub const OS_MODE_TYPE: u32 = OS_MODE_DIR
                | OS_MODE_SYMLINK
                | OS_MODE_NAMED_PIPE
                | OS_MODE_SOCKET
                | OS_MODE_DEVICE 
                | OS_MODE_CHAR_DEVICE
                | OS_MODE_IRREGULAR;
pub const OS_MODE_PERM: u32 = 0o7777;


pub fn to_syscall_type(mode: u32) -> FileType {
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

pub fn to_os_file_type(mode: u32) -> u32 {
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

pub fn to_os_file_mode(mode: u32) -> u32 {
    to_os_file_type(mode) | (mode & OS_MODE_PERM)
}

pub fn to_unix_time(time: TimeOrNow) -> i64 {
    match time {
        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    }
}

pub fn new_hard_link_id() -> Vec<u8> {
    let mut id = vec![0; 17];
    id[0] = 0x01;
    rng().fill_bytes(&mut id[1..17]);
    id
}

pub fn root_entry(dir: &Path) -> Result<Entry> {
    let dir_meta = dir.metadata()
    .map_err(|e| {
        log::error!("Failed to get directory metadata: {}", e);
        e
    })?;
    Ok(Entry {
        name: "".to_string(),
        is_directory: true,
        attributes: Some(FuseAttributes {
            crtime: dir_meta.modified()?.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            mtime: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            file_mode: OS_MODE_DIR | 0o777,
            uid: dir_meta.uid(),
            gid: dir_meta.gid(),
            ..Default::default()
        }),
        ..Default::default()
    })
}