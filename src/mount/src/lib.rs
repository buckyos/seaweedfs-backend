#![allow(dead_code)]

mod file_handle;
mod path;
mod weedfs;

use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;

const TTL: Duration = Duration::from_secs(1); // 1秒的缓存时间
const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: SystemTime::UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: SystemTime::UNIX_EPOCH,
    ctime: SystemTime::UNIX_EPOCH,
    crtime: SystemTime::UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

const HELLO_TXT_CONTENT: &str = "Hello, World!\n";
const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 14,
    blocks: 1,
    atime: SystemTime::UNIX_EPOCH,
    mtime: SystemTime::UNIX_EPOCH,
    ctime: SystemTime::UNIX_EPOCH,
    crtime: SystemTime::UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

pub struct HelloFS;

impl HelloFS {
    pub fn new() -> Self {
        HelloFS
    }

    pub fn mount<P: AsRef<std::path::Path>>(mountpoint: P) -> Result<(), Box<dyn std::error::Error>> {
        let options = vec![MountOption::RO, MountOption::FSName("hello".to_string())];
        fuser::mount2(Self::new(), mountpoint, &options)?;
        Ok(())
    }
}

impl Filesystem for HelloFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("hello.txt") {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    fn mount_and_test<F>(test_fn: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
    {
        let mount_point = "/tmp/hello_fs_test";
        
        // 确保挂载点存在
        fs::create_dir_all(mount_point)?;
        
        // 在后台启动文件系统
        let child = std::thread::spawn(move || {
            HelloFS::mount(mount_point).unwrap();
        });

        // 等待文件系统挂载
        std::thread::sleep(Duration::from_secs(1));

        // 运行测试
        let result = test_fn();

        // 卸载文件系统
        Command::new("fusermount")
            .args(["-u", mount_point])
            .status()?;

        // 清理
        fs::remove_dir(mount_point)?;

        result
    }

    #[test]
    fn test_read_hello_txt() -> Result<(), Box<dyn std::error::Error>> {
        mount_and_test(|| {
            let content = fs::read_to_string("/tmp/hello_fs_test/hello.txt")?;
            assert_eq!(content, HELLO_TXT_CONTENT);
            Ok(())
        })
    }

    #[test]
    fn test_list_directory() -> Result<(), Box<dyn std::error::Error>> {
        mount_and_test(|| {
            let entries: Vec<_> = fs::read_dir("/tmp/hello_fs_test")?
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            assert!(entries.contains(&"hello.txt".to_string()));
            Ok(())
        })
    }
} 