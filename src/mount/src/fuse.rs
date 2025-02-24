use anyhow::Result;
use dfs_common::pb::{filer_pb::{Entry, FuseAttributes}, *};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use fuser::{
    FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow
};
use libc::{ENOTEMPTY, ENOENT, EIO, EINVAL};
use dfs_common::*;
use crate::utils::*;
use crate::inode::*;
use futures::stream::StreamExt;

#[derive(Debug, Clone)]
pub struct FuseWeedfsOption {
    pub weedfs: WeedfsOption,
    pub dir: PathBuf,
    pub dir_auto_create: bool,
    pub umask: u32,
    pub allow_others: bool, 
    pub block_size: usize,
}

impl Default for FuseWeedfsOption {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/"),
            dir_auto_create: true,
            umask: 0o022,
            allow_others: false, 
            block_size: 512,
            weedfs: WeedfsOption::default(),
        }
    }
}


pub struct FuseWeedfs {
    option: FuseWeedfsOption, 
    id_map: FuseIdMap,
    weedfs: Weedfs<FuseIdMap, ChunkCacheInMem>,
}

impl FuseWeedfs {
    pub fn mount(option: FuseWeedfsOption) -> Result<()> {
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
    
    pub async fn new(option: FuseWeedfsOption) -> Result<Self> {
        let id_map = FuseIdMap::new(option.weedfs.filer_root_path.clone());

        if option.dir_auto_create && !option.dir.exists() {
            log::info!("Creating directory: {:?}", option.dir);
            std::fs::create_dir_all(&option.dir)
                .map_err(|e| {
                    log::error!("Failed to create directory: {}", e);
                    e
                })?;
        }
        let root_entry = root_entry(&option.dir)
            .map_err(|e| {
                log::error!("Failed to get directory metadata: {}", e);
                e
            })?;
        Ok(Self {
            weedfs: Weedfs::new(
                option.weedfs.clone(), 
                root_entry, 
                id_map.clone(), 
                ChunkCacheInMem::new(1024)
            ).await?,
            id_map: id_map,
            option,
        })
    }

    fn option(&self) -> &FuseWeedfsOption {
        &self.option
    }

    async fn get_entry_by_inode(&self, inode: u64) -> Option<(PathBuf, Entry, Option<FuseFileHandle>)> {
        let path = self.id_map.get_path_by_inode(inode);
        if path.is_none() {
            return None;
        }
        let path = path.unwrap();
        if let Some(fh) = self.id_map.get_file_by_inode(inode) {
            Some((path.parent().unwrap().to_path_buf(), fh.entry(), Some(fh)))
        } else {
            self.weedfs.get_entry_by_path(path.as_path()).await
                .map(|entry| (path.parent().unwrap_or(Path::new("/")).to_path_buf(), entry, None))
        }
    }

    async fn list_entries(&self, mut reply: ReplyDirectory, path: PathBuf, fh: u64, handle: Arc<Mutex<DirectoryHandle>>, start_from: String, is_plus: bool) {
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
        match self.weedfs.filer_client().list_entries(path.as_path(), &start_from, None).await {
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
                            let inode = self.id_map.lookup(
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
        let mut stream = self.weedfs.filer_client().rename_entry(old_path, new_path).await?;
        while let Some(resp) = stream.next().await {
            if let Err(e) = resp {
                return Err(e);
            }
            let resp = resp.unwrap();
            if let Some(new_entry) = resp.event_notification.as_ref().and_then(|e| e.new_entry.as_ref()) {
                let notification = resp.event_notification.as_ref().unwrap();
                let old_child = Path::new(&resp.directory).join(&notification.old_entry.as_ref().unwrap().name);
                let new_child = Path::new(&notification.new_parent_path).join(&new_entry.name);
                let (source_inode, _) = self.id_map.move_path(&old_child, &new_child);
                if let Some(source_inode) = source_inode {
                    if let Some(fh) = self.id_map.get_file_by_inode(source_inode) {
                        fh.rename(new_entry.name.clone());
                    }
                }
            }
        }
        Ok(())
    }
}



impl Filesystem for FuseWeedfs {
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
        
        let old_entry_path = self.id_map.get_path_by_inode(ino);
        if old_entry_path.is_none() {
            log::trace!("link: old entry not found, ino: {}", ino);
            reply.error(ENOENT);
            return;
        }
        let old_entry_path = old_entry_path.unwrap();
        
        let new_parent_path = self.id_map.get_path_by_inode(newparent);
        if new_parent_path.is_none() {
            log::trace!("link: new parent not found, newparent: {}", newparent);
            reply.error(ENOENT);
            return;
        }
        let new_parent_path = new_parent_path.unwrap();

        let old_entry = with_thread_local_runtime(self.weedfs.get_entry_by_path(old_entry_path.as_path()));
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
            self.weedfs.filer_client().update_entry(
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
            self.weedfs.filer_client().create_entry(
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
        self.id_map.add_path(
            ino,
            new_entry_path
        );

        reply.entry(    
            &self.option().weedfs.ttl, 
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
        let parent_path = self.id_map.get_path_by_inode(parent);
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
            self.weedfs.filer_client().create_entry(
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
        let inode = self.id_map.lookup(
            entry_full_path.as_path(), 
            now, 
            false, 
            false, 
            0, 
            true
        );

        reply.entry(
            &self.option().weedfs.ttl, 
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
        let full_path = self.id_map.get_path_by_inode(ino);
        if full_path.is_none() {
            log::trace!("readlink: ino not found, ino: {}", ino);
            reply.error(ENOENT);
            return;
        }
        let full_path = full_path.unwrap();
        let entry = with_thread_local_runtime(self.weedfs.get_entry_by_path(full_path.as_path()));
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
        let parent_path = self.id_map.get_path_by_inode(parent);
        if parent_path.is_none() {
            log::trace!("unlink: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        let entry_full_path = parent_path.join(name);
        let entry = with_thread_local_runtime(self.weedfs.get_entry_by_path(entry_full_path.as_path()));
        if entry.is_none() {
            log::trace!("unlink: entry not found, path: {}", entry_full_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let entry = entry.unwrap();
        let is_delete_data = entry.hard_link_counter <= 1;
        match with_thread_local_runtime(
            self.weedfs.filer_client().delete_entry(entry_full_path.as_path(), is_delete_data)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("unlink: delete entry failed, error: {}", e);
                reply.error(EIO);
                return;
            }
        }

        let file_handle = self.id_map.get_file_by_inode(entry.attributes.as_ref().unwrap().inode);
        if let Some(file_handle) = file_handle {
            file_handle.on_unlink();
        }
        self.id_map.remove_path(&entry_full_path.as_path());
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
        let old_parent_path = self.id_map.get_path_by_inode(old_parent);
        if old_parent_path.is_none() {
            log::trace!("rename: old parent not found, old_parent: {}", old_parent);
            reply.error(ENOENT);
            return;
        }
        let old_parent_path = old_parent_path.unwrap();
        let old_full_path = old_parent_path.join(old_name);

        let new_parent_path = self.id_map.get_path_by_inode(new_parent);
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
                if e.to_string().contains("is not empty") {
                    reply.error(ENOTEMPTY);
                } else {
                    reply.error(EIO);
                }
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

        let parent_path = self.id_map.get_path_by_inode(parent);
        if parent_path.is_none() {
            log::trace!("mkdir: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        

        match with_thread_local_runtime(
            self.weedfs.filer_client().create_entry(
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
        let inode = self.id_map.lookup(
            entry_full_path.as_path(), 
            now, 
            true, 
            false, 
            0, 
            true
        );

        reply.entry(
            &self.option().weedfs.ttl, 
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
        let parent_path = self.id_map.get_path_by_inode(parent);
        if parent_path.is_none() {
            log::trace!("rmdir: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let parent_path = parent_path.unwrap();
        match with_thread_local_runtime(
            self.weedfs.filer_client().delete_entry(&parent_path.join(name), true)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("rmdir: delete entry failed, error: {}", e);
                if e.to_string().contains("fail to delete non-empty folder") {
                    reply.error(ENOTEMPTY);
                } else {
                    reply.error(EIO);
                }
                return;
            }
        }
        let entry_full_path = parent_path.join(name);
        self.id_map.remove_path(&entry_full_path.as_path());
        reply.ok();
    }

    fn opendir(
        &mut self, 
        _req: &Request<'_>, 
        ino: u64, 
        flags: i32, 
        reply: ReplyOpen
    ) {
        if !self.id_map.has_inode(ino) {
            reply.error(ENOENT);
            return;
        }
        let (id, _) = self.id_map.open_directory();
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
        self.id_map.remove_directory(fh);
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
        let dh = self.id_map.get_directory_handle(fh);
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

        let path = self.id_map.get_path_by_inode(ino).unwrap();
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
            let dir_path = self.id_map.get_path_by_inode(parent);
            if dir_path.is_none() {
                log::trace!("lookup: parent not found, parent: {}", parent);
                reply.error(ENOENT);
                return;
            }
            let dir_path = dir_path.unwrap();
            dir_path.join(name)
        };

        let entry = with_thread_local_runtime(self.weedfs.get_entry_by_path(full_path.as_path()));
        
        if entry.is_none() {
            log::trace!("lookup: entry not found, path: {}", full_path.to_string_lossy());
            reply.error(ENOENT);
            return;
        }
        let mut entry = entry.unwrap();
        let inode= self.id_map.lookup(
                full_path.as_path(), 
                entry.attributes.as_ref().unwrap().crtime as u64, 
                entry.is_directory, 
                entry.hard_link_counter > 0, 
                entry.attributes.as_ref().unwrap().inode, 
                true
            );
        log::trace!("lookup: parent: {}, name: {}, inode: {}", parent, name.to_string_lossy(), inode);

        if let Some(file_handle) = self.id_map.get_file_by_inode(inode) {
            log::trace!("lookup: parent: {}, name: {}, inode: {}, found file handle", parent, name.to_string_lossy(), inode);
            entry = file_handle.entry();
        }

        reply.entry(
            &self.option().weedfs.ttl, 
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
        let dir_full_path = self.id_map.get_path_by_inode(parent);
        if dir_full_path.is_none() {
            log::trace!("mknod: parent not found, parent: {}", parent);
            reply.error(ENOENT);
            return;
        }
        let dir_full_path = dir_full_path.unwrap();
        let entry_full_path = dir_full_path.join(name);
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        // FIXME: 有一致性问题
        let inode = self.id_map.allocate_inode(entry_full_path.as_path(), now);
        let entry = Entry {
            name: name.to_string_lossy().to_string(),
            is_directory: false,
            attributes: Some(FuseAttributes {
                mtime: now as i64,
                crtime: now as i64,
                file_mode: to_os_file_mode(mode),
                uid: req.uid(),
                gid: req.gid(),
                ttl_sec: self.option().weedfs.ttl.as_secs() as i32,
                rdev: rdev,
                inode: inode,
                ..Default::default()
            }),
            ..Default::default()
        };


        

        match with_thread_local_runtime(
            self.weedfs.filer_client().create_entry(dir_full_path.as_path(), &entry)
        ) {
            Ok(_) => (),
            Err(e) => {
                log::trace!("mknod: parent: {}, name: {}, create entry failed, error: {}", parent, name.to_string_lossy(), e);
                reply.error(ENOENT);
                return;
            }
        }

        let inode = self.id_map.lookup(
            entry_full_path.as_path(), 
            now, 
            false, 
            false, 
            inode, 
            true
        );
        reply.entry(
            &self.option().weedfs.ttl, 
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
                let fh = self.id_map.open_file(self.weedfs.clone(), ino, entry);
                reply.opened(fh, flags as u32);
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
        self.id_map.release_file(fh, ino);
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
        let file_handle = self.id_map.get_file_by_id(fh);
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
        let file_handle = self.id_map.get_file_by_id(fh);
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
        let file_handle = self.id_map.get_file_by_id(fh);
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
        fh: Option<u64>, 
        reply: ReplyAttr
    ) {
        log::trace!("getattr: ino: {}", ino);
        match with_thread_local_runtime(
            self.get_entry_by_inode(ino)
        ) {
            Some((_, entry, _)) => {
                reply.attr(
                    &self.option().weedfs.ttl,
                    &EntryAttr::from((entry, ino, true)).into()
                );
            }
            None => {
                if let Some(file_handle) = fh.and_then(|fh| self.id_map.get_file_by_id(fh)) {
                    log::trace!("getattr: ino: {}, entry not found, but fh found {}", ino, file_handle.id().inode);
                    reply.attr(
                        &self.option().weedfs.ttl,
                        &EntryAttr::from((file_handle.entry(), ino, true)).into()
                    );
                } else {
                    log::trace!("getattr: ino: {}, entry not found", ino);
                    reply.error(ENOENT);
                }
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
                    self.weedfs.filer_client().update_entry(&parent, entry.clone())
                ) {
                    Ok(_) => (),
                    Err(e) => {
                        log::trace!("setattr: ino: {}, update entry failed, error: {}", ino, e);
                        reply.error(ENOENT);
                        return;
                    }
                }

                reply.attr(
                    &self.option().weedfs.ttl,
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
