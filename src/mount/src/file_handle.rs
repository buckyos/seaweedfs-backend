use anyhow::Result;
use std::sync::{Arc, RwLock};
use dfs_common::pb::filer_pb::Entry;
use dfs_common::chunks::{self, LookupFileId};
use dfs_common::ChunkGroup;
use dfs_common::ChunkCache;

pub type  FileHandleId = u64;

struct EntryAndChunks<T: ChunkCache> {
    entry: Entry,
    chunk_group: ChunkGroup<T>,
}

impl<T: ChunkCache> EntryAndChunks<T> {
    fn from_entry(lookup: &impl LookupFileId, chunk_cache: T, entry: Entry) -> Result<Self> {
        let file_size = chunks::file_size(Some(&entry));
        let mut entry = entry;
        entry.attributes.as_mut().unwrap().file_size = file_size;
        let chunk_group = ChunkGroup::new(chunk_cache);
        chunk_group.set_chunks(lookup, file_size, entry.chunks.clone())?;
        Ok(Self {
            entry,
            chunk_group,
        })
    }
}

struct FileHandleInner<T: ChunkCache> {
    fh: FileHandleId,
    counter: i64,
    locked_entry: RwLock<Option<EntryAndChunks<T>>>,
    inode: u64, 
}

pub struct FileHandle<T: ChunkCache> {
    inner: Arc<FileHandleInner<T>>,
}

impl<T: ChunkCache> Clone for FileHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ChunkCache> std::fmt::Debug for FileHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileHandle {{ fh: {} }}", self.inner.fh)
    }
}

impl<T: ChunkCache + Clone> FileHandle<T> {
    pub fn new(
        lookup: &impl LookupFileId,
        chunk_cache: T,  
        handle_id: FileHandleId, 
        inode: u64, 
        entry: Option<Entry>
    ) -> Self {
        let entry_and_chunks = entry.and_then(|entry| EntryAndChunks::from_entry(lookup, chunk_cache, entry).ok());
        Self {
            inner: Arc::new(FileHandleInner {
                fh: handle_id,
                counter: 1,
                inode,
                locked_entry: RwLock::new(entry_and_chunks),
            }),
        }
    }
}

impl<T: ChunkCache> FileHandle<T> {
    pub fn read_from_chunks(
        &self, 
        lookup: &impl LookupFileId, 
        buff: &mut [u8], 
        offset: i64
    ) -> std::io::Result<(usize, u64)> {
        let entry_and_chunks = self.inner.locked_entry.read().unwrap();
        if let Some(entry_and_chunks) = &*entry_and_chunks {
            let mut file_size = entry_and_chunks.entry.attributes.as_ref().unwrap().file_size;
            if file_size == 0 {
                file_size = chunks::file_size(Some(&entry_and_chunks.entry));
            }
            if file_size == 0 {
                return Ok((0, 0));
            } 
            if offset as u64 == file_size {
                return Ok((0, 0));
            } 
            if offset as u64 > file_size {
                return Ok((0, 0))
            }
            if offset < entry_and_chunks.entry.content.len() as i64 {
                buff.copy_from_slice(&entry_and_chunks.entry.content[offset as usize..]);
                let total_read = entry_and_chunks.entry.content.len() - offset as usize;
                return Ok((total_read, 0));
            }

            let (read, ts_ns) = entry_and_chunks.chunk_group.read_at(lookup, file_size, buff, offset as u64)?;

            Ok((read, ts_ns))
        } else {
            Ok((0, 0))
        }
        
    }
}
