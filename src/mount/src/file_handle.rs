use anyhow::Result;
use std::cmp::max;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use dfs_common::pb::{*, filer_pb::{Entry, FuseAttributes}};
use dfs_common::chunks::{self, LookupFileId, UploadChunk};
use dfs_common::ChunkGroup;
use dfs_common::ChunkCache;
use dfs_common::PageWriter;
use dfs_common::SealedChunkPage;

pub type  FileHandleId = u64;

struct EntryAndChunks<T: ChunkCache> {
    entry: Entry,
    chunk_group: ChunkGroup<T>,
}

impl<T: ChunkCache> EntryAndChunks<T> {
    fn from_entry(lookup: &impl LookupFileId, chunk_cache: T, entry: Entry) -> Result<Self> {
        let file_size = entry.file_size();
        let mut entry = entry;
        entry.attributes.as_mut().unwrap().file_size = file_size;
        let chunk_group = ChunkGroup::new(chunk_cache);
        chunk_group.set_chunks(lookup, file_size, entry.chunks.clone())?;
        Ok(Self {
            entry,
            chunk_group,
        })
    }


    fn read_from_chunks(
        &self, 
        lookup: &impl LookupFileId, 
        buff: &mut [u8], 
        offset: i64
    ) -> std::io::Result<(usize, u64)> {
        let mut file_size = self.entry.attributes.as_ref().unwrap().file_size;
        if file_size == 0 {
            file_size = self.entry.file_size();
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
        if offset < self.entry.content.len() as i64 {
            buff.copy_from_slice(&self.entry.content[offset as usize..]);
            let total_read = self.entry.content.len() - offset as usize;
            return Ok((total_read, 0));
        }

        let (read, ts_ns) = self.chunk_group.read_at(lookup, file_size, buff, offset as u64)?;

        Ok((read, ts_ns))
    }
}

struct FileHandleMutPart<C: ChunkCache> {
    entry: EntryAndChunks<C>,
    page_writer: PageWriter,
    dirty: bool,
}


pub trait FileHandleOwner: 'static + Clone + ChunkCache + LookupFileId + UploadChunk + Send + Sync {
    fn chunk_size_limit(&self) -> usize;
    fn concurrent_writers(&self) -> usize;
    fn get_path(&self, inode: u64) -> Option<PathBuf>;
}


struct FileHandleInner<T: FileHandleOwner> {
    fh: FileHandleId,
    inode: u64, 
    counter: i64, 
    owner: T,
    mut_part: RwLock<FileHandleMutPart<T>>,
}

pub struct FileHandle<T: FileHandleOwner> {
    inner: Arc<FileHandleInner<T>>,
}

impl<T: FileHandleOwner> Clone for FileHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: FileHandleOwner> std::fmt::Debug for FileHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileHandle {{ fh: {} }}", self.inner.fh)
    }
}

impl<T: FileHandleOwner> FileHandle<T> {
    pub fn new(
        owner: T,
        handle_id: FileHandleId, 
        inode: u64, 
        entry: Entry,
    ) -> Result<Self> {
        let entry_and_chunks = EntryAndChunks::from_entry(&owner, owner.clone(), entry)?;
        Ok(Self {
            inner: Arc::new(FileHandleInner {
                fh: handle_id,
                counter: 1,
                inode,
                mut_part: RwLock::new(FileHandleMutPart {
                    entry: entry_and_chunks,
                    page_writer: PageWriter::new(owner.chunk_size_limit(), owner.concurrent_writers()),
                    dirty: false,
                }),
                owner,
            }),
        })
    }

    pub fn id(&self) -> FileHandleId {
        self.inner.fh
    }

    fn owner(&self) -> &T {
        &self.inner.owner
    }

    pub fn entry(&self) -> Entry {
        let mut_part = self.inner.mut_part.read().unwrap();
        mut_part.entry.entry.clone()
    }

    pub fn update_entry(&self, entry: Entry, update_chunks: bool) -> Result<()> {
        let mut mut_part = self.inner.mut_part.write().unwrap();
        if update_chunks {
            mut_part.entry.chunk_group.set_chunks(self.owner(), entry.attributes.as_ref().unwrap().file_size, entry.chunks.clone())?;
        }
        mut_part.entry.entry = entry;
        mut_part.dirty = true;
        Ok(())
    }

    pub fn full_path(&self) -> PathBuf {
        self.owner().get_path(self.inner.inode).unwrap()
    }

    pub fn read(
        &self, 
        buff: &mut [u8], 
        offset: i64
    ) -> std::io::Result<(usize, u64)> {
        let mut_part = &self.inner.mut_part.read().unwrap();
        let (read, ts_ns) = mut_part.entry.read_from_chunks(self.owner(), buff, offset)?;
        let max_stop = mut_part.page_writer.read(buff, offset, ts_ns);
        let read = max(read, (max_stop as i64 - offset) as usize);
        Ok((read as usize, ts_ns))
    }

    fn upload(&self, uploads: Vec<(i64, u64)>) -> Result<()> {
        let full_path = self.full_path();
        let file_id = full_path.file_name().unwrap().to_str().unwrap();
        for (chunk_index, ts_ns) in uploads {
            if let Some(mut chunks) = {
                if let Some(page) = {
                    let mut_part = self.inner.mut_part.read().unwrap();
                    mut_part.page_writer.get_sealed(chunk_index, ts_ns) 
                } {
                    let mut chunks = Vec::new();
                    let readers = page.split_readers();
                    for (reader, offset, ts_ns) in readers {
                        let chunk = self.owner().upload(reader, file_id, offset, ts_ns)?;
                        chunks.push(chunk);
                    }
                    Some(chunks)
                } else {
                    None
                }
            } {
                let mut mut_part = self.inner.mut_part.write().unwrap();
                mut_part.page_writer.post_uploaded(chunk_index, ts_ns);
                mut_part.entry.chunk_group.add_chunks(chunks.clone());
                mut_part.entry.entry.chunks.append(&mut chunks);
            }
        }
        Ok(())
    }

    pub fn write(
        &self, 
        data: &[u8], 
        offset: i64
    ) -> std::io::Result<usize> {
        let uploads = {
            let mut mut_part = self.inner.mut_part.write().unwrap();
            let ts_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            let file_size = &mut mut_part.entry.entry.attributes.as_mut().unwrap().file_size;
            *file_size = max(*file_size, offset as u64 + data.len() as u64);
    
            mut_part.page_writer.write(offset, data, ts_ns)
        };

        if let Some(uploads) = uploads {
            let fh = self.clone();
            tokio::spawn(async move {
                let _ = fh.upload(uploads);
            });
        }
       
        
        // if offset == 0 {
        //     // detect mime type
        //     fh.contentType = http.DetectContentType(data)
        // }
    
        // fh.dirtyMetadata = true
        Ok(data.len())
    }
}
