use anyhow::Result;
use std::cmp::max;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use dfs_common::pb::{*, filer_pb::Entry};
use dfs_common::chunks::{self, LookupFileId, UploadChunk};
use dfs_common::ChunkGroup;
use dfs_common::ChunkCache;
use dfs_common::PageWriter;
use dfs_common::SealedChunkPage;

pub type FileHandleId = u64;

struct FileHandleMutPart<C: ChunkCache> {
    entry: Entry,
    chunk_group: ChunkGroup<C>,
    page_writer: PageWriter,
    dirty: bool,
}

pub trait FileHandleOwner: 'static + Clone + ChunkCache + LookupFileId + UploadChunk + Send + Sync {
    fn chunk_size_limit(&self) -> usize;
    fn concurrent_writers(&self) -> usize;
    fn get_path(&self, inode: u64) -> Option<PathBuf>;
    fn filer_client(&self) -> &FilerClient;
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
        let file_size = entry.file_size();
        let mut entry = entry;
        entry.attributes.as_mut().unwrap().file_size = file_size;
        let chunk_group = ChunkGroup::new(owner.clone());
        chunk_group.set_chunks(&owner, file_size, entry.chunks.clone())?;
      
        Ok(Self {
            inner: Arc::new(FileHandleInner {
                fh: handle_id,
                counter: 1,
                inode,
                mut_part: RwLock::new(FileHandleMutPart {
                    entry,
                    chunk_group,
                    page_writer: PageWriter::new(handle_id, owner.chunk_size_limit(), owner.concurrent_writers()),
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
        mut_part.entry.clone()
    }

    pub fn update_entry(&self, entry: Entry, update_chunks: bool) -> Result<()> {
        let mut mut_part = self.inner.mut_part.write().unwrap();
        if update_chunks {
            mut_part.chunk_group.set_chunks(self.owner(), entry.attributes.as_ref().unwrap().file_size, entry.chunks.clone())?;
        }
        mut_part.entry = entry;
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
        let (read, ts_ns) = {
            let mut file_size = mut_part.entry.attributes.as_ref().unwrap().file_size;
            if file_size == 0 {
                file_size = mut_part.entry.file_size();
            }
            if file_size == 0 {
                (0, 0)
            } else if offset as u64 == file_size {
                (0, 0)
            } else if offset as u64 > file_size {
                (0, 0)
            } else if offset < mut_part.entry.content.len() as i64 {
                log::trace!("{:?} read from entry content: offset: {}", self, offset);
                buff.copy_from_slice(&mut_part.entry.content[offset as usize..]);
                let total_read = mut_part.entry.content.len() - offset as usize;
                (total_read, 0)
            } else {
                mut_part.chunk_group.read_at(self.owner(), file_size, buff, offset as u64)?
            }
        };

        let max_stop = mut_part.page_writer.read(buff, offset, ts_ns);
        let read = max(read, (max_stop as i64 - offset) as usize);
        Ok((read as usize, ts_ns))
    }

    async fn upload(&self, uploads: Vec<(i64, u64)>) -> Result<()> {
        let full_path = self.full_path();
        for (chunk_index, ts_ns) in uploads {
            if let Some(mut chunks) = {
                if let Some(page) = {
                    let mut_part = self.inner.mut_part.read().unwrap();
                    mut_part.page_writer.get_sealed(chunk_index, ts_ns) 
                } {
                    let mut chunks = Vec::new();
                    let readers = page.split_readers();
                    log::trace!("{:?} upload: sealed index: {}, ts_ns: {}", self, chunk_index, ts_ns);
                    for reader in readers {
                        log::trace!("{:?} upload: sealed index: {}, offset: {}, ts_ns: {}", self, chunk_index, reader.offset, reader.ts_ns);
                        let chunk = self.owner().upload(full_path.as_path(), &reader.content, reader.offset, reader.ts_ns as i64).await
                            .map_err(|e| {
                                log::error!("{:?} upload: sealed index: {}, offset: {}, ts_ns: {}, error: {}", self, chunk_index, reader.offset, reader.ts_ns, e);
                                e
                            })?;
                        chunks.push(chunk);
                    }
                    Some(chunks)
                } else {
                    None
                }
            } {
                let mut mut_part = self.inner.mut_part.write().unwrap();
                mut_part.chunk_group.add_chunks(chunks.clone());
                mut_part.entry.chunks.append(&mut chunks);
                mut_part.page_writer.post_uploaded(chunk_index, ts_ns);
            }
        }
        Ok(())
    }

    pub fn write(&self, data: &[u8], offset: i64) -> std::io::Result<usize> {
        let uploads = {
            let mut mut_part = self.inner.mut_part.write().unwrap();
            let ts_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            let file_size = &mut mut_part.entry.attributes.as_mut().unwrap().file_size;
            *file_size = max(*file_size, offset as u64 + data.len() as u64);
            let uploads = mut_part.page_writer.write(offset, data, ts_ns);
            if uploads.is_some() {
                mut_part.dirty = true;
            }
            uploads
        };

        if let Some(uploads) = uploads {
            let fh = self.clone();
            tokio::spawn(async move {
                let _ = fh.upload(uploads).await;
            });
        }
        
        Ok(data.len())
    }

    pub async fn flush(&self) -> Result<()> {
        let (uploads, waiter) = {
            let mut mut_part = self.inner.mut_part.write().unwrap();
            mut_part.page_writer.flush()
        };
        
        if let Some(uploads) = uploads {
            let fh = self.clone();
            tokio::spawn(async move {
                let _ = fh.upload(uploads).await
                    .map_err(|e| {
                        log::error!("{:?} flush: upload failed: {}", fh, e);
                        e
                    });
            });
        }

        if let Some(waiter) = waiter {
            log::trace!("{:?} flush: wait all uploaded", self);
            let _ = waiter.await;
        }

        log::trace!("{:?} flush: wait all uploaded: ok", self);

        let mut mut_part = self.inner.mut_part.write().unwrap();    

        if mut_part.dirty {
            let manifest_chunks = chunks::compact_chunks(self.owner(), self.owner(), &self.full_path(), mut_part.entry.chunks.clone()).await?;
            mut_part.entry.chunks = manifest_chunks;
            log::trace!("{:?} flush: create entry, chunks: {}", self, mut_part.entry.chunks.len());
            self.owner().filer_client().create_entry(&self.full_path().parent().unwrap(), &mut_part.entry).await
                .map_err(|e| {
                    log::error!("{:?} flush: create entry failed: {}", self, e);
                    anyhow::anyhow!("create entry failed: {}", e)
                })?;
            mut_part.dirty = false;
        }
        
        Ok(())
    }
}

