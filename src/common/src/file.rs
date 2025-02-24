use anyhow::Result;
use std::cmp::max;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::pb::{*, filer_pb::Entry};
use crate::chunks::{self, UploadChunk};
use crate::ChunkGroup;
use crate::ChunkCache;
use crate::PageWriter;
use crate::SealedChunkPage;
use crate::weedfs::Weedfs;

struct FileHandleMutPart<C: ChunkCache> {
    entry: Entry,
    chunk_group: ChunkGroup<C>,
    page_writer: PageWriter,
    dirty: bool,
    unlinked: bool,
}

pub trait FileHandleOwner: 'static + Send + Sync {
    type FileHandleId: std::fmt::Debug + Send + Sync;
    fn get_path(&self, id: &Self::FileHandleId) -> Option<PathBuf>;
}

struct FileHandleInner<T: FileHandleOwner, C: ChunkCache> {
    id: T::FileHandleId, 
    fs: Weedfs<T, C>,
    mut_part: RwLock<FileHandleMutPart<C>>,
}

pub struct FileHandle<T: FileHandleOwner, C: ChunkCache> {
    inner: Arc<FileHandleInner<T, C>>,
}

impl<T: FileHandleOwner, C: ChunkCache> Clone for FileHandle<T, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: FileHandleOwner, C: ChunkCache> std::fmt::Debug for FileHandle<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileHandle {{ id: {:?} }}", self.inner.id)
    }
}


impl<T: FileHandleOwner, C: ChunkCache> FileHandle<T, C> {
    pub fn new(
        fs: Weedfs<T, C>,
        entry: Entry,
        id: T::FileHandleId,
    ) -> Result<Self> {        
        let file_size = entry.file_size();
        let mut entry = entry;
        entry.attributes.as_mut().unwrap().file_size = file_size;
        let chunk_group = ChunkGroup::new(fs.chunk_cache().clone());
        chunk_group.set_chunks(&fs, file_size, entry.chunks.clone())?;
      
        Ok(Self {
            inner: Arc::new(FileHandleInner {
                id,
                mut_part: RwLock::new(FileHandleMutPart {
                    entry,
                    chunk_group,
                    page_writer: PageWriter::new(fs.option().chunk_size_limit, fs.option().concurrent_writers),
                    dirty: false, 
                    unlinked: false,
                }),
                fs,
            }),
        })
    }

    pub fn id(&self) -> &T::FileHandleId {
        &self.inner.id
    }

    fn fs(&self) -> &Weedfs<T, C> {
        &self.inner.fs
    }

    pub fn rename(&self, name: String) {
        let mut mut_part = self.inner.mut_part.write().unwrap();
        mut_part.entry.name = name;
    }

    pub fn entry(&self) -> Entry {
        let mut_part = self.inner.mut_part.read().unwrap();
        mut_part.entry.clone()
    }

    pub fn update_entry(&self, entry: Entry, update_chunks: bool) -> Result<()> {
        let mut mut_part = self.inner.mut_part.write().unwrap();
        if update_chunks {
            mut_part.chunk_group.set_chunks(self.fs(), entry.attributes.as_ref().unwrap().file_size, entry.chunks.clone())?;
        }
        mut_part.entry = entry;
        mut_part.dirty = true;
        Ok(())
    }

    pub fn full_path(&self) -> Option<PathBuf> {
        self.fs().file_owner().get_path(&self.inner.id)
    }

    pub fn read(
        &self, 
        buff: &mut [u8], 
        offset: i64
    ) -> std::io::Result<(usize, u64)> {
        let mut_part = &self.inner.mut_part.read().unwrap();
        let (read, ts_ns) = {
            let file_size = mut_part.chunk_group.file_size();
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
                mut_part.chunk_group.read(self.fs(), buff, offset as u64)
                    .map_err(|e| {
                        log::error!("{:?} read: read from chunk group failed: {}", self, e);
                        e
                    })?
            }
        };

        let read = if let Some(max_stop) = mut_part.page_writer.read(buff, offset, ts_ns) {
            max(read, (max_stop as i64 - offset) as usize)
        } else {
            read
        };
        Ok((read as usize, ts_ns))
    }

    async fn upload(&self, uploads: Vec<(i64, u64)>) -> Result<()> {
        let full_path = self.full_path();
        if full_path.is_none() {
            return Err(anyhow::anyhow!("full path is none"));
        }
        let full_path = full_path.unwrap();
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
                        let chunk = self.fs().upload(full_path.as_path(), &reader.content, reader.offset, reader.ts_ns as i64).await
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
                mut_part.page_writer.post_uploaded(Some((chunk_index, ts_ns)));
            }
        }
        Ok(())
    }

    pub fn on_unlink(&self) {
        log::trace!("{:?} on_unlink", self);
        let mut mut_part = self.inner.mut_part.write().unwrap();
        mut_part.unlinked = true;
        mut_part.page_writer.post_uploaded(None);
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
            if mut_part.unlinked {
                mut_part.dirty = false;
                return Ok(());
            }
            let (uploads, waiter) = mut_part.page_writer.flush();
            if uploads.is_some() {
                mut_part.dirty = true;
            }
            (uploads, waiter)
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
            if let Some(full_path) = self.full_path() {
                let manifest_chunks = chunks::compact_chunks(self.fs(), self.fs(), full_path.as_path(), mut_part.entry.chunks.clone()).await
                    .map_err(|e| {
                        log::error!("{:?} flush: compact chunks failed: {}", self, e);
                        anyhow::anyhow!("compact chunks failed: {}", e)
                    })?;
                mut_part.entry.chunks = manifest_chunks;
                log::trace!("{:?} flush: create entry, chunks: {}", self, mut_part.entry.chunks.len());
           
                self.fs().filer_client().create_entry(&full_path.parent().unwrap(), &mut_part.entry).await
                    .map_err(|e| {
                        log::error!("{:?} flush: create entry failed: {}", self, e);
                        anyhow::anyhow!("create entry failed: {}", e)
                    })?;
            } else {
                log::trace!("{:?} flush: full path is none", self);
            }
            mut_part.dirty = false;
        }
        
        Ok(())
    }
}

