use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::sync::RwLock;
use anyhow::{Result, Error};
use crate::pb::filer_pb::FileChunk;
use crate::chunks;

pub type SectionIndex = u64;
const SECTION_SIZE: u64 = 2 * 1024 * 1024 * 32; // 64MiB
pub struct FileChunkSection {
    pub section_index: SectionIndex,
    pub chunks: Vec<FileChunk>,

}

impl FileChunkSection {
    pub fn read_at(&self, file_size: u64, buf: &mut [u8], offset: u64) -> std::io::Result<(usize, u64)> {
        Ok((0, 0))
    }
}

pub struct ChunkGroup {
    sections: RwLock<BTreeMap<SectionIndex, FileChunkSection>>,
}

impl ChunkGroup {
    pub fn new() -> Self {
        Self {
            sections: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn set_chunks(&self, lookup: impl Clone + Fn(&str) -> Result<Vec<String>>, chunks: Vec<FileChunk>) -> Result<()> {
        let mut sections = self.sections.write().unwrap();
        let mut data_chunks = Vec::new();
        for chunk in chunks {
            if !chunk.is_chunk_manifest {
                data_chunks.push(chunk);
                continue;
            }

            let mut resolved_chunks = chunks::resolve_chunk_manifest(lookup.clone(), &chunk)?;
            data_chunks.append(&mut resolved_chunks);
        }   

        for chunk in data_chunks {
            let index_range = chunk.offset as u64 / SECTION_SIZE..(chunk.offset as u64 + chunk.size) / SECTION_SIZE + 1;
            for si in index_range {
                if let Some(section) = sections.get_mut(&si) {
                    section.chunks.push(chunk.clone());
                } else {
                    let section = FileChunkSection {
                        section_index: si,
                        chunks: vec![chunk.clone()],
                    };
                    sections.insert(si, section);
                }
            }
        }

        Ok(())
    }

    pub fn read_at(&self, file_size: u64, buf: &mut [u8], offset: u64) -> std::io::Result<(usize, u64)> {
        if offset >= file_size {
            return Ok((0, 0));
        }
        let mut read = 0;
        let mut ts_ns = 0;
        let sections = self.sections.read().unwrap();
        let index_range = offset / SECTION_SIZE..(offset + buf.len() as u64) / SECTION_SIZE + 1;
        for si in index_range {
            let read_range = max(offset, si * SECTION_SIZE)..min(offset + buf.len() as u64, (si + 1) * SECTION_SIZE);
            if let Some(section) = sections.get(&si) {
                let (n, ns) = section.read_at(file_size, &mut buf[(read_range.start - offset) as usize..(read_range.end - offset) as usize], read_range.start)?;
                read += n;
                ts_ns = max(ts_ns, ns);
            } else {
                let read_range = read_range.start..min(read_range.end, file_size);
                buf[(read_range.start - offset) as usize..(read_range.end - offset) as usize].fill(0);
            }
        }

        Ok((read, ts_ns))
    }
}

