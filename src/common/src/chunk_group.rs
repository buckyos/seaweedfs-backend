use std::cmp::{max, min};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Mutex, RwLock};
use anyhow::Result;
use crate::pb::filer_pb::FileChunk;
use crate::chunks::{self, LookupFileId, VisibleInterval};
use crate::interval_list::{IntervalList, Interval};
use crate::reader_cache::{ReaderCache, ChunkView};
use crate::chunk_cache::ChunkCache;
pub type SectionIndex = u64;
const SECTION_SIZE: u64 = 2 * 1024 * 1024 * 32; // 64MiB

struct ReaderPattern {
    is_sequential_counter: i64,
    last_read_stop_offset: i64,
}

impl ReaderPattern {
    fn new() -> Self {
        Self {
            is_sequential_counter: 0,
            last_read_stop_offset: 0,
        }
    }

    fn mode_change_limit() -> i64 {
        3
    }

    fn monitor_read_at(&mut self, offset: i64, size: usize) {
        let last_offset = self.last_read_stop_offset;
        self.last_read_stop_offset = offset + size as i64;
        if last_offset == offset {
            if self.is_sequential_counter < Self::mode_change_limit() {
                self.is_sequential_counter += 1;
            }
        } else {
            if self.is_sequential_counter > -Self::mode_change_limit() {
                self.is_sequential_counter -= 1;
            }
        }
    }

    fn is_random_mode(&self) -> bool {
        self.is_sequential_counter < 0
    }
}

struct SectionMutPart {
    reader_pattern: ReaderPattern,
    last_chunk_fid: Option<String>,
}

impl fmt::Debug for FileChunkSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileChunkSection {{ index: {} }}", self.section_index)
    }
}

struct FileChunkSection {
    section_index: SectionIndex,
    chunks: Vec<FileChunk>,
    visibles: IntervalList<VisibleInterval>,
    chunk_views: IntervalList<ChunkView>, 
    mut_part: Mutex<SectionMutPart>,
}

impl FileChunkSection {
    fn new(section_index: SectionIndex, chunks: Vec<FileChunk>) -> Self {
        log::trace!("new: section_index: {}, chunks: {:?}", section_index, chunks);
        // FIXME: should read resolved chunks when read at
        let section_range = (section_index * SECTION_SIZE) as i64..((section_index + 1) * SECTION_SIZE) as i64;
        let visibles = chunks::read_resolved_chunks(&chunks, section_range.clone());
        log::trace!("new: section_index: {}, visibles: {:?}", section_index, visibles);
        let visables = visibles.iter().map(|visable| &visable.value.file_id).collect::<HashSet<_>>();
        let (compacted, _) = chunks.into_iter().partition(|chunk| visables.contains(&chunk.file_id));
        let chunk_views = visibles.iter().map(|visable| {
            let view_range = max(section_range.start, visable.range.start)..min(section_range.end, visable.range.end);
            let view = Interval {
                ts_ns: visable.ts_ns,
                value: ChunkView {
                    file_id: visable.value.file_id.clone(),
                    offset_in_chunk: visable.range.start - view_range.start + visable.value.offset_in_chunk,
                    chunk_size: visable.value.chunk_size,
                    view_size: (view_range.end - view_range.start) as u64,
                    view_offset: view_range.start,
                },
                range: view_range,
            };
            log::trace!("new: section_index: {}, chunk_view: {:?}", section_index, view);
            view
        }).collect();
        
        Self {
            visibles,
            chunk_views,
            section_index,
            chunks: compacted,
            mut_part: Mutex::new(SectionMutPart {
                reader_pattern: ReaderPattern::new(),
                last_chunk_fid: None,
            }),
        }
    }

    fn add_chunk(&mut self, chunk: FileChunk) {
        let range = max(chunk.offset, (self.section_index * SECTION_SIZE) as i64)..min(chunk.offset + chunk.size as i64, ((self.section_index + 1) * SECTION_SIZE) as i64);
        self.visibles.insert(
            Interval {
                ts_ns: chunk.modified_ts_ns,
                range: range.clone(),
                value: VisibleInterval {
                    file_id: chunk.file_id.to_string(),
                    offset_in_chunk: range.start - chunk.offset,
                    chunk_size: chunk.size,
                },
            }
        );

        let garbage_file_ids = self.visibles.iter().filter(|visible| {
            let offset = visible.range.start - visible.value.offset_in_chunk;
            offset >= range.start && offset + visible.value.chunk_size as i64 <= range.end
        }).map(|visible| &visible.value.file_id).collect::<HashSet<_>>();

        self.chunks.retain(|chunk| !garbage_file_ids.contains(&chunk.file_id));

        self.chunk_views.insert(
            Interval {
                ts_ns: chunk.modified_ts_ns,
                range: range.clone(),
                value: ChunkView {
                    file_id: chunk.file_id.to_string(),
                    offset_in_chunk: range.start - chunk.offset,
                    chunk_size: chunk.size,
                    view_size: (range.end - range.start) as u64,
                    view_offset: range.start,
                },
            }
        );


        self.chunks.push(chunk);
    }

    

    fn read_chunk_slice_at<'a, T: ChunkCache>(
        &self, 
        lookup: &impl LookupFileId, 
        reader_cache: &ReaderCache<T>, 
        buf: &mut [u8], 
        chunk_view: &ChunkView, 
        next_chunk_views: impl Iterator<Item = &'a ChunkView>,
        offset: u64
    ) -> std::io::Result<usize> {
        log::trace!("read_chunk_slice_at: section: {}, chunk: {}, offset: {}", self.section_index, chunk_view.file_id, offset);
        let mut mut_part = self.mut_part.lock().unwrap();
        if mut_part.reader_pattern.is_random_mode() {
            match reader_cache.outer_cache().read(buf, &chunk_view.file_id, offset as usize) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            log::trace!("read_chunk_slice_at: fetch_chunk_range: section: {}, chunk: {}", self.section_index, chunk_view.file_id);
                            return chunks::fetch_chunk_range(lookup, buf, &chunk_view.file_id, offset as i64)
                                .map_err(|e| {
                                    log::trace!("read_chunk_slice_at: fetch_chunk_range: section: {}, chunk: {}, error: {}", self.section_index, chunk_view.file_id, e);
                                    std::io::Error::new(std::io::ErrorKind::Other, e)
                                });
                        }
                        _ => return Err(e),
                    }
                }
            }
        } else {
            let should_cache = (chunk_view.view_offset as u64 + chunk_view.view_size) <= reader_cache.outer_cache().max_file_part_size();
            let read = reader_cache.read_at(lookup, buf, &chunk_view.file_id, offset as usize, chunk_view.chunk_size as usize, should_cache)?;
            if mut_part.last_chunk_fid.is_none() || mut_part.last_chunk_fid.as_ref().unwrap() != &chunk_view.file_id {
                if let Some(last_chunk_fid) = mut_part.last_chunk_fid.as_ref() {
                    reader_cache.uncache(last_chunk_fid);
                }
                reader_cache.maybe_cache(lookup, next_chunk_views);
            }
            mut_part.last_chunk_fid = Some(chunk_view.file_id.clone());
            return Ok(read);
        }
    }

    fn read_at<T: ChunkCache>(
        &self, 
        lookup: &impl LookupFileId, 
        file_size: u64,
        reader_cache: &ReaderCache<T>, 
        buf: &mut [u8], 
        offset: i64
    ) -> std::io::Result<(usize, u64)> {
        log::trace!("read_at: section: {:?}, offset: {}, size: {}", self, offset, buf.len());
        {
            let mut mut_part = self.mut_part.lock().unwrap();
            mut_part.reader_pattern.monitor_read_at(offset, buf.len() as usize);
        }

        let mut start = offset;
        let mut read = 0;
        let mut ts_ns = 0;
        let mut remaining = buf.len() as i64;
        for (i, view) in self.chunk_views.iter().enumerate() {
            if start < view.value.view_offset {
                let gap = (view.value.view_offset - start) as usize;
                buf[(start - offset) as usize..(start - offset) as usize + gap].fill(0);
                start = view.value.view_offset;
                read += gap;
                remaining -= gap as i64;
                if remaining <= 0 {
                    break;
                }
            }

            let chunk_range = max(view.value.view_offset, start)..min(view.value.view_offset + view.value.view_size as i64, start + remaining);
            ts_ns = view.ts_ns;
            let chunk_offset = chunk_range.start - view.value.view_offset + view.value.offset_in_chunk;
            
            let n = self.read_chunk_slice_at(
                lookup, 
                reader_cache, 
                &mut buf[(start - offset) as usize..(start - offset) as usize + (chunk_range.end - chunk_range.start) as usize], 
                &view.value, 
                self.chunk_views.iter().skip(i + 1).map(|v| &v.value),
                chunk_offset as u64
            )?;
            start += n as i64;
            remaining -= n as i64;
            read += n;
            if remaining <= 0 {
                break;
            }
        }

        if remaining > 0 {
            let delta = min(remaining as usize, (file_size - start as u64) as usize);
            log::trace!("read_at: section: {:?}, remaining: {}, delta: {}", self, remaining, delta);
            buf[(start - offset) as usize..(start - offset) as usize + delta].fill(0);
            read += delta;
        }

        Ok((read, ts_ns as u64))

    }
}

struct SectionsAndFileSize {
    sections: BTreeMap<SectionIndex, FileChunkSection>,
    file_size: u64,
}

// file -> section -> chunk
pub struct ChunkGroup<T: ChunkCache> {
    mut_part: RwLock<SectionsAndFileSize>, 
    reader_cache: ReaderCache<T>,
}

impl<T: ChunkCache> ChunkGroup<T> {
    pub fn new(chunk_cache: T) -> Self {
        Self {
            mut_part: RwLock::new(SectionsAndFileSize {
                sections: BTreeMap::new(),
                file_size: 0,
            }),
            reader_cache: ReaderCache::new(chunk_cache),
        }
    }

    pub fn add_chunks(
        &self, 
        chunks: Vec<FileChunk>
    ) {
        let mut mut_part = self.mut_part.write().unwrap();
        for chunk in chunks {
            for section_index in chunk.offset as u64 / SECTION_SIZE..(chunk.offset as u64 + chunk.size) / SECTION_SIZE + 1 {
                log::trace!("add_chunks: section_index: {}, chunk: {}", section_index, chunk.file_id);
                mut_part.sections.entry(section_index).or_insert(FileChunkSection::new(section_index, vec![])).add_chunk(chunk.clone());
            }
        }
    }

    pub fn set_chunks(
        &self, 
        lookup: &impl LookupFileId, 
        file_size: u64,
        chunks: Vec<FileChunk>
    ) -> Result<()> {
        let mut sections: BTreeMap<SectionIndex, Vec<FileChunk>> = BTreeMap::new();
        let mut data_chunks = Vec::new();
        for chunk in chunks {
            if !chunk.is_chunk_manifest {
                data_chunks.push(chunk);
                continue;
            }
            let mut resolved_chunks = chunks::resolve_chunk_manifest(lookup, &chunk)?;
            data_chunks.append(&mut resolved_chunks);
        }   
        for chunk in data_chunks {
            let index_range = chunk.offset as u64 / SECTION_SIZE..(chunk.offset as u64 + chunk.size) / SECTION_SIZE + 1;
            for si in index_range {
                if let Some(section) = sections.get_mut(&si) {
                    section.push(chunk.clone());
                } else {
                    let section = vec![chunk.clone()];
                    sections.insert(si, section);
                }
            }
        }
        let mut sections: BTreeMap<_, _> = sections.into_iter().map(|(si, section)| (si, FileChunkSection::new(si, section))).collect();

        let mut mut_part = self.mut_part.write().unwrap();
        std::mem::swap(&mut mut_part.sections, &mut sections);
        mut_part.file_size = file_size;
        Ok(())
    }

    pub fn read_at(
        &self, 
        lookup: &impl LookupFileId, 
        file_size: u64, 
        buf: &mut [u8], 
        offset: u64
    ) -> std::io::Result<(usize, u64)> {
        if offset >= file_size {
            return Ok((0, 0));
        }
        let mut read = 0;
        let mut ts_ns = 0;
        let mut_part = self.mut_part.read().unwrap();
        let index_range = offset / SECTION_SIZE..(offset + buf.len() as u64) / SECTION_SIZE + 1;
        for si in index_range {
            let read_range = max(offset, si * SECTION_SIZE)..min(offset + buf.len() as u64, (si + 1) * SECTION_SIZE);
            if let Some(section) = mut_part.sections.get(&si) {
                let (n, ns) = section.read_at(
                    lookup, 
                    mut_part.file_size, 
                    &self.reader_cache, 
                    &mut buf[(read_range.start - offset) as usize..(read_range.end - offset) as usize], 
                    read_range.start as i64
                ).map_err(|e| {
                    log::trace!("read_at: section: {:?}, error: {}", section, e);
                    e
                })?;
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

