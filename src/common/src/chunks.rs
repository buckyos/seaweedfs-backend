use anyhow::Result;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::{io::Cursor, ops::Range};
use std::io::Write;
use prost::Message;
use crate::pb::filer_pb::{Entry, FileChunk, FileChunkManifest};
use ureq::{AgentBuilder};
use std::time::Duration;

pub trait LookupFileId {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>>;
}

thread_local! {
    static RESOLVE_CHUNK_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024 * 1024));
}

pub fn total_size(chunks: &[FileChunk]) -> u64 {
    chunks.iter().map(|chunk| chunk.offset + chunk.size as i64).max().unwrap_or(0) as u64
}

pub fn file_size(entry: Option<&Entry>) -> u64 {
    if entry.is_none() {
        return 0;
    }
    let entry = entry.as_ref().unwrap();
    if entry.attributes.is_none() {
        return 0;
    }
    let attributes = entry.attributes.as_ref().unwrap();
    let mut file_size = entry.attributes.as_ref().unwrap().file_size;
    if let Some(remote_entry) = &entry.remote_entry {
        if remote_entry.remote_mtime > attributes.mtime {
            file_size = max(file_size, remote_entry.remote_size as u64);
        }
    }
    max(total_size(entry.chunks.as_slice()), file_size)
}

pub fn has_chunk_manifest(chunks: &[FileChunk]) -> bool {
    chunks.iter().any(|chunk| chunk.is_chunk_manifest)
}

pub fn separate_manifest_chunks(chunks: Vec<FileChunk>) -> (Vec<FileChunk>, Vec<FileChunk>) {
    let mut manifest_chunks = Vec::new();
    let mut non_manifest_chunks = Vec::new();
    for chunk in chunks {
        if chunk.is_chunk_manifest {
            manifest_chunks.push(chunk);
        } else {
            non_manifest_chunks.push(chunk);
        }
    }
    (manifest_chunks, non_manifest_chunks)
}

pub fn fetch_whole_chunk(
    lookup: &impl LookupFileId, 
    chunks_buffer: &mut [u8],
    file_id: &str
) -> Result<()> {
    let url_strings = lookup.lookup(file_id)?;
    let size = chunks_buffer.len();
    retried_fetch_chunk_data(
        Cursor::new(chunks_buffer), 
        url_strings, true, 0, size)?;
    Ok(())
}

pub fn fetch_chunk_range(
    lookup: &impl LookupFileId, 
    chunks_buffer: &mut [u8],
    file_id: &str,
    offset: i64
) -> Result<usize> {
    let size = chunks_buffer.len();
    let url_strings = lookup.lookup(file_id)?;
    retried_fetch_chunk_data(chunks_buffer, url_strings, false, offset, size)
}

// resolve one chunk manifest
pub fn resolve_chunk_manifest(
    lookup: &impl LookupFileId, 
    chunk: &FileChunk
) -> Result<Vec<FileChunk>> {
    if !chunk.is_chunk_manifest {
        return Ok(vec![]);
    }
    
    let manifest = RESOLVE_CHUNK_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        fetch_whole_chunk(lookup, &mut buf[..], &chunk.file_id)?;
        FileChunkManifest::decode(&buf[..]).map_err(|e| anyhow::anyhow!("failed to decode manifest: {}", e))
    })?;

    Ok(manifest.chunks)
    
}

// resolve chunk manifest recursively
pub fn resolve_chunks_manifest(
    lookup: &impl LookupFileId, 
    chunks: Vec<FileChunk>, 
    offset_range: Range<i64>
) -> Result<(Vec<FileChunk>, Vec<FileChunk>)> {
    // TODO maybe parallel this
    let mut data_chunks = Vec::new();
    let mut manifest_chunks = Vec::new();
    for chunk in chunks {
        if max(chunk.offset, offset_range.start) >= min(chunk.offset + chunk.size as i64, offset_range.end) {
            continue;
        }

        if !chunk.is_chunk_manifest {
            data_chunks.push(chunk);
            continue;
        }

        let resolved_chunks = resolve_chunk_manifest(
            lookup.clone(), 
            &chunk)?;
        
        manifest_chunks.push(chunk);
        // recursive
        let (mut sub_data_chunks, mut sub_manifest_chunks) = resolve_chunks_manifest(lookup.clone(), resolved_chunks, offset_range.clone())?;
        data_chunks.append(&mut sub_data_chunks);
        manifest_chunks.append(&mut sub_manifest_chunks);
    }
    Ok((data_chunks, manifest_chunks))
}

pub fn retried_fetch_chunk_data(
    mut writer: impl Write,
    url_strings: Vec<String>,
    is_full_chunk: bool,
    offset: i64, 
    size: usize
) -> Result<usize> {
    let agent = AgentBuilder::new()
        .timeout_read(Duration::from_secs(30))
        .build();
    let mut last_err = None;
    let mut wait_time = Duration::from_secs(1);
    const MAX_WAIT: Duration = Duration::from_secs(30);

    while wait_time < MAX_WAIT {
        for url in &url_strings {
            let mut req = agent.get(url);

            if !is_full_chunk {
                req = req.set("Range", &format!("bytes={}-{}", offset, offset + size as i64 - 1));
            }

            match req.call() {
                Ok(resp) => {
                    let status = resp.status();
                    if !(200..300).contains(&status) {
                        if status >= 500 {
                            continue;
                        }
                        return Err(anyhow::anyhow!("{}: {}", url, status));
                    }

                    let n = std::io::copy(&mut resp.into_reader(), &mut writer)?;
                    return Ok(n as usize);
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }

        std::thread::sleep(wait_time);
        wait_time *= 2;
    }

    Err(anyhow::anyhow!("failed after retries: {:?}", last_err))
}


