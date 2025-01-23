use anyhow::Result;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::{io::Cursor, ops::Range};
use std::io::Write;
use prost::Message;
use crate::pb::filer_pb::{Entry, FileChunk, FileChunkManifest};

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

pub fn retried_stream_fetch_chunk_data(
    writer: impl Write,
    url_strings: Vec<String>,
    jwt: &str,
    cipher_key: &[u8],
    is_gziped: bool,
    is_full_chunk: bool,
    offset: i64,
    size: i64
) -> Result<()> {
    unimplemented!()
}

fn fetch_whole_chunk(
    lookup: impl Fn(&str) -> Result<Vec<String>>, 
    chunks_buffer: &mut [u8],
    file_id: &str, cipher_key: &[u8], is_gziped: bool
) -> Result<()> {
    let url_strings = lookup(file_id)?;
    retried_stream_fetch_chunk_data(
        Cursor::new(chunks_buffer), 
        url_strings, "", cipher_key, is_gziped, true, 0, 0)
}

// resolve one chunk manifest
pub fn resolve_chunk_manifest(
    lookup: impl Fn(&str) -> Result<Vec<String>>, 
    chunk: &FileChunk
) -> Result<Vec<FileChunk>> {
    if !chunk.is_chunk_manifest {
        return Ok(vec![]);
    }
    
    let manifest = RESOLVE_CHUNK_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        fetch_whole_chunk(lookup, &mut buf[..], &chunk.file_id, &chunk.cipher_key, chunk.is_compressed)?;
        FileChunkManifest::decode(&buf[..]).map_err(|e| anyhow::anyhow!("failed to decode manifest: {}", e))
    })?;

    Ok(manifest.chunks)
    
}

// resolve chunk manifest recursively
pub fn resolve_chunks_manifest(
    lookup: impl Clone + Fn(&str) -> Result<Vec<String>>, 
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
