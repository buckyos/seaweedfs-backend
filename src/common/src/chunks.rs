use anyhow::Result;
use serde::Deserialize;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::path::Path;
use std::{io::Cursor, ops::Range};
use std::io::Write;
use prost::Message;
use crate::pb::filer_pb::{FileChunk, FileChunkManifest};
use crate::interval_list::{Interval, IntervalList, IntervalValue};
use ureq::AgentBuilder;
use std::time::Duration;

pub trait LookupFileId {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>>;
}

#[async_trait::async_trait]
pub trait UploadChunk {
    async fn upload(
        &self, 
        full_path: &Path, 
        content: impl Send + Clone + Into<reqwest::Body>,
        offset: i64,
        ts_ns: i64
    ) -> Result<FileChunk>;
}


#[derive(Clone)]
pub struct VisibleInterval {
    pub file_id: String,
    pub offset_in_chunk: i64,
    pub chunk_size: u64,
}

impl IntervalValue for VisibleInterval {
    fn set_range(&mut self, old_range: Range<i64>, new_range: Range<i64>) {
        self.offset_in_chunk += new_range.start - old_range.start;
    }
}



pub fn read_resolved_chunks(chunks: &[FileChunk], _range: Range<i64>) -> IntervalList<VisibleInterval> {
    #[derive(Clone)]
    struct Point<'a> {
        x: i64,
        ts: i64,
        chunk: &'a FileChunk,
        is_start: bool,
    }

    // 收集所有点
    let mut points = Vec::new();
    for chunk in chunks {
        // let range = max(chunk.offset, range.start)..min(chunk.offset + chunk.size as i64, range.end);
        points.push(Point {
            x: chunk.offset,
            ts: chunk.modified_ts_ns,
            chunk,
            is_start: true,
        });
        points.push(Point {
            x: chunk.offset + chunk.size as i64,
            ts: chunk.modified_ts_ns,
            chunk,
            is_start: false,
        });
    }

    // 排序点
    points.sort_by(|a, b| {
        if a.x != b.x {
            return a.x.cmp(&b.x);
        }
        if a.ts != b.ts {
            return a.ts.cmp(&b.ts);
        }
        if a.is_start {
            return std::cmp::Ordering::Greater;
        }
        std::cmp::Ordering::Less
    });

    let mut queue: Vec<Point> = Vec::new();
    let mut prev_x = 0;
    let mut visible_intervals = IntervalList::new();

    for point in &points {
        if point.is_start {
            if let Some(prev_point) = queue.last() {
                if point.x != prev_x && prev_point.ts < point.ts {
                    let chunk = prev_point.chunk;
                    visible_intervals.push_back(Interval {
                        range: prev_x..point.x,
                        ts_ns: chunk.modified_ts_ns,
                        value: VisibleInterval {
                            file_id: chunk.file_id.to_string(),
                            offset_in_chunk: prev_x - chunk.offset,
                            chunk_size: chunk.size,
                        },
                    });
                    prev_x = point.x;
                    continue;
                } else if prev_point.ts < point.ts {
                    queue.push(point.clone());
                    prev_x = point.x;
                    continue;
                }
            }

            // 找到合适的插入位置
            let pos = queue.binary_search_by(|p| p.ts.cmp(&point.ts))
                .unwrap_or_else(|e| e);
            queue.insert(pos, point.clone());
        } else {
            // 找到并移除对应的点
            if let Some(pos) = queue.iter().position(|p| p.ts == point.ts) {
                queue.remove(pos);
                if pos == queue.len() {
                    if let Some(prev_point) = queue.last() {
                        let chunk = prev_point.chunk;
                        visible_intervals.push_back(Interval {
                            range: prev_x..point.x,
                            ts_ns: chunk.modified_ts_ns,
                            value: VisibleInterval {
                                file_id: chunk.file_id.to_string(),
                                offset_in_chunk: prev_x - chunk.offset,
                                chunk_size: chunk.size,
                            },
                        });
                        prev_x = point.x;
                    }
                }
            }
        }
    }

    visible_intervals
}

thread_local! {
    static RESOLVE_CHUNK_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024 * 1024));
}

pub fn total_size(chunks: &[FileChunk]) -> u64 {
    chunks.iter().map(|chunk| chunk.offset + chunk.size as i64).max().unwrap_or(0) as u64
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
            lookup, 
            &chunk)?;
        
        manifest_chunks.push(chunk);
        // recursive
        let (mut sub_data_chunks, mut sub_manifest_chunks) = resolve_chunks_manifest(lookup, resolved_chunks, offset_range.clone())?;
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


#[derive(Debug, Deserialize)]
pub struct UploadResult {
    pub name: Option<String>,
    pub size: Option<u32>,
    pub error: Option<String>,
    #[serde(rename = "eTag")]
    pub etag: Option<String>,
    #[serde(rename = "cipherKey")]
    pub cipher_key: Option<Vec<u8>>,
    pub mime: Option<String>,
    pub gzip: Option<u32>,
    #[serde(rename = "contentMd5")]
    pub content_md5: Option<String>,
}

pub async fn retried_upload_chunk(
    target_url: &str, 
    file_id: &str, 
    content: impl Clone + Into<reqwest::Body>
) -> Result<UploadResult> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut wait_time = Duration::from_secs(1);
    const MAX_WAIT: Duration = Duration::from_secs(30);
    let mut last_err = None;

    while wait_time < MAX_WAIT {
        let body = content.clone().into();
        // let body = match &content.content {
        //     SplitPageContent::Mem(content) => {
        //         let content = unsafe {
        //             std::mem::transmute::<&[u8], &[u8]>(*content)
        //         };
        //         reqwest::Body::from(content)
        //     }
        //     SplitPageContent::File(_content) => {
        //         unimplemented!()
        //     }
        // };
        match client.post(target_url)
            .header("Content-Disposition", format!("form-data; name=\"file\"; filename=\"{}\"", file_id))
            .header("Idempotency-Key", target_url)
            .body(body)
            .send()
            .await {
                Ok(resp) => {
                    let status = resp.status();
                    if !(200..300).contains(&status.as_u16()) {
                        if status.as_u16() >= 500 {
                            wait_time *= 2;
                            std::thread::sleep(wait_time);
                            continue;
                        }
                        return Err(anyhow::anyhow!("{}: {}", target_url, status));
                    }
                    let body = resp.bytes().await?;
                    let upload_result: UploadResult = serde_json::from_slice(&body)?;
                    return Ok(upload_result);
                }
                Err(e) => {
                    last_err = Some(e);
                    wait_time *= 2;
                    std::thread::sleep(wait_time);
                    continue;
                }
        }
    }

    Err(anyhow::anyhow!("failed after retries: {:?}", last_err))
}


pub async fn compact_chunks(
    lookup: &impl LookupFileId, 
    upload: &impl UploadChunk,
    full_path: &Path,
    chunks: Vec<FileChunk>
) -> Result<Vec<FileChunk>> {
    let (mut manifest_chunks, non_manifest_chunks) = separate_manifest_chunks(chunks);
    let (mut data_chunks, _) = resolve_chunks_manifest(lookup, non_manifest_chunks, 0..i64::MAX)?;
    let visibles = read_resolved_chunks(&data_chunks, 0..i64::MAX);
    let visible_file_ids = visibles.into_iter().map(|visible| visible.value.file_id.clone()).collect::<HashSet<_>>();
    data_chunks.retain(|chunk| visible_file_ids.contains(&chunk.file_id));    
    let mut merged_manifest_chunks = manifestize_chunks(upload, full_path, data_chunks, 10000).await?;
    manifest_chunks.append(&mut merged_manifest_chunks);
    Ok(manifest_chunks)
}

async fn manifestize_chunks(
    upload: &impl UploadChunk,
    full_path: &Path,
    chunks: Vec<FileChunk>,
    merge_factor: usize
) -> Result<Vec<FileChunk>> {
    let mut manifest_chunks = Vec::new();
    for to_merge in chunks.chunks(merge_factor) {
        let manifest = FileChunkManifest {
            chunks: Vec::from(to_merge),
        };
        let range = to_merge.into_iter()
            .map(|chunk| chunk.offset..chunk.offset + chunk.size as i64)
            .reduce(|mut acc, range| {
                if acc.start > range.start {
                    acc.start = range.start;
                }
                if acc.end < range.end {
                    acc.end = range.end;
                }
                acc
            }).unwrap();
        
        let manifest_data = manifest.encode_to_vec();
        let mut manifest_chunk = upload.upload(full_path, manifest_data, 0, 0).await?;
        manifest_chunk.is_chunk_manifest = true;
        manifest_chunk.offset = range.start;
        manifest_chunk.size = (range.end - range.start) as u64;
        manifest_chunks.push(manifest_chunk);
    }
    Ok(manifest_chunks)
}
