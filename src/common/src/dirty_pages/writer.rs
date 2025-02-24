use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::fmt;
use tokio::sync::oneshot;

use super::page::{ChunkPage, WritableChunkPage};
use super::mem::{MemPage, SealedMemPage};

pub struct PageWriter {
    chunk_size: usize,
    writable_chunk_limit: usize,

    is_sequential_counter: i64,
    last_write_stop_offset: i64,
    writable_chunks: BTreeMap<i64, MemPage>,
    sealed_chunks: BTreeMap<i64, SealedMemPage>,
    flush_waiters: Vec<oneshot::Sender<()>>,
}

const MODE_CHANGE_LIMIT: i64 = 3;

impl fmt::Debug for PageWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageWriter")
    }
}

impl PageWriter {
    pub fn new(chunk_size: usize, writable_chunk_limit: usize) -> Self {
        Self {
            chunk_size,
            writable_chunk_limit,
            is_sequential_counter: 0,
            last_write_stop_offset: 0,
            writable_chunks: BTreeMap::new(),
            sealed_chunks: BTreeMap::new(),
            flush_waiters: vec![],
        }
    }

    fn monitor_write_at(&mut self, offset: i64) {
        if self.last_write_stop_offset == offset {
            if self.is_sequential_counter < MODE_CHANGE_LIMIT {
                self.is_sequential_counter += 1;
            }
        } else {
            if self.is_sequential_counter > -MODE_CHANGE_LIMIT {
                self.is_sequential_counter -= 1;
            }
        }
    }

    // fn is_sequential_mode(&self) -> bool {
    //     self.is_sequential_counter >= 0
    // }

    pub fn get_sealed(&self, chunk_index: i64, ts_ns: u64) -> Option<SealedMemPage> {
        self.sealed_chunks.get(&chunk_index).and_then(|page| {
            if page.latest_ts_ns() == ts_ns {
                Some(page.clone())
            } else {
                None
            }
        })
    }

    pub fn post_uploaded(&mut self, uploaded: Option<(i64, u64)>)  {
        if let Some((chunk_index, ts_ns)) = uploaded {
            if let Some(page) = self.sealed_chunks.get(&chunk_index) {
                if page.latest_ts_ns() == ts_ns {
                    log::trace!("{:?} post_uploaded: sealed index: {}, ts_ns: {}", self, chunk_index, ts_ns);
                    self.sealed_chunks.remove(&chunk_index);
                }
            }
            if self.sealed_chunks.len() == 0 && self.writable_chunks.len() == 0 {
                log::trace!("{:?} post_uploaded: all flushed", self);
                for sender in self.flush_waiters.drain(..) {
                    sender.send(()).unwrap();
                }
            }
        } else {
            log::trace!("{:?} post_uploaded: all flushed", self);
            for sender in self.flush_waiters.drain(..) {
                sender.send(()).unwrap();
            }
        }
    }


    pub fn write(&mut self, offset: i64, data: &[u8], ts_ns: u64) -> Option<Vec<(i64, u64)>> {
        self.monitor_write_at(offset);
        let mut chunk_index = offset / self.chunk_size as i64;

        let mut offset = offset;
        let mut remaining_data = &data[..];
        let mut uploads = None;
        while remaining_data.len() > 0 {
            let writen = min(remaining_data.len(), ((chunk_index + 1) * self.chunk_size as i64 - offset) as usize);
            let to_upload = if let Some(page) = self.writable_chunks.get_mut(&chunk_index) {
                page.write(&remaining_data[..writen], offset, ts_ns);
                if page.is_complete() {
                    self.sealed_chunks.insert(chunk_index, SealedMemPage::from(self.writable_chunks.remove(&chunk_index).unwrap()));
                    Some((chunk_index, ts_ns))
                } else {
                    None
                }
            } else {
                let mut page = MemPage::new(chunk_index, self.chunk_size);
                page.write(&remaining_data[..writen], offset, ts_ns);
                self.writable_chunks.insert(chunk_index, page);
                if self.writable_chunks.len() > self.writable_chunk_limit {
                    // get max writen sized page
                    let max_fulless_index = self.writable_chunks.iter().max_by_key(|(_, page)| page.written_size()).map(|(index, _)| *index).unwrap();
                    let max_fullness_page = self.writable_chunks.remove(&max_fulless_index).unwrap();
                    let max_fullness_ts_ns = max_fullness_page.latest_ts_ns();
                    self.sealed_chunks.insert(max_fulless_index, SealedMemPage::from(max_fullness_page));
                    Some((max_fulless_index, max_fullness_ts_ns))
                } else {
                    None
                }
            };
            if let Some(index) = to_upload {
                if uploads.is_none() {
                    uploads = Some(vec![index]);
                } else {
                    uploads.as_mut().unwrap().push(index);
                }
            }
            remaining_data = &remaining_data[writen..];
            offset += writen as i64;
            chunk_index += 1;
        }
        uploads
    }

    pub fn read(&self, data: &mut [u8], offset: i64, ts_ns: u64) -> Option<u64> {
        if self.sealed_chunks.len() == 0 && self.writable_chunks.len() == 0 {
            return None;
        }

        let mut chunk_index = offset / self.chunk_size as i64;
        let mut remaining_data = &mut data[..];
        let mut offset = offset;
        let mut max_stop = None;
        while remaining_data.len() > 0 {
            let read_size = min(remaining_data.len(), ((chunk_index + 1) * self.chunk_size as i64 - offset) as usize);
            if let Some(page) = self.sealed_chunks.get(&chunk_index) {
                if let Some(read_stop) = page.read(&mut remaining_data[..read_size], offset, ts_ns) {
                    max_stop = Some(max(read_stop, max_stop.unwrap_or_default()))
                }
            } 
            if let Some(page) = self.writable_chunks.get(&chunk_index) {
                if let Some(read_stop) = page.read(&mut remaining_data[..read_size], offset, ts_ns) {
                    max_stop = Some(max(read_stop, max_stop.unwrap_or_default()))
                }
            }
            remaining_data = &mut remaining_data[read_size..];
            offset += read_size as i64;
            chunk_index += 1;
        }
        max_stop
    }

    pub fn flush(&mut self) -> (Option<Vec<(i64, u64)>>, Option<oneshot::Receiver<()>>) {
        log::trace!("{:?} flush", self);
        let mut to_upload = BTreeMap::new();
        let mut uploads = vec![];
        std::mem::swap(&mut self.writable_chunks, &mut to_upload);
        for (chunk_index, page) in to_upload {
            uploads.push((chunk_index, page.latest_ts_ns()));
            log::trace!("{:?} flush: insert sealed chunk: {}", self, chunk_index);
            self.sealed_chunks.insert(chunk_index, SealedMemPage::from(page));
        }

        let receiver = self.wait_all_uploaded();
        if uploads.len() > 0 {
            (Some(uploads), receiver)
        } else {
            (None, receiver)
        }
    }

    fn wait_all_uploaded(&mut self) -> Option<oneshot::Receiver<()>> {
        if self.sealed_chunks.len() > 0 || self.writable_chunks.len() > 0 {
            let (sender, receiver) = oneshot::channel();
            self.flush_waiters.push(sender);
            Some(receiver)
        } else {
            None
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_page_writer() {
        let mut writer = PageWriter::new(2 * 1024 * 1024, 10);
        let writes = vec![
            (100, "data at 100"),
            (1000, "data at 1000"),
            (10000, "data at 10000"),
            (100000, "data at 100000"),
        ];

        for (offset, data) in writes {
            writer.write(offset, data.as_bytes(), 0);
        }

        let read_offset = 1024 * 1024 - 1000;
        let read_size = 100;
        let mut buf = vec![0u8; read_size];
        let max_stop = writer.read(&mut buf, read_offset, 0);
        
        assert_eq!(max_stop, None);
    }
    
}
