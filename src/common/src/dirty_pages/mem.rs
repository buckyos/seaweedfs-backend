use std::{cmp::min, io::Read, sync::Arc};

use super::page::{ChunkPage, WritableChunkPage, SealedChunkPage};

pub struct MemPage {
    data: Vec<u8>,
    written_size: u64,
}

impl MemPage {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            data: vec![0; chunk_size],
            written_size: 0,
        }
    }
}

impl WritableChunkPage for MemPage {
    fn write(&mut self, data: &[u8], offset: i64, ts_ns: u64) {
        unimplemented!()
    }
}

impl ChunkPage for MemPage {
    fn read(&self, data: &mut [u8], offset: i64, ts_ns: u64) -> u64 {
        unimplemented!()
    }

    fn latest_ts_ns(&self) -> u64 {
        unimplemented!()
    }
    fn is_complete(&self) -> bool {
        unimplemented!()
    }
    fn written_size(&self) -> u64 {
        unimplemented!()
    }
}


#[derive(Clone)]
pub struct SealedMemPage(Arc<MemPage>);

impl From<MemPage> for SealedMemPage {
    fn from(page: MemPage) -> Self {
        Self(Arc::new(page))
    }
}

impl ChunkPage for SealedMemPage {
    fn read(&self, data: &mut [u8], offset: i64, ts_ns: u64) -> u64 {
        self.0.read(data, offset, ts_ns)
    }

    fn latest_ts_ns(&self) -> u64 {
        self.0.latest_ts_ns()
    }

    fn is_complete(&self) -> bool {
        self.0.is_complete()
    }
    fn written_size(&self) -> u64 {
        self.0.written_size()
    }
}


impl SealedChunkPage for SealedMemPage {
    fn split_readers(&self) -> Box<dyn Iterator<Item = (Box<dyn Read>, i64, u64)>> {
        unimplemented!()
    }
}
