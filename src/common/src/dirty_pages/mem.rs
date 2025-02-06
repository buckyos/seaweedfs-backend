use std::{cmp::{max, min}, sync::Arc};
use super::page::*;
use crate::interval_list::{Interval, IntervalList};

pub struct MemPage {
    chunk_index: i64,
    data: Vec<u8>,
    usage: IntervalList<()>,
}

impl MemPage {
    pub fn new(chunk_index: i64, chunk_size: usize) -> Self {
       Self {
            chunk_index,
            data: vec![0; chunk_size],
            usage: IntervalList::new(),
       }
    }
}

impl WritableChunkPage for MemPage {
    fn write(&mut self, data: &[u8], offset: i64, ts_ns: u64) {
        let inner_offset = (offset % self.data.len() as i64) as usize;
        self.data[inner_offset..inner_offset + data.len()].copy_from_slice(data);
        self.usage.insert(Interval {
            ts_ns: ts_ns as i64,
            range: (inner_offset as i64)..(inner_offset + data.len()) as i64,
            value: (),
        });
    }
}

impl ChunkPage for MemPage {
    fn read(&self, data: &mut [u8], offset: i64, _ts_ns: u64) -> u64 {
        let mut max_stop = 0;
        let chunk_offset = self.chunk_index * self.data.len() as i64;
        for interval in self.usage.iter() {
            let range = max(offset, chunk_offset as i64 + interval.range.start)..min(offset + data.len() as i64, chunk_offset as i64 + interval.range.end);
            if range.start < range.end {
                data[(range.start - offset) as usize..(range.end - offset) as usize].copy_from_slice(&self.data[(range.start - chunk_offset) as usize..(range.end - chunk_offset) as usize]);
                max_stop = max(max_stop, range.end);
            }
        }
        max_stop as u64
    }

    fn latest_ts_ns(&self) -> u64 {
        self.usage.iter().max_by_key(|interval| interval.ts_ns).unwrap().ts_ns as u64
    }
    fn is_complete(&self) -> bool {
        self.usage.len() == 1 && self.usage.front().unwrap().range.end - self.usage.front().unwrap().range.start == self.data.len() as i64
    }
    fn written_size(&self) -> u64 {
        self.usage.iter().map(|interval| interval.range.end - interval.range.start).sum::<i64>() as u64
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
    fn split_readers(&self) -> impl Iterator<Item = SplitChunkPage> {
        // FIXME: this is not correct, we need to split the data by the usage interval
        self.0.usage.iter().map(|interval| SplitChunkPage {
            offset: interval.range.start,
            ts_ns: interval.ts_ns as u64,
            content: SplitPageContent::Mem(&self.0.data[interval.range.start as usize..interval.range.end as usize]),
        })
    }
}
