use std::io::Read;

pub trait ChunkPage {
    fn read(&self, data: &mut [u8], offset: i64, ts_ns: u64) -> u64;
    fn latest_ts_ns(&self) -> u64;
    fn is_complete(&self) -> bool;
    fn written_size(&self) -> u64;
}

pub trait WritableChunkPage: ChunkPage {
    fn write(&mut self, data: &[u8], offset: i64, ts_ns: u64);
}

pub trait SealedChunkPage: ChunkPage + Sync {
    fn split_readers(&self) -> Box<dyn Iterator<Item = (Box<dyn Read>, i64, u64)>>;
}
