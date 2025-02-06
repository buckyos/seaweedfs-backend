use tokio::fs::File;
pub trait ChunkPage {
    fn read(&self, data: &mut [u8], offset: i64, ts_ns: u64) -> u64;
    fn latest_ts_ns(&self) -> u64;
    fn is_complete(&self) -> bool;
    fn written_size(&self) -> u64;
}

pub trait WritableChunkPage: ChunkPage {
    fn write(&mut self, data: &[u8], offset: i64, ts_ns: u64);
}

pub enum SplitPageContent<'a> {
    Mem(&'a [u8]),
    File(File),
}

pub struct SplitChunkPage<'a> {
    pub offset: i64,
    pub ts_ns: u64,
    pub content: SplitPageContent<'a>,
}

pub trait SealedChunkPage: ChunkPage + Sync {
    fn split_readers(&self) -> impl Iterator<Item = SplitChunkPage>;
}
