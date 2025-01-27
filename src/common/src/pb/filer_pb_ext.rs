use crate::pb::filer_pb::FileId;
pub trait FileIdExt: ToString {
}


impl FileIdExt for FileId {
   
}

impl ToString for FileId {
    fn to_string(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.file_key.to_be_bytes());
        bytes.extend_from_slice(&self.cookie.to_be_bytes());
        let bytes: Vec<u8> = bytes.into_iter().skip_while(|&x| x == 0).collect();
        format!("{},{}", self.volume_id, hex::encode(&bytes[..]))
    }
}