use anyhow::Result;
use crate::pb::{filer_pb::{Entry, FileChunk, FileId, FuseAttributes}, *};
use std::{collections::HashMap, future::Future, str::FromStr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::pb::filer_pb::AssignVolumeRequest;
use crate::chunks::{self, LookupFileId, UploadChunk};
use crate::ChunkCache;
use crate::file::FileHandleOwner;
use rand::{RngCore, rng, prelude::SliceRandom};
use std::thread_local;
use std::cell::RefCell;
use tokio::runtime::Runtime;


thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(None);
}


pub fn with_thread_local_runtime<F: Future>(f: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // log::trace!("using existing tokio runtime");
        handle.block_on(f)
    } else {
        RUNTIME.with(|rt| {
            if rt.borrow().is_none() {
                // log::trace!("creating tokio runtime");
                *rt.borrow_mut() = Some(Runtime::new().expect("Failed to create runtime"));
            } else {
                // log::trace!("using existing created tokio runtime");
            }

            let rt_ref = rt.borrow();
            let runtime = rt_ref.as_ref().unwrap();
            runtime.block_on(f)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeServerAccess {
    FilerProxy,
    PublicUrl,
    Url,
}

#[derive(Debug)]
pub struct WeedfsOption {
    pub filer_addresses: Vec<String>,
    pub chunk_size_limit: usize,
    pub concurrent_writers: usize,
    pub filer_root_path: PathBuf,
   
    pub replication: String,
    pub collection: String,
    pub ttl: Duration,
    pub disk_type: String,
    pub data_center: String,

    pub volume_server_access: VolumeServerAccess,
}

impl Default for WeedfsOption {
    fn default() -> Self {
        Self {
            filer_addresses: vec!["localhost:8888".to_string()],
            filer_root_path: PathBuf::from("/"),
            chunk_size_limit: 2 * 1024 * 1024,
            concurrent_writers: 32,
            ttl: Duration::from_secs(0),
            replication: "".to_string(),
            collection: "".to_string(),
            disk_type: "".to_string(),
            data_center: "".to_string(),
            volume_server_access: VolumeServerAccess::Url,
        }
    }
}

struct WeedfsInner<T: FileHandleOwner, C: ChunkCache> {
    option: WeedfsOption, 
    root_entry: Entry,
    filer_client: FilerClient,
    chunk_cache: C, 
    volume_location_cache: RwLock<HashMap<String, (Vec<String>, Vec<String>)>>,
    file_owner: T,
}

pub struct Weedfs<T: FileHandleOwner, C: ChunkCache> {
    inner: Arc<WeedfsInner<T, C>>, 
}


impl<T: FileHandleOwner, C: ChunkCache> Clone for Weedfs<T, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}


impl<T: FileHandleOwner, C: ChunkCache> Weedfs<T, C> {
    pub async fn new(option: WeedfsOption, root_entry: Entry, file_owner: T, chunk_cache: C) -> Result<Self> {    
        let filer_gprc_addresses = option.filer_addresses.clone().into_iter().map(|addr| {
            let mut parts = addr.split(':');
            let host = parts.next().unwrap_or("localhost");
            let port = parts.next().unwrap_or("8888").parse::<u16>().unwrap_or(8888);
            format!("http://{}:{}", host, port + 10000)
        }).collect();
        let filer_client = FilerClient::connect(filer_gprc_addresses).await
            .map_err(|e| {
                log::error!("Failed to connect to filer: {}", e);
                e
            })?;

        Ok(Self {
            inner: Arc::new(WeedfsInner {
                root_entry,
                filer_client,
                chunk_cache, 
                volume_location_cache: RwLock::new(HashMap::new()),
                option,
                file_owner,
            }),
        })
    }

    pub fn file_owner(&self) -> &T {
        &self.inner.file_owner
    }

    pub fn option(&self) -> &WeedfsOption {
        &self.inner.option
    }

    pub fn chunk_cache(&self) -> &C {
        &self.inner.chunk_cache
    }

    pub fn filer_client(&self) -> &FilerClient {
        &self.inner.filer_client
    }

    pub fn root_entry(&self) -> &Entry {
        &self.inner.root_entry
    }

    pub async fn get_entry_by_path(&self, path: &Path) -> Option<Entry> {
        if path == self.option().filer_root_path {
            return Some(self.root_entry().clone());
        } 

        self.filer_client().lookup_directory_entry(path).await.ok().and_then(|entry| entry)
    }

    pub async fn lookup_volume(&self, file_id: &str) -> Result<Vec<String>> {
        let pulic_url = match self.option().volume_server_access {
            VolumeServerAccess::FilerProxy => {
                return Ok(vec![format!("http://{}/?proxyChunkId={}", self.option().filer_addresses[self.filer_client().prefer_address_index()], file_id)]);
            }, 
            VolumeServerAccess::PublicUrl => {
                true
            }, 
            VolumeServerAccess::Url => {
                false
            },
        };
        
        let file_id = FileId::from_str(file_id)
            .map_err(|e| {
                anyhow::anyhow!("invalid file id: {}", e)
            })?;
        let vid = file_id.volume_id.to_string();
        let cached_locations = {
            let vid_cache = self.inner.volume_location_cache.read().unwrap();
            if let Some(locations) = vid_cache.get(&vid) {
                Some(locations.clone())
            } else {
                None
            }
        };

        let (same_dc_locations, other_locations) = if let Some(locations) = cached_locations {
            locations
        } else {
            let mut locations = self.filer_client().lookup_volume(vec![vid.clone()]).await
                .map_err(|e| {
                    anyhow::anyhow!("lookup volume on filer failed: {}", e)
                })?;
            if locations.is_empty() {
                return Err(anyhow::anyhow!("no locations found for volume {}", vid));
            }
            let locations = locations.remove(&vid).unwrap();
            let mut same_dc_locations = Vec::new();
            let mut other_locations = Vec::new();
            for location in locations.locations {
                if location.data_center == self.option().data_center {
                    if pulic_url {
                        same_dc_locations.push(location.public_url);
                    } else {
                        same_dc_locations.push(location.url);
                    }
                } else {
                    if pulic_url {
                        other_locations.push(location.public_url);
                    } else {
                        other_locations.push(location.url);
                    }
                }
            }
            self.inner.volume_location_cache.write().unwrap().insert(vid, (same_dc_locations.clone(), other_locations.clone()));
            (same_dc_locations, other_locations)
        };

        let mut same_dc_urls: Vec<String> = same_dc_locations.into_iter().map(|location| format!("http://{}/{}", location, file_id.to_string())).collect();
        same_dc_urls.shuffle(&mut rand::rng());
        let mut other_urls: Vec<String> = other_locations.into_iter().map(|location| format!("http://{}/{}", location, file_id.to_string())).collect();
        other_urls.shuffle(&mut rand::rng());
        
        let mut urls = same_dc_urls;
        urls.append(&mut other_urls);
        Ok(urls)
    }
}

impl<T: FileHandleOwner, C: ChunkCache> LookupFileId for Weedfs<T, C> {
    fn lookup(&self, file_id: &str) -> Result<Vec<String>> {
        with_thread_local_runtime(self.lookup_volume(file_id))
    }
}

#[async_trait::async_trait]
impl<T: FileHandleOwner, C: ChunkCache> UploadChunk for Weedfs<T, C> {
    async fn upload(
        &self,
        full_path: &Path,
        content: impl Send + Clone + Into<reqwest::Body>, 
        offset: i64,
        ts_ns: i64
    ) -> Result<FileChunk> {
        let assign_volume_req = AssignVolumeRequest {
            count: 1,
            replication: self.option().replication.clone(),
            collection: self.option().collection.clone(),
            ttl_sec: self.option().ttl.as_secs() as i32,
            disk_type: self.option().disk_type.clone(),
            data_center: self.option().data_center.clone(),
            rack: "".to_string(),
            data_node: "".to_string(),
            path: full_path.to_string_lossy().to_string(),
        };
        let assign_volume_resp = self.filer_client().assign_volume(assign_volume_req).await
            .map_err(|e| {
                anyhow::anyhow!("assign volume failed: {}", e)
            })?;
        let file_id = &assign_volume_resp.file_id;
        let location = assign_volume_resp.location
            .ok_or_else(|| anyhow::anyhow!("no location found"))?;
        
        log::trace!("path: {}, offset {}, ts: {} assign volume: {}", full_path.to_string_lossy(), offset, ts_ns, file_id);
        let upload_url = match self.option().volume_server_access {
            VolumeServerAccess::FilerProxy => {
                format!("http://{}/?proxyChunkId={}", location.url, file_id)
            },
            VolumeServerAccess::PublicUrl => {
                format!("http://{}/{}", location.public_url, file_id)
            },
            VolumeServerAccess::Url => {
                format!("http://{}/{}", location.url, file_id)
            },
        };

        // FIXME: auth, gzip and cipher
        let result = chunks::retried_upload_chunk(
            &upload_url, 
            full_path.file_name().unwrap().to_string_lossy().to_string().as_str(), 
            content
        ).await
            .map_err(|e| {
                anyhow::anyhow!("upload chunk {} failed: {}", file_id, e)
            })?;
        // TODO: insert to chunk cache
        // if content.offset == 0 {
        //     self.chunk_cache().set(&assign_volume_resp.file_id, Arc::new());
        // }
        Ok(FileChunk {
            offset: offset,
            size: result.size.unwrap() as u64,
            modified_ts_ns: ts_ns,
            e_tag: result.content_md5.unwrap_or_default(),
            source_file_id: "".to_string(),
            fid: FileId::from_str(&assign_volume_resp.file_id).ok(),
            source_fid: None,
            cipher_key: result.cipher_key.unwrap_or_default(),
            is_compressed: result.gzip.unwrap_or(0) > 0,
            is_chunk_manifest: false,
            file_id: assign_volume_resp.file_id,
        })
    }
}

