use std::cmp::max;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use anyhow::Result;
use tokio::time::sleep;
use tonic::transport::Channel;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread_local;
use tokio::runtime::Runtime;
use std::cell::RefCell;

use super::filer_pb::{*, seaweed_filer_client::*};
use crate::chunks;
pub trait EntryExt {
    fn file_size(&self) -> u64;
    fn truncate(&mut self, size: u64) -> bool;
}

impl EntryExt for Entry {
    fn file_size(&self) -> u64 {
        let attributes = self.attributes.as_ref().unwrap();
        let mut file_size = attributes.file_size;
        if let Some(remote_entry) = &self.remote_entry {
            if remote_entry.remote_mtime > attributes.mtime {
                file_size = max(file_size, remote_entry.remote_size as u64);
            }
        }
        max(chunks::total_size(&self.chunks), file_size)
    }

    fn truncate(&mut self, size: u64) -> bool {
        if size < self.file_size() {
            let mut chunks = vec![];
            for chunk in self.chunks.iter() {
                if chunk.offset as u64 >= size {
                    continue;    
                } 
                if chunk.offset as u64 + chunk.size as u64 > size {
                    let mut chunk = chunk.clone();
                    chunk.size = size - chunk.offset as u64;
                    chunks.push(chunk);
                } else {
                    chunks.push(chunk.clone());
                }
            }
            self.chunks = chunks;
            true
        } else {
            false
        }
    }
}

pub trait FileIdExt: ToString + FromStr {
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

impl FromStr for FileId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, ',');
        let volume_id = parts.next().unwrap().parse::<u32>()?;
        let file_key = parts.next().unwrap().parse::<u64>()?;
        let cookie = parts.next().unwrap().parse::<u32>()?;
        Ok(FileId { volume_id, file_key, cookie })
    }
}
    
thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(None);
}

pub struct FilerClient {
    clients: Vec<SeaweedFilerClient<Channel>>,
    last_success: AtomicUsize,
    timeout: Duration,
    max_retries: u32,
}

impl FilerClient {
    pub fn connect<D>(addresses: Vec<D>) -> Result<Self> 
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        RUNTIME.with(|rt| {
            if rt.borrow().is_none() {
                *rt.borrow_mut() = Some(Runtime::new().expect("Failed to create runtime"));
            }
            let rt_ref = rt.borrow();
            let runtime = rt_ref.as_ref().unwrap();

            runtime.block_on(async {
                let mut clients = Vec::new();
                
                for addr in addresses {
                    let endpoint = tonic::transport::Endpoint::new(addr)?
                        .timeout(Duration::from_secs(30))
                        .tcp_keepalive(Some(Duration::from_secs(60)))
                        .http2_keep_alive_interval(Duration::from_secs(30));
                        
                    let channel = endpoint.connect().await?;
                    clients.push(SeaweedFilerClient::new(channel));
                }

                if clients.is_empty() {
                    return Err(anyhow::anyhow!("no available filer servers"));
                }

                Ok(Self {
                    clients,
                    last_success: AtomicUsize::new(0),
                    timeout: Duration::from_secs(30),
                    max_retries: 3,
                })
            })
        })
    }

    async fn async_with_retry<F, Fut, T>(&self, f: F) -> Result<T>
    where 
        F: Fn(SeaweedFilerClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut retries = 0;
        let mut delay = Duration::from_millis(100);
        let start_idx = self.last_success.load(Ordering::Relaxed);
        
        loop {
            for i in 0..self.clients.len() {
                let idx = (start_idx + i) % self.clients.len();
                let client = self.clients[idx].clone();

                match tokio::time::timeout(
                    self.timeout,
                    f(client)
                ).await {
                    Ok(Ok(resp)) => {
                        self.last_success.store(idx, Ordering::Relaxed);
                        return Ok(resp);
                    }
                    Ok(Err(e)) => {
                        if i == self.clients.len() - 1 {
                            if retries >= self.max_retries {
                                return Err(anyhow::anyhow!("max retries reached: {}", e));
                            }
                            retries += 1;
                            sleep(delay).await;
                            delay *= 2;
                        }
                    }
                    Err(_) => {
                        if i == self.clients.len() - 1 {
                            if retries >= self.max_retries {
                                return Err(anyhow::anyhow!("timeout after {} retries", retries));
                            }
                            retries += 1;
                            sleep(delay).await;
                            delay *= 2;
                        }
                    }
                }
            }
        }
    }

    fn with_retry<F, Fut, T>(&self, f: F) -> Result<T>
    where 
        F: Fn(SeaweedFilerClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        RUNTIME.with(|rt| {
            if rt.borrow().is_none() {
                *rt.borrow_mut() = Some(Runtime::new().expect("Failed to create runtime"));
            }
            let runtime = rt.borrow();
            runtime.as_ref().unwrap().block_on(async {
                let mut retries = 0;
                let mut delay = Duration::from_millis(100);
                let start_idx = self.last_success.load(Ordering::Relaxed);
                
                loop {
                    for i in 0..self.clients.len() {
                        let idx = (start_idx + i) % self.clients.len();
                        let client = self.clients[idx].clone();

                        match tokio::time::timeout(
                            self.timeout,
                            f(client)
                        ).await {
                            Ok(Ok(resp)) => {
                                self.last_success.store(idx, Ordering::Relaxed);
                                return Ok(resp);
                            }
                            Ok(Err(e)) => {
                                if i == self.clients.len() - 1 {
                                    if retries >= self.max_retries {
                                        return Err(anyhow::anyhow!("max retries reached: {}", e));
                                    }
                                    retries += 1;
                                    sleep(delay).await;
                                    delay *= 2;
                                }
                            }
                            Err(_) => {
                                if i == self.clients.len() - 1 {
                                    if retries >= self.max_retries {
                                        return Err(anyhow::anyhow!("timeout after {} retries", retries));
                                    }
                                    retries += 1;
                                    sleep(delay).await;
                                    delay *= 2;
                                }
                            }
                        }
                    }
                }
            })
        })
    }

    pub fn prefer_address_index(&self) -> usize {
        self.last_success.load(Ordering::Relaxed)
    }

    
    pub async fn async_assign_volume(&self, req: AssignVolumeRequest) -> Result<AssignVolumeResponse> {
        self.async_with_retry(|mut client| {
            let req = req.clone();
            async move {
                let response = client.assign_volume(req).await?;
                Ok(response.into_inner())
            }
        }).await
    }


    pub fn create_entry(&self, parent_dir: &Path, entry: &Entry) -> Result<()> {
        self.with_retry(|mut client| {
            let req = CreateEntryRequest {
                directory: parent_dir.to_string_lossy().to_string(),
                entry: Some(entry.clone()),
                skip_check_parent_directory: true, 
                ..Default::default()
            };
            async move {
                let _ = client.create_entry(req).await?;
                Ok(())
            }
        })
    }

    // 同步 API
    pub fn lookup_volume(&self, req: Vec<String>) -> Result<HashMap<String, Locations>> {
        self.with_retry(|mut client| {
            let req = LookupVolumeRequest {
                volume_ids: req.clone(),
            };
            async move {
                let response = client.lookup_volume(req).await?;
                Ok(response.into_inner().locations_map)
            }
        })
    }

    pub fn lookup_directory_entry(&self, path: &Path) -> Result<Option<Entry>> {
        self.with_retry(|mut client| {
            let req = LookupDirectoryEntryRequest {
                directory: path.to_string_lossy().to_string(),
                name: path.file_name().unwrap().to_string_lossy().to_string(),
            };
            async move {
                let response = client.lookup_directory_entry(req).await?;
                Ok(response.into_inner().entry.map(|mut entry| {
                    entry.name = path.to_string_lossy().to_string();
                    entry
                }))
            }
        })
    }

    pub fn update_entry(&self, entry: Entry) -> Result<()> {
        self.with_retry(|mut client| {
            let req = UpdateEntryRequest {
                directory: Path::new(&entry.name).parent().unwrap().to_string_lossy().to_string(),
                entry: Some(entry.clone()),
                is_from_other_cluster: false,
                signatures: vec![],
            };
            async move {
                let _ = client.update_entry(req).await?;
                Ok(())
            }
        })
    }
}
