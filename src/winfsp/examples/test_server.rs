use dfs_winfsp::WinWfs;
use std::io::Write;

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .format(|buf, record| {
            writeln!(buf,
                "{} [{}:{}] {}",
                buf.timestamp(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();

    let mount_point = std::env::args().nth(1).unwrap_or_else(|| "W:\\".to_string());
    
    let wfs = WinWfs::new();
    wfs.mount(mount_point).unwrap();
} 