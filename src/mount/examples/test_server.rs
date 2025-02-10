use dfs_mount::*;
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
    
    let tmp_dir = std::env::temp_dir();
    let tmp_dir = tmp_dir.join("weedfs_test");

    let mut option = WfsOption::default();
    option.dir = tmp_dir.clone();
    Wfs::mount(option).unwrap();
}