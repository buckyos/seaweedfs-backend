use dfs_mount::*;
use std::io::Write;


fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .filter_module("ureq", log::LevelFilter::Off)
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
    
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./".to_string());
    let dir = if std::path::Path::new(&dir).is_relative() {
        let exe_path = std::env::current_exe().unwrap();
        let exe_dir = exe_path.parent().unwrap();
        exe_dir.join(dir)
    } else {
        std::path::PathBuf::from(dir)
    };

    let mut option = FuseWeedfsOption::default();
    option.dir = dir;
    option.allow_others = true;
    FuseWeedfs::mount(option).unwrap();
}