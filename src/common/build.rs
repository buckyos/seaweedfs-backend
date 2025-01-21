fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/pb")
        .compile_protos(
            &[
                "../../seaweedfs/weed/pb/filer.proto",
            ],
            &["../../seaweedfs/weed/pb"], 
        )?;
    Ok(())
} 