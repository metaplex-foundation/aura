fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src") // Output directory for the generated Rust code within grpc module
        .compile(
            &[ // Paths to the .proto files
                "proto/gap_filler.proto",
                "proto/asset_urls.proto",
                ],
            &["proto"], // Include paths for proto file dependencies
        )?;
    Ok(())
}
