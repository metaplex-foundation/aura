fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src")  // Output directory for the generated Rust code within grpc module
        .compile(
            &["proto/gap_filler.proto"], // Paths to the .proto files
            &["proto"]                   // Include paths for proto file dependencies
        )?;
    Ok(())
}
