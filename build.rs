#[allow(unused_imports)]
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "./protobuf/history.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("greeter_descriptor.bin"))
        .out_dir("./src/protobuf")
        .type_attribute(".", "#[derive(deepsize::DeepSizeOf)]")
        .compile(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    println!("cargo:rerun-if-changed={}", proto_file);

    Ok(())
}
