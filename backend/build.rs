#![allow(unused_imports)]
use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let curr_dir = env::current_dir().unwrap_or_else(|e| panic!("current directory is inaccessible: {}", e));

    let proto_files = vec!["./protobuf/history.proto"];
    let fd_out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let pb_out_dir = curr_dir.join("src/protobuf");

    if !pb_out_dir.exists() {
        fs::create_dir(&pb_out_dir).unwrap_or_else(|e| panic!("cannot create directory {:?}: {}", pb_out_dir, e));
    }

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(fd_out_dir.join("grpc_reflection_descriptor.bin"))
        .out_dir(pb_out_dir)
        .type_attribute(".", "#[derive(deepsize::DeepSizeOf)]")
        .compile(&proto_files, &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
