#![allow(unused_imports)]
use std::{env, fs, path::PathBuf};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter(None, log::LevelFilter::Debug)
        .init();

    let curr_dir = env::current_dir().unwrap_or_else(|e| panic!("current directory is inaccessible: {}", e));

    let scalapb_target = curr_dir.join("../scalapb/scalapb.proto");
    if !scalapb_target.exists() {
        fs::copy(scalapb_target.parent().unwrap().join("_scalapb.proto"), &scalapb_target)?;
    }

    let proto_files = vec!["backend/protobuf/services.proto"];
    let proto_includes = vec![".."];
    let fd_out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let pb_out_dir = curr_dir.join("src/protobuf");
    let descriptor_path = fd_out_dir.join("grpc_reflection_descriptor.bin");

    if !pb_out_dir.exists() {
        fs::create_dir(&pb_out_dir).unwrap_or_else(|e| panic!("cannot create directory {:?}: {}", pb_out_dir, e));
    }

    // We cannot avoid --include_imports flag, see https://github.com/tokio-rs/prost/issues/880
    // As a workaround, we compile file descriptors set (FDS) separately, edit it, and only then compile Rust code.
    // Note that this does double work - FDS are compiled into Rust files twice!
    let builder = tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(descriptor_path.clone())
        .out_dir(pb_out_dir.clone())
        .type_attribute(".", "#[derive(deepsize::DeepSizeOf)]");

    builder
        .clone()
        .emit_rerun_if_changed(false)
        .compile(&proto_files, &proto_includes)
        .unwrap_or_else(|e| panic!("protobuf (.proto -> FDS) compile error: {}", e));

    // Remove undesired file descriptors
    use prost::Message;
    let descriptor_bytes = fs::read(&descriptor_path).unwrap();
    let mut descriptor = prost_types::FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();
    descriptor.file.retain(|x| proto_files.contains(&x.name()));
    fs::write(&descriptor_path, descriptor.encode_to_vec())?;

    builder
        .skip_protoc_run()
        .emit_rerun_if_changed(false)
        .compile(&[&descriptor_path], &proto_includes)
        .unwrap_or_else(|e| panic!("protobuf (FDS -> Rust) compile error: {}", e));

    // Add imports
    let prepend_text = "pub use chat_history_manager_core::protobuf::history::*;\n\n";
    prepend_text_to_file(&pb_out_dir, "history.rs", prepend_text);

    Ok(())
}

fn prepend_text_to_file(path: &Path, file_name: &str, text: &str) {
    let file_path = path.join(file_name);
    let mut file = File::open(&file_path)
        .unwrap_or_else(|e| panic!("cannot open {file_name} file: {e}"));
    let mut buf = Vec::with_capacity(file.metadata().unwrap().len() as usize + text.len());
    buf.write_all(text.as_bytes()).unwrap();
    file.read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("cannot read {file_name} file: {e}"));

    let mut file = File::create(&file_path)
        .unwrap_or_else(|e| panic!("cannot overwrite {file_name} file: {e}"));
    file.write_all(&buf)
        .unwrap_or_else(|e| panic!("cannot write to {file_name} file: {e}"));
}
