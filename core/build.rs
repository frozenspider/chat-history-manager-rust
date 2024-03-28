#![allow(unused_imports)]
use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter(None, log::LevelFilter::Debug)
        .init();

    let curr_dir = env::current_dir().unwrap_or_else(|e| panic!("current directory is inaccessible: {}", e));

    let scalapb_target = curr_dir.join("../scalapb/scalapb.proto");
    if !scalapb_target.exists() {
        fs::copy(scalapb_target.parent().unwrap().join("_scalapb.proto"), &scalapb_target)?;
    }

    let proto_files = vec!["core/protobuf/entities.proto"];
    let proto_includes = vec![".."];
    let fd_out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let pb_out_dir = curr_dir.join("src/protobuf");
    let descriptor_path = fd_out_dir.join("grpc_reflection_descriptor.bin");

    if !pb_out_dir.exists() {
        fs::create_dir(&pb_out_dir).unwrap_or_else(|e| panic!("cannot create directory {:?}: {}", pb_out_dir, e));
    }

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(descriptor_path)
        .out_dir(pb_out_dir)
        .type_attribute(".", "#[derive(deepsize::DeepSizeOf, serde::Serialize, serde::Deserialize)]")
        .enum_attribute(".", r#"#[serde(rename_all = "snake_case")]"#)
        // All oneof fields should be marked with #[serde(flatten)]
        .field_attribute("Message.typed", r#"#[serde(flatten)]"#)
        .field_attribute("RichTextElement.val", r#"#[serde(flatten)]"#)
        .field_attribute("sealed_value_optional", r#"#[serde(flatten)]"#)
        .emit_rerun_if_changed(false)
        .compile(&proto_files, &proto_includes)
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    Ok(())
}
