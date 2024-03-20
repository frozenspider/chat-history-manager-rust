fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Even though this is a NOOP, not having this causes
    // "OUT_DIR env var is not set, do you have a build script?"
    // error from "tauri::generate_context" macro.
    Ok(())
}
