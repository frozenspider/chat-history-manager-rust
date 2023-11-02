use std::env::args;
use std::process;

use deepsize::DeepSizeOf;
use log::LevelFilter;
use mimalloc::MiMalloc;

use chat_history_manager_rust::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/** Starts a server by default. */
fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .init();

    let server_port: u16 = 50051;

    let mut args = args();
    args.next(); // Consume first argument, which is a command itself.
    match args.next().map(|s| s.to_lowercase()).as_deref() {
        None => {
            start_server(server_port).unwrap();
        }
        Some("parse") => {
            let path = args.next().unwrap();
            let parsed = match parse_file(&path) {
                Ok(res) => res,
                Err(why) => {
                    eprintln!("Parsing failed!\n{:?}", why);
                    process::exit(1);
                }
            };
            let size: usize = parsed.deep_size_of();
            log::info!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
        Some("request_myself") => {
            debug_request_myself(server_port + 1).unwrap();
        }
        Some(etc) => {
            panic!("Unrecognized command: {etc}")
        }
    }
}
